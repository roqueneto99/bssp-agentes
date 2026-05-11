"""
src/sync/lead_comparison.py
============================

Reconciliação diária RD Station x Hablla.

Roda DEPOIS do `incremental_sync` (RD) e do `run_incremental_sync` (Hablla)
dentro do `sync_job.py` das 03:00 BRT. Produz três artefatos:

1) leads.hablla_match_key / hablla_match_score  — chave usada e confiança.
2) daily_lead_comparison                        — diff por dimensão por par.
3) daily_comparison_summary                     — agregado do dia para o painel.

Algoritmo de match em cascata (ordem = prioridade decrescente):

    1. external_id  — hablla_person_id já gravado em leads.hablla_person_id  (1.000)
    2. email        — email normalizado (lower/strip)                         (0.950)
    3. phone        — telefone em E.164 BR                                    (0.850)
    4. name         — Jaro-Winkler em nome+sobrenome ≥ 0.92                   (0.700–0.799)

Dimensões comparadas:
    - stage              → lifecycle_stage (RD) vs s3_estagio + hablla_card_status
    - classification     → s2_classificacao (RD calc.) vs derivado Hablla
    - last_interaction   → last_conversion_date vs s3_ultima_msg_em (diverge se Δ > 7d)
    - origin             → primeira tag/origem RD vs hablla_board_id (curso)

Uso programático:
    from src.sync.lead_comparison import run_daily_comparison
    summary = await run_daily_comparison(snapshot_date=None)  # default: hoje BRT
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import text

from src.database.connection import get_session

logger = logging.getLogger("sync.lead_comparison")

TZ_BRT = timezone(timedelta(hours=-3))

LAST_INTERACTION_DIVERGENCE_DAYS = 7
NAME_MATCH_MIN_SCORE = 0.92


# ---------------------------------------------------------------------------
# Normalizadores
# ---------------------------------------------------------------------------
_NON_DIGIT = re.compile(r"\D+")


def norm_email(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return s.strip().lower() or None


def norm_phone_br(s: Optional[str]) -> Optional[str]:
    """Telefone BR em E.164. '11999998888' -> '+5511999998888'."""
    if not s:
        return None
    digits = _NON_DIGIT.sub("", s)
    if not digits:
        return None
    if digits.startswith("55"):
        digits = digits[2:]
    digits = digits.lstrip("0")
    if len(digits) < 10 or len(digits) > 11:
        return None
    return f"+55{digits}"


def norm_name(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    return re.sub(r"\s+", " ", s.strip().lower()) or None


def jaro_winkler(a: str, b: str) -> float:
    """Implementação enxuta (sem rapidfuzz)."""
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    la, lb = len(a), len(b)
    match_dist = max(la, lb) // 2 - 1
    a_match = [False] * la
    b_match = [False] * lb
    matches = 0
    for i, ca in enumerate(a):
        lo = max(0, i - match_dist)
        hi = min(i + match_dist + 1, lb)
        for j in range(lo, hi):
            if b_match[j] or b[j] != ca:
                continue
            a_match[i] = True
            b_match[j] = True
            matches += 1
            break
    if not matches:
        return 0.0
    transpositions = 0
    k = 0
    for i in range(la):
        if not a_match[i]:
            continue
        while not b_match[k]:
            k += 1
        if a[i] != b[k]:
            transpositions += 1
        k += 1
    transpositions //= 2
    jaro = (
        matches / la
        + matches / lb
        + (matches - transpositions) / matches
    ) / 3
    prefix = 0
    for i in range(min(4, la, lb)):
        if a[i] == b[i]:
            prefix += 1
        else:
            break
    return jaro + prefix * 0.1 * (1 - jaro)


# ---------------------------------------------------------------------------
# Match em cascata
# ---------------------------------------------------------------------------
@dataclass
class MatchResult:
    lead_id: Optional[int]
    hablla_person_id: Optional[str]
    key: Optional[str]
    score: float


def _score_for(key: str, raw_score: float = 1.0) -> float:
    base = {"external_id": 1.000, "email": 0.950, "phone": 0.850, "name": 0.700}[key]
    if key == "name":
        return round(base + max(0.0, min(0.1, (raw_score - 0.92))), 3)
    return base


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
SQL_FETCH_RD = text("""
    SELECT id, uuid, email, name, personal_phone, mobile_phone,
           lifecycle_stage, s2_classificacao,
           last_conversion_date, tags,
           hablla_person_id, hablla_card_status, hablla_em_atendimento,
           s3_estagio, s3_ultima_msg_em, hablla_board_id,
           hablla_match_key, hablla_match_score,
           synced_at, updated_at
      FROM leads
""")

SQL_FETCH_HABLLA_ONLY = text("""
    SELECT id, hablla_person_id, email, phone_e164, name,
           hablla_card_status, hablla_em_atendimento, hablla_board_id,
           ultima_msg_em, last_seen_at
      FROM leads_hablla_only
""")

SQL_UPSERT_COMPARISON = text("""
    INSERT INTO daily_lead_comparison (
        snapshot_date, lead_id, hablla_only_id, hablla_person_id,
        bucket, match_key, match_score,
        diff_stage, diff_classification, diff_last_interaction, diff_origin,
        rd_stage, hablla_stage,
        rd_classification, hablla_classification,
        rd_last_interaction, hablla_last_interaction,
        rd_origin, hablla_origin,
        updated_in_last_24h
    ) VALUES (
        :snapshot_date, :lead_id, :hablla_only_id, :hablla_person_id,
        :bucket, :match_key, :match_score,
        :diff_stage, :diff_classification, :diff_last_interaction, :diff_origin,
        :rd_stage, :hablla_stage,
        :rd_classification, :hablla_classification,
        :rd_last_interaction, :hablla_last_interaction,
        :rd_origin, :hablla_origin,
        :updated_in_last_24h
    )
    ON CONFLICT (snapshot_date, COALESCE(lead_id, 0), COALESCE(hablla_only_id, 0))
    DO UPDATE SET
        bucket = EXCLUDED.bucket,
        match_key = EXCLUDED.match_key,
        match_score = EXCLUDED.match_score,
        diff_stage = EXCLUDED.diff_stage,
        diff_classification = EXCLUDED.diff_classification,
        diff_last_interaction = EXCLUDED.diff_last_interaction,
        diff_origin = EXCLUDED.diff_origin,
        rd_stage = EXCLUDED.rd_stage,
        hablla_stage = EXCLUDED.hablla_stage,
        rd_classification = EXCLUDED.rd_classification,
        hablla_classification = EXCLUDED.hablla_classification,
        rd_last_interaction = EXCLUDED.rd_last_interaction,
        hablla_last_interaction = EXCLUDED.hablla_last_interaction,
        rd_origin = EXCLUDED.rd_origin,
        hablla_origin = EXCLUDED.hablla_origin,
        updated_in_last_24h = EXCLUDED.updated_in_last_24h
""")

SQL_UPDATE_LEAD_MATCH = text("""
    UPDATE leads
       SET hablla_match_key = :key,
           hablla_match_score = :score
     WHERE id = :lead_id
""")


# ---------------------------------------------------------------------------
# Helpers de diff
# ---------------------------------------------------------------------------
def _origin_from_tags(tags: Any) -> Optional[str]:
    if not tags:
        return None
    if isinstance(tags, list):
        for t in tags:
            if not isinstance(t, str):
                continue
            for prefix in ("origem:", "utm_source:", "fonte:"):
                if t.lower().startswith(prefix):
                    return t.split(":", 1)[1].strip()
        return tags[0] if tags and isinstance(tags[0], str) else None
    return None


def _interaction_diverges(rd_dt: Optional[datetime], h_dt: Optional[datetime]) -> bool:
    if not rd_dt or not h_dt:
        return bool(rd_dt) != bool(h_dt)
    return abs((rd_dt - h_dt).days) > LAST_INTERACTION_DIVERGENCE_DAYS


def _compute_diffs_rd_vs_hablla(rd: dict, h: dict) -> dict[str, bool]:
    return {
        "stage": (rd["lifecycle_stage"] or "").lower() != (h["hablla_card_status"] or "").lower(),
        "classification": bool(rd["s2_classificacao"]) ^ bool(h["hablla_card_status"]),
        "last_interaction": _interaction_diverges(rd["last_conversion_date"], h["ultima_msg_em"]),
        "origin": (_origin_from_tags(rd["tags"]) or "") != (h["hablla_board_id"] or ""),
    }


def _compute_diffs_inline(r: dict) -> dict[str, bool]:
    return {
        "stage": (r["lifecycle_stage"] or "").lower() != (r["hablla_card_status"] or "").lower(),
        "classification": bool(r["s2_classificacao"]) ^ bool(r["hablla_card_status"]),
        "last_interaction": _interaction_diverges(r["last_conversion_date"], r["s3_ultima_msg_em"]),
        "origin": (_origin_from_tags(r["tags"]) or "") != (r["hablla_board_id"] or ""),
    }


def _accumulate_diff_stats(stats: dict, diffs: dict[str, bool]) -> None:
    for k, v in diffs.items():
        if v:
            stats[f"divergent_{k}"] += 1


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------
def _row_match(snapshot_date, rd, h, m, diffs, cutoff):
    return {
        "snapshot_date": snapshot_date,
        "lead_id": rd["id"],
        "hablla_only_id": None,
        "hablla_person_id": m.hablla_person_id,
        "bucket": "matched_divergent" if any(diffs.values()) else "matched_aligned",
        "match_key": m.key,
        "match_score": m.score,
        **{f"diff_{k}": v for k, v in diffs.items()},
        "rd_stage": rd["lifecycle_stage"],
        "hablla_stage": h["hablla_card_status"],
        "rd_classification": rd["s2_classificacao"],
        "hablla_classification": h["hablla_card_status"],
        "rd_last_interaction": rd["last_conversion_date"],
        "hablla_last_interaction": h["ultima_msg_em"],
        "rd_origin": _origin_from_tags(rd["tags"]),
        "hablla_origin": h["hablla_board_id"],
        "updated_in_last_24h": (rd["updated_at"] and rd["updated_at"] > cutoff)
            or (h["last_seen_at"] and h["last_seen_at"] > cutoff),
    }


def _row_inline(snapshot_date, r, diffs, cutoff):
    return {
        "snapshot_date": snapshot_date,
        "lead_id": r["id"],
        "hablla_only_id": None,
        "hablla_person_id": r["hablla_person_id"],
        "bucket": "matched_divergent" if any(diffs.values()) else "matched_aligned",
        "match_key": r["hablla_match_key"] or "email",
        "match_score": r["hablla_match_score"] or 0.95,
        **{f"diff_{k}": v for k, v in diffs.items()},
        "rd_stage": r["lifecycle_stage"],
        "hablla_stage": r["hablla_card_status"],
        "rd_classification": r["s2_classificacao"],
        "hablla_classification": r["hablla_card_status"],
        "rd_last_interaction": r["last_conversion_date"],
        "hablla_last_interaction": r["s3_ultima_msg_em"],
        "rd_origin": _origin_from_tags(r["tags"]),
        "hablla_origin": r["hablla_board_id"],
        "updated_in_last_24h": bool(r["updated_at"] and r["updated_at"] > cutoff),
    }


def _row_only_rd(snapshot_date, r, cutoff):
    return {
        "snapshot_date": snapshot_date, "lead_id": r["id"], "hablla_only_id": None,
        "hablla_person_id": None, "bucket": "only_rd",
        "match_key": None, "match_score": None,
        "diff_stage": False, "diff_classification": False,
        "diff_last_interaction": False, "diff_origin": False,
        "rd_stage": r["lifecycle_stage"], "hablla_stage": None,
        "rd_classification": r["s2_classificacao"], "hablla_classification": None,
        "rd_last_interaction": r["last_conversion_date"], "hablla_last_interaction": None,
        "rd_origin": _origin_from_tags(r["tags"]), "hablla_origin": None,
        "updated_in_last_24h": bool(r["updated_at"] and r["updated_at"] > cutoff),
    }


def _row_only_hablla(snapshot_date, h, cutoff):
    return {
        "snapshot_date": snapshot_date, "lead_id": None, "hablla_only_id": h["id"],
        "hablla_person_id": h["hablla_person_id"], "bucket": "only_hablla",
        "match_key": None, "match_score": None,
        "diff_stage": False, "diff_classification": False,
        "diff_last_interaction": False, "diff_origin": False,
        "rd_stage": None, "hablla_stage": h["hablla_card_status"],
        "rd_classification": None, "hablla_classification": h["hablla_card_status"],
        "rd_last_interaction": None, "hablla_last_interaction": h["ultima_msg_em"],
        "rd_origin": None, "hablla_origin": h["hablla_board_id"],
        "updated_in_last_24h": bool(h["last_seen_at"] and h["last_seen_at"] > cutoff),
    }


SQL_UPSERT_SUMMARY = text("""
    INSERT INTO daily_comparison_summary (
        snapshot_date, total, matched_aligned, matched_divergent,
        only_rd, only_hablla,
        divergent_stage, divergent_classification,
        divergent_last_interaction, divergent_origin,
        matched_by_external_id, matched_by_email,
        matched_by_phone, matched_by_name,
        novos_24h, atualizados_24h, computed_at
    ) VALUES (
        :snapshot_date, :total, :matched_aligned, :matched_divergent,
        :only_rd, :only_hablla,
        :divergent_stage, :divergent_classification,
        :divergent_last_interaction, :divergent_origin,
        :matched_by_external_id, :matched_by_email,
        :matched_by_phone, :matched_by_name,
        :novos_24h, :atualizados_24h, NOW()
    )
    ON CONFLICT (snapshot_date) DO UPDATE SET
        total = EXCLUDED.total,
        matched_aligned = EXCLUDED.matched_aligned,
        matched_divergent = EXCLUDED.matched_divergent,
        only_rd = EXCLUDED.only_rd,
        only_hablla = EXCLUDED.only_hablla,
        divergent_stage = EXCLUDED.divergent_stage,
        divergent_classification = EXCLUDED.divergent_classification,
        divergent_last_interaction = EXCLUDED.divergent_last_interaction,
        divergent_origin = EXCLUDED.divergent_origin,
        matched_by_external_id = EXCLUDED.matched_by_external_id,
        matched_by_email = EXCLUDED.matched_by_email,
        matched_by_phone = EXCLUDED.matched_by_phone,
        matched_by_name = EXCLUDED.matched_by_name,
        novos_24h = EXCLUDED.novos_24h,
        atualizados_24h = EXCLUDED.atualizados_24h,
        computed_at = NOW()
""")


# ---------------------------------------------------------------------------
# Job principal
# ---------------------------------------------------------------------------
async def run_daily_comparison(
    snapshot_date: Optional[date] = None,
    dry_run: bool = False,
) -> dict:
    if snapshot_date is None:
        snapshot_date = datetime.now(TZ_BRT).date()
    cutoff_24h = datetime.now(TZ_BRT) - timedelta(hours=24)

    async with get_session() as session:
        rd_rows = (await session.execute(SQL_FETCH_RD)).mappings().all()
        hablla_only_rows = (await session.execute(SQL_FETCH_HABLLA_ONLY)).mappings().all()

        by_external = {r["hablla_person_id"]: r for r in rd_rows if r["hablla_person_id"]}
        by_email: dict[str, dict] = {}
        by_phone: dict[str, dict] = {}
        by_name: list[tuple[str, dict]] = []
        for r in rd_rows:
            e = norm_email(r["email"])
            if e:
                by_email.setdefault(e, r)
            for ph in (r["mobile_phone"], r["personal_phone"]):
                p = norm_phone_br(ph)
                if p:
                    by_phone.setdefault(p, r)
            n = norm_name(r["name"])
            if n:
                by_name.append((n, r))

        stats = dict.fromkeys([
            "total", "matched_aligned", "matched_divergent", "only_rd", "only_hablla",
            "matched_by_external_id", "matched_by_email", "matched_by_phone", "matched_by_name",
            "divergent_stage", "divergent_classification",
            "divergent_last_interaction", "divergent_origin",
            "atualizados_24h", "novos_24h",
        ], 0)
        stats["snapshot_date"] = snapshot_date.isoformat()

        matched_lead_ids: set[int] = set()

        # 1) Hablla-only → tentar casar via cascata
        for h in hablla_only_rows:
            match = MatchResult(None, h["hablla_person_id"], None, 0.0)
            if h["hablla_person_id"] and h["hablla_person_id"] in by_external:
                r = by_external[h["hablla_person_id"]]
                match = MatchResult(r["id"], h["hablla_person_id"],
                                    "external_id", _score_for("external_id"))
            else:
                e = norm_email(h["email"])
                if e and e in by_email:
                    r = by_email[e]
                    match = MatchResult(r["id"], h["hablla_person_id"],
                                        "email", _score_for("email"))
                else:
                    p = norm_phone_br(h["phone_e164"])
                    if p and p in by_phone:
                        r = by_phone[p]
                        match = MatchResult(r["id"], h["hablla_person_id"],
                                            "phone", _score_for("phone"))
                    else:
                        n = norm_name(h["name"])
                        if n:
                            best = (0.0, None)
                            for cand_name, cand_row in by_name:
                                s = jaro_winkler(n, cand_name)
                                if s > best[0]:
                                    best = (s, cand_row)
                            if best[0] >= NAME_MATCH_MIN_SCORE and best[1]:
                                match = MatchResult(best[1]["id"], h["hablla_person_id"],
                                                    "name", _score_for("name", best[0]))

            if match.key and match.lead_id is not None:
                stats[f"matched_by_{match.key}"] += 1
                matched_lead_ids.add(match.lead_id)
                row = next(r for r in rd_rows if r["id"] == match.lead_id)
                diffs = _compute_diffs_rd_vs_hablla(row, h)
                bucket = "matched_divergent" if any(diffs.values()) else "matched_aligned"
                _accumulate_diff_stats(stats, diffs)
                stats[bucket] += 1
                if not dry_run:
                    await session.execute(SQL_UPDATE_LEAD_MATCH, {
                        "lead_id": match.lead_id, "key": match.key, "score": match.score,
                    })
                    await session.execute(SQL_UPSERT_COMPARISON,
                                          _row_match(snapshot_date, row, h, match, diffs, cutoff_24h))
            else:
                stats["only_hablla"] += 1
                if not dry_run:
                    await session.execute(SQL_UPSERT_COMPARISON,
                                          _row_only_hablla(snapshot_date, h, cutoff_24h))

        # 2) RD com Hablla inline (sync diário) ou sem nenhum match
        for r in rd_rows:
            if r["id"] in matched_lead_ids:
                continue
            if r["hablla_person_id"]:
                diffs = _compute_diffs_inline(r)
                bucket = "matched_divergent" if any(diffs.values()) else "matched_aligned"
                stats[bucket] += 1
                stats["matched_by_email"] += 1
                _accumulate_diff_stats(stats, diffs)
                if not dry_run:
                    await session.execute(SQL_UPSERT_COMPARISON,
                                          _row_inline(snapshot_date, r, diffs, cutoff_24h))
            else:
                stats["only_rd"] += 1
                if not dry_run:
                    await session.execute(SQL_UPSERT_COMPARISON,
                                          _row_only_rd(snapshot_date, r, cutoff_24h))

        stats["total"] = (stats["matched_aligned"] + stats["matched_divergent"]
                          + stats["only_rd"] + stats["only_hablla"])

        if not dry_run:
            await session.execute(SQL_UPSERT_SUMMARY, {
                "snapshot_date": snapshot_date,
                **{k: stats[k] for k in (
                    "total", "matched_aligned", "matched_divergent",
                    "only_rd", "only_hablla",
                    "divergent_stage", "divergent_classification",
                    "divergent_last_interaction", "divergent_origin",
                    "matched_by_external_id", "matched_by_email",
                    "matched_by_phone", "matched_by_name",
                    "novos_24h", "atualizados_24h",
                )},
            })
            await session.commit()

    logger.info("Comparativo %s: %s", snapshot_date, stats)
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD (default: hoje BRT)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    snap = date.fromisoformat(args.date) if args.date else None
    asyncio.run(run_daily_comparison(snapshot_date=snap, dry_run=args.dry_run))


if __name__ == "__main__":
    _main()
