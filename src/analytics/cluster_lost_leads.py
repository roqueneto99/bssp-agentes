"""
src/analytics/cluster_lost_leads.py
====================================

Clusterização de leads "perdidos" (only_rd do comparativo) — leads que estão
no RD Station mas nunca chegaram ao Hablla.

Pipeline:
    1) Carrega leads sem hablla_person_id atualizados desde `since_date`.
    2) Extrai features: s2_score, dias-sem-conversão, idade do lead,
       lifecycle_stage (one-hot), fit_score, interest_score.
    3) Normaliza com StandardScaler.
    4) Roda K-Means com K∈[3..6], escolhe o melhor via silhouette score.
    5) Para cada cluster, descreve estatísticas e chama Claude pra gerar:
       - nome curto
       - hipótese de comportamento
       - copy de WhatsApp e e-mail pra reengajar
    6) Persiste tudo (run, clusters, assignments).

Uso programático:
    from src.analytics.cluster_lost_leads import run_cluster_analysis
    summary = await run_cluster_analysis(since_date=date(2026,4,1))

CLI:
    python -m src.analytics.cluster_lost_leads --since 2026-04-01
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import statistics
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text

from src.database.connection import get_session
from src.agents.base import LLMProvider, LLMMessage

logger = logging.getLogger("analytics.cluster_lost_leads")

TZ_BRT = timezone(timedelta(hours=-3))

K_RANGE = (3, 6)
MIN_LEADS_TO_CLUSTER = 30


# ---------------------------------------------------------------------------
# Carregamento
# ---------------------------------------------------------------------------

SQL_FETCH_LOST = text("""
    SELECT id, email, name,
           COALESCE(s2_score, 0)::float            AS s2_score,
           lifecycle_stage,
           fit_score, interest_score,
           last_conversion_date, first_conversion_date,
           updated_at
      FROM leads
     WHERE hablla_person_id IS NULL
       AND (:since_date IS NULL OR updated_at >= :since_dt)
""")


@dataclass
class LeadRow:
    id: int
    email: str
    name: Optional[str]
    s2_score: float
    lifecycle: Optional[str]
    fit: Optional[str]
    interest: Optional[str]
    days_since_last: float
    days_since_first: float
    updated_at: datetime


def _to_lead_row(r) -> LeadRow:
    now = datetime.now(TZ_BRT)
    def days_since(dt):
        if dt is None:
            return 365.0
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ_BRT)
        return max(0.0, (now - dt).days)
    return LeadRow(
        id=r["id"],
        email=r["email"],
        name=r["name"],
        s2_score=float(r["s2_score"] or 0.0),
        lifecycle=(r["lifecycle_stage"] or "").lower(),
        fit=(r["fit_score"] or "").upper(),
        interest=(r["interest_score"] or "").upper(),
        days_since_last=days_since(r["last_conversion_date"]),
        days_since_first=days_since(r["first_conversion_date"]),
        updated_at=r["updated_at"],
    )


# ---------------------------------------------------------------------------
# Features
# ---------------------------------------------------------------------------

LIFECYCLES = ("lead", "mql", "sql", "opportunity", "customer", "")
FIT_LEVELS = ("A", "B", "C", "D", "")
INT_LEVELS = ("A", "B", "C", "D", "")


def _featurize(rows: list[LeadRow]) -> tuple[np.ndarray, list[str]]:
    """Constrói matriz de features. Retorna (X, feature_names)."""
    feat_names = ["s2_score", "days_since_last", "days_since_first"]
    feat_names += [f"lifecycle_{x}" for x in LIFECYCLES if x]
    feat_names += [f"fit_{x}" for x in FIT_LEVELS if x]
    feat_names += [f"interest_{x}" for x in INT_LEVELS if x]

    X = np.zeros((len(rows), len(feat_names)), dtype=np.float32)
    for i, r in enumerate(rows):
        X[i, 0] = r.s2_score
        X[i, 1] = r.days_since_last
        X[i, 2] = r.days_since_first
        col = 3
        for lc in LIFECYCLES:
            if not lc:
                continue
            X[i, col] = 1.0 if r.lifecycle == lc else 0.0
            col += 1
        for fl in FIT_LEVELS:
            if not fl:
                continue
            X[i, col] = 1.0 if r.fit == fl else 0.0
            col += 1
        for il in INT_LEVELS:
            if not il:
                continue
            X[i, col] = 1.0 if r.interest == il else 0.0
            col += 1
    return X, feat_names


def _choose_k(X_norm: np.ndarray) -> tuple[int, KMeans, float]:
    """K-Means com K=3..6 escolhendo o melhor silhouette score."""
    best = (-1.0, None, 0)
    for k in range(K_RANGE[0], K_RANGE[1] + 1):
        if X_norm.shape[0] < k * 5:
            continue
        km = KMeans(n_clusters=k, n_init=10, random_state=42)
        labels = km.fit_predict(X_norm)
        try:
            s = silhouette_score(X_norm, labels, sample_size=min(2000, X_norm.shape[0]),
                                 random_state=42)
        except Exception:
            s = -1.0
        logger.info("K=%d silhouette=%.3f", k, s)
        if s > best[0]:
            best = (s, km, k)
    if best[1] is None:
        raise RuntimeError("Não foi possível clusterizar (poucos leads).")
    return best[2], best[1], best[0]


# ---------------------------------------------------------------------------
# Estatísticas por cluster
# ---------------------------------------------------------------------------

@dataclass
class ClusterStats:
    cluster_index: int
    n: int
    score_medio: float
    score_mediano: float
    days_since_last_mediana: float
    lifecycle_dominante: str
    fit_dominante: str
    interest_dominante: str
    raw_features_avg: dict[str, float] = field(default_factory=dict)


def _stats_for(rows: list[LeadRow]) -> dict[str, Any]:
    scores = [r.s2_score for r in rows]
    days = [r.days_since_last for r in rows]
    lc = Counter(r.lifecycle or "—" for r in rows).most_common(1)[0][0]
    fit = Counter(r.fit or "—" for r in rows).most_common(1)[0][0]
    intg = Counter(r.interest or "—" for r in rows).most_common(1)[0][0]
    return {
        "n": len(rows),
        "score_medio": round(statistics.fmean(scores), 2) if scores else 0.0,
        "score_mediano": round(statistics.median(scores), 2) if scores else 0.0,
        "days_mediana": int(statistics.median(days)) if days else 0,
        "lifecycle_dominante": lc,
        "fit_dominante": fit,
        "interest_dominante": intg,
    }


# ---------------------------------------------------------------------------
# LLM — nomeia e gera copy
# ---------------------------------------------------------------------------

LLM_PROMPT = """Você é estrategista de marketing de uma faculdade de pós-graduação.

Recebeu um cluster de leads que se interessaram pela BSSP mas nunca conversaram
com o time comercial (estão no RD Station, não foram pro Hablla). Seu trabalho
é dar um nome curto a esse perfil, criar uma hipótese de comportamento e
sugerir copy de reengajamento (WhatsApp e e-mail).

PERFIL DO CLUSTER:
- Total de leads: {n}
- Score médio (0–100): {score_medio}
- Mediana de dias desde última conversão: {days_mediana}
- Lifecycle stage dominante (RD): {lifecycle_dominante}
- Fit score dominante: {fit_dominante}
- Interest score dominante: {interest_dominante}

REGRAS:
- O nome do cluster tem 2-4 palavras, em português, evocativo (ex: "Curiosos frios", "Quase-MQL dormente").
- A hipótese explica em 1-2 frases POR QUE esse grupo provavelmente não avançou.
- A copy de WhatsApp tem ATÉ 280 caracteres, tom humano, com 1 pergunta no fim.
- A copy de e-mail tem assunto (≤60 chars) e corpo (≤600 chars, sem assinatura).
- Não use clichês ("não perca", "última chance"). Seja específico ao perfil.

Responda APENAS em JSON válido:
{{
  "nome": "...",
  "hipotese": "...",
  "copy_whatsapp": "...",
  "copy_email_assunto": "...",
  "copy_email_corpo": "..."
}}
"""


async def _gen_cluster_copy(llm: LLMProvider, stats: dict) -> dict:
    """Chama Claude pra batizar e gerar copy de um cluster."""
    prompt = LLM_PROMPT.format(**stats)
    try:
        resp = await llm.complete(
            messages=[LLMMessage(role="user", content=prompt)],
            response_format="json",
            temperature=0.5,
            max_tokens=800,
        )
        # Tenta parsear JSON com tolerância a code fences
        content = resp.content.strip()
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
            content = content.strip()
        data = json.loads(content)
        return {
            "nome": data.get("nome", "")[:64],
            "hipotese": data.get("hipotese", ""),
            "copy_whatsapp": data.get("copy_whatsapp", ""),
            "copy_email_assunto": data.get("copy_email_assunto", "")[:255],
            "copy_email_corpo": data.get("copy_email_corpo", ""),
        }
    except Exception as e:
        logger.exception("Falha ao gerar copy via LLM: %s", e)
        return {
            "nome": f"Cluster {stats['n']} leads",
            "hipotese": "Não foi possível gerar via IA — preencher manualmente.",
            "copy_whatsapp": "",
            "copy_email_assunto": "",
            "copy_email_corpo": "",
        }


# ---------------------------------------------------------------------------
# Persistência
# ---------------------------------------------------------------------------

SQL_INSERT_RUN = text("""
    INSERT INTO cluster_runs (since_date, n_leads, n_clusters, silhouette_score)
    VALUES (:since_date, :n_leads, :n_clusters, :silhouette)
    RETURNING id
""")

SQL_INSERT_CLUSTER = text("""
    INSERT INTO lead_clusters (
        run_id, cluster_index, label, hipotese,
        copy_whatsapp, copy_email_assunto, copy_email_corpo,
        n_leads, score_medio, score_mediano,
        lifecycle_dominante, dias_sem_interacao_mediana, metadados
    ) VALUES (
        :run_id, :cluster_index, :label, :hipotese,
        :copy_wa, :copy_email_assunto, :copy_email_corpo,
        :n_leads, :score_medio, :score_mediano,
        :lifecycle, :dias_mediana, :metadados
    )
    RETURNING id
""")

SQL_INSERT_ASSIGN = text("""
    INSERT INTO lead_cluster_assignments (run_id, cluster_id, lead_id, distance)
    VALUES (:run_id, :cluster_id, :lead_id, :distance)
    ON CONFLICT (run_id, lead_id) DO NOTHING
""")


# ---------------------------------------------------------------------------
# Job principal
# ---------------------------------------------------------------------------

async def run_cluster_analysis(
    since_date: Optional[date] = None,
    dry_run: bool = False,
) -> dict:
    """
    Roda a análise completa. Retorna resumo executivo (sem persistir leads
    individuais quando dry_run=True).
    """
    since_dt = (
        datetime.combine(since_date, datetime.min.time(), tzinfo=TZ_BRT)
        if since_date else None
    )

    async with get_session() as session:
        rows_raw = (await session.execute(SQL_FETCH_LOST, {
            "since_date": since_date, "since_dt": since_dt,
        })).mappings().all()
        rows = [_to_lead_row(r) for r in rows_raw]

        if len(rows) < MIN_LEADS_TO_CLUSTER:
            return {
                "status": "insufficient_data",
                "n_leads": len(rows),
                "min_required": MIN_LEADS_TO_CLUSTER,
                "message": f"Apenas {len(rows)} leads no recorte — mínimo {MIN_LEADS_TO_CLUSTER}.",
            }

        # Features + normalização
        X, feat_names = _featurize(rows)
        scaler = StandardScaler()
        X_norm = scaler.fit_transform(X)

        # K dinâmico
        k, km, silhouette = _choose_k(X_norm)
        labels = km.predict(X_norm)
        distances = np.linalg.norm(X_norm - km.cluster_centers_[labels], axis=1)
        logger.info("Escolhido K=%d (silhouette=%.3f) sobre %d leads", k, silhouette, len(rows))

        # Stats por cluster + LLM
        llm = LLMProvider(
            provider="anthropic",
            api_key=os.getenv("LLM_API_KEY", ""),
            model=os.getenv("LLM_MODEL", "claude-sonnet-4-20250514"),
        )

        clusters_payload = []
        for ci in range(k):
            cluster_rows = [r for r, lab in zip(rows, labels) if lab == ci]
            stats = _stats_for(cluster_rows)
            copy_data = await _gen_cluster_copy(llm, stats) if not dry_run else {
                "nome": f"Cluster {ci}", "hipotese": "", "copy_whatsapp": "",
                "copy_email_assunto": "", "copy_email_corpo": "",
            }
            clusters_payload.append({
                "cluster_index": ci,
                "stats": stats,
                "copy": copy_data,
                "rows": cluster_rows,
            })

        if dry_run:
            return {
                "status": "ok_dry_run",
                "n_leads": len(rows),
                "k": k,
                "silhouette": round(silhouette, 3),
                "clusters": [
                    {"index": c["cluster_index"], "stats": c["stats"]}
                    for c in clusters_payload
                ],
            }

        # Persistir
        run_id = (await session.execute(SQL_INSERT_RUN, {
            "since_date": since_date,
            "n_leads": len(rows),
            "n_clusters": k,
            "silhouette": round(silhouette, 3),
        })).scalar_one()

        cluster_ids: list[int] = []
        for c in clusters_payload:
            cid = (await session.execute(SQL_INSERT_CLUSTER, {
                "run_id": run_id,
                "cluster_index": c["cluster_index"],
                "label": c["copy"]["nome"],
                "hipotese": c["copy"]["hipotese"],
                "copy_wa": c["copy"]["copy_whatsapp"],
                "copy_email_assunto": c["copy"]["copy_email_assunto"],
                "copy_email_corpo": c["copy"]["copy_email_corpo"],
                "n_leads": c["stats"]["n"],
                "score_medio": c["stats"]["score_medio"],
                "score_mediano": c["stats"]["score_mediano"],
                "lifecycle": c["stats"]["lifecycle_dominante"],
                "dias_mediana": c["stats"]["days_mediana"],
                "metadados": json.dumps({
                    "fit_dominante": c["stats"]["fit_dominante"],
                    "interest_dominante": c["stats"]["interest_dominante"],
                }),
            })).scalar_one()
            cluster_ids.append(cid)

        # Assignments
        for i, r in enumerate(rows):
            await session.execute(SQL_INSERT_ASSIGN, {
                "run_id": run_id,
                "cluster_id": cluster_ids[int(labels[i])],
                "lead_id": r.id,
                "distance": float(distances[i]),
            })

        await session.commit()

    return {
        "status": "ok",
        "run_id": run_id,
        "n_leads": len(rows),
        "k": k,
        "silhouette": round(silhouette, 3),
        "clusters": [
            {
                "id": cluster_ids[c["cluster_index"]],
                "index": c["cluster_index"],
                "label": c["copy"]["nome"],
                "stats": c["stats"],
            }
            for c in clusters_payload
        ],
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="YYYY-MM-DD (default: nenhum filtro)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    sd = date.fromisoformat(args.since) if args.since else None
    out = asyncio.run(run_cluster_analysis(since_date=sd, dry_run=args.dry_run))
    print(json.dumps(out, indent=2, default=str, ensure_ascii=False))


if __name__ == "__main__":
    _main()
