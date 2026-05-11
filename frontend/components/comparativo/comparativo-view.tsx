'use client';
/**
 * frontend/components/comparativo/comparativo-view.tsx
 *
 * Tabela com tabs por bucket + drawer de detalhe.
 * Carrega /api/comparativo/itens via apiFetch do server (proxy interno do Next).
 */
import { useEffect, useState, useTransition } from 'react';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Table, TableHeader, TableRow, TableHead, TableBody, TableCell,
} from '@/components/ui/table';
import {
  Sheet, SheetContent, SheetHeader, SheetTitle, SheetDescription,
} from '@/components/ui/sheet';

type Bucket = 'matched_aligned' | 'matched_divergent' | 'only_rd' | 'only_hablla';
type Dim = 'stage' | 'classification' | 'last_interaction' | 'origin';

interface LeadComparison {
  id: number;
  lead_id: number | null;
  hablla_only_id: number | null;
  bucket: Bucket;
  match_key: string | null;
  match_score: number | null;
  name: string | null;
  email: string | null;
  phone: string | null;
  diffs: Dim[];
  rd_stage: string | null;
  hablla_stage: string | null;
  rd_classification: string | null;
  hablla_classification: string | null;
  rd_last_interaction: string | null;
  hablla_last_interaction: string | null;
  rd_origin: string | null;
  hablla_origin: string | null;
  updated_in_last_24h: boolean;
}

interface Page {
  items: LeadComparison[];
  page: number;
  size: number;
  total: number;
}

const BUCKET_LABEL: Record<Bucket, string> = {
  matched_aligned:   'Alinhados',
  matched_divergent: 'Divergentes',
  only_rd:           'Só no RD',
  only_hablla:       'Só no Hablla',
};

const DIM_LABEL: Record<Dim, string> = {
  stage: 'Estágio',
  classification: 'Classificação',
  last_interaction: 'Última interação',
  origin: 'Origem',
};

export function ComparativoView({
  date,
  resumo,
}: {
  date: string;
  resumo: { matched_divergent: number };
}) {
  const [bucket, setBucket] = useState<Bucket>('matched_divergent');
  const [q, setQ] = useState('');
  const [onlyDelta, setOnlyDelta] = useState(true);
  const [data, setData] = useState<Page | null>(null);
  const [detail, setDetail] = useState<LeadComparison | null>(null);
  const [pending, startTransition] = useTransition();

  useEffect(() => {
    const url = new URL('/proxy/api/comparativo/itens', window.location.origin);
    url.searchParams.set('date', date);
    url.searchParams.set('bucket', bucket);
    if (q.length >= 2) url.searchParams.set('q', q);
    if (onlyDelta) url.searchParams.set('only_updated_24h', 'true');
    url.searchParams.set('size', '50');

    startTransition(async () => {
      const res = await fetch(url, { cache: 'no-store' });
      const j: Page = await res.json();
      setData(j);
    });
  }, [date, bucket, q, onlyDelta]);

  return (
    <>
      <Tabs value={bucket} onValueChange={(v) => setBucket(v as Bucket)}>
        <div className="flex flex-wrap items-center gap-3">
          <TabsList>
            {(Object.keys(BUCKET_LABEL) as Bucket[]).map((b) => (
              <TabsTrigger key={b} value={b}>{BUCKET_LABEL[b]}</TabsTrigger>
            ))}
          </TabsList>
          <Input
            placeholder="Buscar por nome ou e-mail…"
            className="max-w-xs"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
          <Button
            variant={onlyDelta ? 'default' : 'outline'}
            size="sm"
            onClick={() => setOnlyDelta((v) => !v)}
          >
            {onlyDelta ? '✓ ' : ''}só últimas 24h
          </Button>
        </div>

        {(Object.keys(BUCKET_LABEL) as Bucket[]).map((b) => (
          <TabsContent key={b} value={b} className="mt-4">
            <ItemsTable
              data={data}
              loading={pending}
              onSelect={setDetail}
            />
          </TabsContent>
        ))}
      </Tabs>

      <Sheet open={!!detail} onOpenChange={(o) => !o && setDetail(null)}>
        <SheetContent className="sm:max-w-2xl">
          {detail && <Detail item={detail} />}
        </SheetContent>
      </Sheet>
    </>
  );
}

function ItemsTable({
  data,
  loading,
  onSelect,
}: {
  data: Page | null;
  loading: boolean;
  onSelect: (i: LeadComparison) => void;
}) {
  if (loading && !data) return <p className="text-sm text-muted-foreground py-8">Carregando…</p>;
  if (!data || data.items.length === 0) {
    return <p className="text-sm text-muted-foreground py-8">Nenhum lead nesta categoria para o período.</p>;
  }
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Lead</TableHead>
          <TableHead>Match</TableHead>
          <TableHead>Diffs</TableHead>
          <TableHead>RD → Hablla</TableHead>
          <TableHead className="text-right">24h?</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {data.items.map((it) => (
          <TableRow
            key={it.id}
            onClick={() => onSelect(it)}
            className="cursor-pointer hover:bg-muted/40"
          >
            <TableCell>
              <div className="font-medium">{it.name ?? '—'}</div>
              <div className="text-xs text-muted-foreground">{it.email ?? it.phone ?? '—'}</div>
            </TableCell>
            <TableCell>
              {it.match_key ? (
                <Badge variant="outline" className="font-mono text-xs">
                  {it.match_key} · {(it.match_score ?? 0).toFixed(2)}
                </Badge>
              ) : <span className="text-xs text-muted-foreground">—</span>}
            </TableCell>
            <TableCell>
              <div className="flex flex-wrap gap-1">
                {it.diffs.length === 0
                  ? <span className="text-xs text-muted-foreground">alinhado</span>
                  : it.diffs.map((d) => (
                      <Badge key={d} variant="secondary" className="text-xs">
                        {DIM_LABEL[d]}
                      </Badge>
                    ))}
              </div>
            </TableCell>
            <TableCell className="text-xs">
              <span className="text-muted-foreground">{it.rd_stage ?? '—'}</span>
              {' → '}
              <span>{it.hablla_stage ?? '—'}</span>
            </TableCell>
            <TableCell className="text-right">
              {it.updated_in_last_24h && (
                <Badge className="text-[10px]">delta</Badge>
              )}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function Detail({ item }: { item: LeadComparison }) {
  const rows: Array<[string, string | null, string | null, boolean]> = [
    ['Estágio',           item.rd_stage,            item.hablla_stage,            item.diffs.includes('stage')],
    ['Classificação',     item.rd_classification,   item.hablla_classification,   item.diffs.includes('classification')],
    ['Última interação',  item.rd_last_interaction, item.hablla_last_interaction, item.diffs.includes('last_interaction')],
    ['Origem/Curso',      item.rd_origin,           item.hablla_origin,           item.diffs.includes('origin')],
  ];
  return (
    <>
      <SheetHeader>
        <SheetTitle>{item.name ?? item.email ?? 'Lead'}</SheetTitle>
        <SheetDescription>
          {item.email ?? '—'} · match: <code>{item.match_key ?? 'sem match'}</code>
        </SheetDescription>
      </SheetHeader>
      <div className="mt-6 space-y-3">
        {rows.map(([label, rd, h, divergent]) => (
          <div
            key={label}
            className={`grid grid-cols-3 gap-3 py-3 px-3 rounded-md ${
              divergent ? 'bg-yellow-50 dark:bg-yellow-950/30' : ''
            }`}
          >
            <div className="text-sm font-medium">{label}</div>
            <div className="text-sm">
              <div className="text-xs text-muted-foreground">RD</div>
              {rd ?? <span className="text-muted-foreground">—</span>}
            </div>
            <div className="text-sm">
              <div className="text-xs text-muted-foreground">Hablla</div>
              {h ?? <span className="text-muted-foreground">—</span>}
            </div>
          </div>
        ))}
      </div>
    </>
  );
}
