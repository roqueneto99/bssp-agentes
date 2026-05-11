/**
 * frontend/app/(app)/comparativo/page.tsx
 * ========================================
 *
 * Página /comparativo — visão diária RD Station x Hablla.
 *
 * Layout (mobile-first, estética Notion/Stripe):
 *   1. Header com seletor de data + toggle "só atualizados 24h"
 *   2. Grid 4 cards (matched_aligned | divergent | only_rd | only_hablla)
 *   3. Linha de chips com contagem por dimensão divergente
 *   4. Tabs (bucket) → Tabela de leads (paginada) com badge de diff
 *   5. Drawer com o detalhe lado-a-lado RD vs Hablla
 *
 * Server Component que pré-carrega o resumo; tabela e drawer são client.
 */
import { Suspense } from 'react';
import { apiFetch } from '@/lib/api';
import { ComparativoView } from '@/components/comparativo/comparativo-view';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';

export const dynamic = 'force-dynamic';

interface ResumoComparativo {
  snapshot_date: string;
  total: number;
  matched_aligned: number;
  matched_divergent: number;
  only_rd: number;
  only_hablla: number;
  divergent_stage: number;
  divergent_classification: number;
  divergent_last_interaction: number;
  divergent_origin: number;
  matched_by_external_id: number;
  matched_by_email: number;
  matched_by_phone: number;
  matched_by_name: number;
  novos_24h: number;
  atualizados_24h: number;
  computed_at: string;
}

interface PageProps {
  searchParams: Promise<{ date?: string }>;
}

export default async function ComparativoPage({ searchParams }: PageProps) {
  const params = await searchParams;
  const date = params.date ?? new Date().toISOString().slice(0, 10);

  const resumo = await apiFetch<ResumoComparativo>(
    `/api/comparativo/resumo?date=${date}`,
    { throwOnError: false },
  ).catch(() => null);

  return (
    <div className="space-y-6 p-6 max-w-7xl mx-auto">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">
            Comparativo RD Station × Hablla
          </h1>
          <p className="text-sm text-muted-foreground">
            Snapshot diário gerado às 03:00 BRT pelo cron de sync.
          </p>
        </div>
        <Badge variant="outline" className="font-mono">
          {resumo?.snapshot_date ?? date}
        </Badge>
      </header>

      {!resumo ? (
        <EmptyState date={date} />
      ) : (
        <>
          <KpiGrid resumo={resumo} />
          <DiffChips resumo={resumo} />
          <Suspense fallback={<Skeleton className="h-[600px] w-full" />}>
            <ComparativoView date={date} resumo={resumo} />
          </Suspense>
        </>
      )}
    </div>
  );
}

function KpiGrid({ resumo }: { resumo: ResumoComparativo }) {
  const cards = [
    {
      title: 'Alinhados',
      value: resumo.matched_aligned,
      hint: 'RD e Hablla concordam em todas as dimensões',
      tone: 'success' as const,
    },
    {
      title: 'Divergentes',
      value: resumo.matched_divergent,
      hint: 'Mesmo lead, valores diferentes nos dois sistemas',
      tone: 'warning' as const,
    },
    {
      title: 'Só no RD',
      value: resumo.only_rd,
      hint: 'Lead existe no RD, não foi achado no Hablla',
      tone: 'info' as const,
    },
    {
      title: 'Só no Hablla',
      value: resumo.only_hablla,
      hint: 'Lead existe no Hablla, não foi achado no RD',
      tone: 'info' as const,
    },
  ];

  return (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
      {cards.map((c) => (
        <Card key={c.title}>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              {c.title}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-semibold">{c.value.toLocaleString('pt-BR')}</div>
            <p className="text-xs text-muted-foreground mt-1">{c.hint}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function DiffChips({ resumo }: { resumo: ResumoComparativo }) {
  const dims = [
    { key: 'stage',              label: 'Estágio',        n: resumo.divergent_stage },
    { key: 'classification',     label: 'Classificação',  n: resumo.divergent_classification },
    { key: 'last_interaction',   label: 'Última interação', n: resumo.divergent_last_interaction },
    { key: 'origin',             label: 'Origem/Curso',   n: resumo.divergent_origin },
  ];
  return (
    <div className="flex flex-wrap gap-2 items-center">
      <span className="text-xs text-muted-foreground">Divergências por dimensão:</span>
      {dims.map((d) => (
        <Badge key={d.key} variant="secondary" className="font-normal">
          {d.label} · <span className="font-semibold ml-1">{d.n}</span>
        </Badge>
      ))}
      {resumo.atualizados_24h > 0 && (
        <Badge variant="outline" className="font-normal ml-auto">
          {resumo.atualizados_24h} mexeram nas últimas 24h
        </Badge>
      )}
    </div>
  );
}

function EmptyState({ date }: { date: string }) {
  return (
    <Card>
      <CardContent className="py-12 text-center space-y-2">
        <p className="text-lg font-medium">Sem snapshot para {date}</p>
        <p className="text-sm text-muted-foreground">
          O job de comparação roda às 03:00 BRT. Se a data é hoje e ainda não rodou,
          aguarde — ou acione manualmente em <code>/api/comparativo/rerun</code>.
        </p>
      </CardContent>
    </Card>
  );
}
