import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

export const metadata = { title: 'BSSP — Auditoria' };

export default function Page() {
  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Auditoria</h1>
          <p className="text-sm text-muted-foreground">Linha do tempo unificada de ações no sistema.</p>
        </div>
        <Badge variant="warning">Sprint 0 — placeholder</Badge>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>Em construção</CardTitle>
          <CardDescription>Para todos com filtro por escopo. S2 entrega timeline básico.</CardDescription>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          Esta tela é um placeholder gerado pela Sprint 0. A implementação real
          chega nas próximas sprints conforme o roadmap em
          <em> Frontend_BSSP_Plano_de_Projeto.docx</em>.
        </CardContent>
      </Card>
    </div>
  );
}
