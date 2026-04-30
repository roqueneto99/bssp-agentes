import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

export const metadata = { title: 'BSSP — Agentes' };

export default function Page() {
  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Agentes</h1>
          <p className="text-sm text-muted-foreground">Estado de cada agente, latências, taxa de erro.</p>
        </div>
        <Badge variant="warning">Sprint 0 — placeholder</Badge>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>Em construção</CardTitle>
          <CardDescription>Admin. S5 entrega.</CardDescription>
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
