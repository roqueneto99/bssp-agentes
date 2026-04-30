import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

interface Props { params: { email: string } }

export default function LeadBriefingPage({ params }: Props) {
  const email = decodeURIComponent(params.email);
  return (
    <div className="flex flex-col gap-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">{email}</h1>
          <p className="text-sm text-muted-foreground">Briefing do lead</p>
        </div>
        <Badge variant="warning">Sprint 0 — placeholder</Badge>
      </div>
      <Card>
        <CardHeader>
          <CardTitle>Aguardando dados</CardTitle>
          <CardDescription>S1 conecta a /api/lead/{`{email}`} do FastAPI e renderiza scorecard, histórico, mensagens.</CardDescription>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          Endpoint atual: <code className="rounded bg-muted px-1.5 py-0.5">GET {`{BACKEND}`}/api/lead/{`{email}`}</code>
        </CardContent>
      </Card>
    </div>
  );
}
