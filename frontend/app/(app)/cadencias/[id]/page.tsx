import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';

interface Props { params: { id: string } }

export default function CadenciaPage({ params }: Props) {
  return (
    <div className="flex flex-col gap-6">
      <h1 className="text-2xl font-semibold tracking-tight">Cadência #{params.id}</h1>
      <Card>
        <CardHeader>
          <CardTitle>Editor de cadência</CardTitle>
          <CardDescription>Sprint 3 implementa drag & drop de passos.</CardDescription>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">Placeholder.</CardContent>
      </Card>
    </div>
  );
}
