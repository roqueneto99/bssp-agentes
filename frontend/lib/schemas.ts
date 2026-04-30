import { z } from 'zod';

export const Role = z.enum(['admin', 'sales', 'marketing', 'executive']);
export type Role = z.infer<typeof Role>;

export const Classificacao = z.enum(['SQL', 'MQL', 'SAL', 'COLD']);
export type Classificacao = z.infer<typeof Classificacao>;

export const StatusComercial = z.enum([
  'novo', 'em_conversa', 'ganho', 'perdido', 'pausado',
]);
export type StatusComercial = z.infer<typeof StatusComercial>;

export const Canal = z.enum(['email', 'whatsapp', 'sms']);
export type Canal = z.infer<typeof Canal>;

export const Lead = z.object({
  email: z.string().email(),
  nome: z.string().nullable(),
  telefone: z.string().nullable(),
  curso_interesse: z.string().nullable(),
  cidade: z.string().nullable(),
  classificacao: Classificacao,
  score: z.number().min(0).max(100),
  dimensoes: z.object({
    fit: z.number(),
    interesse: z.number(),
    engajamento: z.number(),
    timing: z.number(),
  }),
  cadencia_ativa: z.string().nullable(),
  status_comercial: StatusComercial,
  ultima_atividade_at: z.string().datetime().nullable(),
  consultor_atribuido: z.string().email().nullable(),
});
export type Lead = z.infer<typeof Lead>;

export const PassoCadencia = z.object({
  ordem: z.number(),
  dia: z.number(),
  canal: Canal,
  nudge: z.string(),
  template_id: z.string(),
});
export type PassoCadencia = z.infer<typeof PassoCadencia>;

export const Cadencia = z.object({
  id: z.number(),
  nome: z.string(),
  rota: z.string().nullable(),
  ativo: z.boolean(),
  passos: z.array(PassoCadencia),
  janela_total_d: z.number(),
  metricas_30d: z
    .object({
      enviadas: z.number(),
      entregues: z.number(),
      abertas: z.number(),
      cliques: z.number(),
      respostas: z.number(),
      bounce: z.number(),
    })
    .optional(),
});
export type Cadencia = z.infer<typeof Cadencia>;

export const Template = z.object({
  id: z.string(),
  version: z.string(),
  nudge: z.string(),
  canal: Canal,
  assunto: z.string().nullable(),
  corpo: z.string(),
  variaveis_obrigatorias: z.array(z.string()),
  variaveis_opcionais: z.array(z.string()),
  tons_suportados: z.array(z.string()),
  aprovado_por: z.string().nullable(),
  aprovado_em: z.string().datetime().nullable(),
  observacoes: z.string().nullable(),
});
export type Template = z.infer<typeof Template>;

export const HealthLight = z.enum(['green', 'amber', 'red', 'unknown']);
export type HealthLight = z.infer<typeof HealthLight>;

export const ComponentHealth = z.object({
  nome: z.string(),
  status: HealthLight,
  ultimo_check_at: z.string().datetime().nullable(),
  detalhes: z.string().nullable(),
});
export type ComponentHealth = z.infer<typeof ComponentHealth>;
