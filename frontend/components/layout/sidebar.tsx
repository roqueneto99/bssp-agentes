"use client";
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import type { LucideIcon } from 'lucide-react';
import {
  LayoutDashboard, Users, Mail, BarChart3, Activity, FileSearch, Cog,
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { hasAnyRole, type Role } from '@/lib/rbac';

interface Item {
  label: string;
  href: string;
  icon: LucideIcon;
  roles: Role[];
}

const ITEMS: Item[] = [
  { label: 'Dashboard',  href: '/exec',       icon: LayoutDashboard, roles: ['admin', 'executive'] },
  { label: 'Pipeline',   href: '/leads',      icon: Users,           roles: ['admin', 'sales'] },
  { label: 'Cadências',  href: '/cadencias',  icon: Mail,            roles: ['admin', 'marketing'] },
  { label: 'Análise',    href: '/analise',    icon: BarChart3,       roles: ['admin', 'marketing', 'executive'] },
  { label: 'Operações',  href: '/ops',        icon: Activity,        roles: ['admin'] },
  { label: 'Auditoria',  href: '/auditoria',  icon: FileSearch,      roles: ['admin', 'sales', 'marketing'] },
  { label: 'Configurações', href: '/settings',icon: Cog,             roles: ['admin'] },
];

export function Sidebar({ role }: { role: Role | undefined }) {
  const pathname = usePathname();
  const visible = ITEMS.filter((i) => hasAnyRole(role, i.roles));
  return (
    <aside className="flex w-60 shrink-0 flex-col border-r bg-card px-3 py-4">
      <Link href="/" className="mb-6 px-3 text-lg font-semibold tracking-tight">
        BSSP <span className="text-muted-foreground font-normal">· Squads</span>
      </Link>
      <nav className="flex flex-col gap-1">
        {visible.map((it) => {
          const active = pathname === it.href || pathname.startsWith(it.href + '/');
          return (
            <Link
              key={it.href}
              href={it.href}
              className={cn(
                'flex items-center gap-3 rounded-md px-3 py-2 text-sm transition-colors',
                active
                  ? 'bg-primary text-primary-foreground'
                  : 'text-muted-foreground hover:bg-accent hover:text-accent-foreground',
              )}
            >
              <it.icon className="h-4 w-4" />
              <span>{it.label}</span>
            </Link>
          );
        })}
      </nav>
      <div className="mt-auto px-3 pt-4 text-xs text-muted-foreground">
        v0.1 · {role ?? 'sem role'}
      </div>
    </aside>
  );
}
