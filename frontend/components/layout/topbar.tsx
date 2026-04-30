"use client";
import { signOut } from 'next-auth/react';
import { LogOut } from 'lucide-react';
import {
  Avatar, AvatarFallback, AvatarImage,
} from '@/components/ui/avatar';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem,
  DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';

interface Props {
  name?: string | null;
  email?: string | null;
  image?: string | null;
}

export function Topbar({ name, email, image }: Props) {
  const initials = (name ?? email ?? '?')
    .split(' ').map((s) => s[0]).join('').slice(0, 2).toUpperCase();
  return (
    <header className="flex h-14 items-center justify-between border-b bg-background px-6">
      <div className="text-sm text-muted-foreground">Frontend de Gerenciamento das Squads</div>
      <DropdownMenu>
        <DropdownMenuTrigger className="flex items-center gap-2 rounded-md p-1.5 hover:bg-accent">
          <Avatar className="h-8 w-8">
            {image ? <AvatarImage src={image} alt={name ?? ''} /> : null}
            <AvatarFallback>{initials}</AvatarFallback>
          </Avatar>
          <span className="text-sm font-medium">{name ?? email}</span>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-56">
          <DropdownMenuLabel className="text-xs text-muted-foreground">{email}</DropdownMenuLabel>
          <DropdownMenuSeparator />
          <DropdownMenuItem onClick={() => signOut({ callbackUrl: '/login' })} className="text-destructive">
            <LogOut className="mr-2 h-4 w-4" /> Sair
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </header>
  );
}
