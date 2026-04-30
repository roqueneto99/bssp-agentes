import { redirect } from 'next/navigation';
import { auth } from '@/lib/auth';
import { homePathFor } from '@/lib/rbac';

export default async function HomePage() {
  const session = await auth();
  if (!session?.user) {
    redirect('/login');
  }
  redirect(homePathFor(session.user.role));
}
