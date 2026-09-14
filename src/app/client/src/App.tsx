import { createBrowserRouter, RouterProvider, Outlet } from 'react-router';
import { useEffect, useState } from 'react';
import { Avatar, AvatarFallback } from '@databricks/appkit-ui/react';
import { ExpedientsPage } from './pages/ExpedientsPage';

interface WhoAmI {
  name: string;
  email: string;
  isRevisor?: boolean;
}

function initials(name: string, email: string): string {
  const base = name || email || '?';
  const parts = base.replace(/[._-]+/g, ' ').split(' ').filter(Boolean);
  const letters = (parts.length >= 2 ? parts[0][0] + parts[1][0] : base.slice(0, 2)) || '?';
  return letters.toUpperCase();
}

function UserBadge() {
  const [me, setMe] = useState<WhoAmI | null>(null);
  useEffect(() => {
    fetch('/api/whoami')
      .then((r) => (r.ok ? (r.json() as Promise<WhoAmI>) : null))
      .then((d) => {
        if (d) setMe(d);
      })
      .catch(() => {});
  }, []);
  if (!me) return null;
  return (
    <div className="ml-auto flex items-center gap-2.5">
      <div className="text-right leading-tight hidden sm:block">
        <div className="text-sm font-medium text-foreground flex items-center gap-1.5 justify-end">
          {me.name}
          {me.isRevisor && (
            <span className="text-[10px] uppercase tracking-wide rounded bg-primary/10 text-primary px-1.5 py-0.5">
              Revisor
            </span>
          )}
        </div>
        {me.email && <div className="text-xs text-muted-foreground">{me.email}</div>}
      </div>
      <Avatar className="h-8 w-8">
        <AvatarFallback className="text-xs">{initials(me.name, me.email)}</AvatarFallback>
      </Avatar>
    </div>
  );
}

function Layout() {
  return (
    <div className="min-h-screen bg-background flex flex-col">
      <header className="border-b px-4 md:px-6 py-3 flex items-center gap-3">
        <h1 className="text-lg font-semibold text-foreground">RevisioFitxers</h1>
        <span className="text-sm text-muted-foreground hidden md:inline">
          Classificació de documents d'expedients
        </span>
        <UserBadge />
      </header>
      <main className="flex-1 p-4 md:p-6">
        <Outlet />
      </main>
    </div>
  );
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [{ path: '/', element: <ExpedientsPage /> }],
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}
