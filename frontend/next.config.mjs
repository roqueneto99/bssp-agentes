/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  poweredByHeader: false,
  // typedRoutes ficará para a Sprint 1, junto da rota dinâmica de leads.
  // O backend FastAPI roda no mesmo projeto Railway. Pode ser proxied via /api/be/*
  // ou consumido direto via NEXT_PUBLIC_BACKEND_URL no client.
  async rewrites() {
    const backend = process.env.BACKEND_URL || '';
    if (!backend) return [];
    return [
      { source: '/api/be/:path*', destination: `${backend}/api/:path*` },
      { source: '/api/be-webhook/:path*', destination: `${backend}/webhooks/:path*` },
    ];
  },
};

export default nextConfig;
