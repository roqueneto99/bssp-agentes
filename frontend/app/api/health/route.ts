import { NextResponse } from 'next/server';

export async function GET() {
  return NextResponse.json({
    status: 'ok',
    service: 'bssp-frontend',
    time: new Date().toISOString(),
    version: process.env.RAILWAY_GIT_COMMIT_SHA?.slice(0, 7) ?? 'dev',
  });
}
