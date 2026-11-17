import { createServer } from 'node:net';

import type { Event } from 'nostr-tools';
import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { WebSocketServer, type WebSocket } from 'ws';

const TOKEN = 'publisher-e2e-token';

interface WorkerDev {
  fetch: (
    input: string,
    init?: { method?: string; headers?: Record<string, string>; body?: string }
  ) => Promise<Response>;
  stop: () => Promise<void>;
}

class LoopbackRelay {
  readonly received: string[] = [];
  mode: 'accept' | 'reject' | 'silent' = 'accept';
  url = '';
  private server: WebSocketServer | null = null;

  async start(): Promise<void> {
    this.server = new WebSocketServer({ host: '127.0.0.1', port: 0 });
    await new Promise<void>((resolve) => {
      this.server?.once('listening', () => resolve());
    });
    const address = this.server.address();
    if (!address || typeof address === 'string') {
      throw new Error('Loopback relay did not bind');
    }
    this.url = `ws://127.0.0.1:${address.port}`;
    this.server.on('connection', (socket: WebSocket) => {
      socket.on('message', (data: Buffer | string) => {
        const text = typeof data === 'string' ? data : data.toString('utf8');
        let parsed: unknown;
        try {
          parsed = JSON.parse(text);
        } catch {
          return;
        }
        if (!Array.isArray(parsed) || parsed[0] !== 'EVENT') return;
        const event = parsed[1] as { id?: string } | undefined;
        const id = event?.id ?? '';
        this.received.push(id);
        if (this.mode === 'silent') return;
        const ok = this.mode === 'accept';
        socket.send(JSON.stringify(['OK', id, ok, ok ? '' : 'blocked: rejected']));
      });
    });
  }

  async stop(): Promise<void> {
    const server = this.server;
    this.server = null;
    if (!server) return;
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  }
}

function signedEvent(content: string): Event {
  return finalizeEvent(
    {
      kind: 1,
      created_at: Math.floor(Date.now() / 1000),
      tags: [],
      content,
    },
    generateSecretKey()
  );
}

async function post(
  worker: WorkerDev,
  path: string,
  body: string,
  headers: Record<string, string> = {}
): Promise<{ status: number; text: string }> {
  const response = await worker.fetch(`http://publisher.e2e${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...headers,
    },
    body,
  });
  return { status: response.status, text: await response.text() };
}

function publishBody(event: Event, relay: string, extra = ''): string {
  return JSON.stringify({
    relays: [relay],
    event,
    ...(extra ? { padding: extra } : {}),
  });
}

describe('Cloudflare publisher worker against a loopback relay', () => {
  const relay = new LoopbackRelay();
  let worker: WorkerDev | null = null;

  beforeAll(async () => {
    await relay.start();
    const { unstable_dev: startWorker } = await import('wrangler');
    worker = (await startWorker('src/index.ts', {
      config: 'wrangler.worker-e2e.toml',
      local: true,
      ip: '127.0.0.1',
      port: 0,
      vars: {
        API_TOKEN: TOKEN,
        ENVIRONMENT: 'test',
        ALLOW_PRIVATE_RELAYS: 'true',
        ALLOW_UNAUTHENTICATED_PUBLISH: 'false',
      },
      experimental: { disableExperimentalWarning: true },
    })) as unknown as WorkerDev;
  });

  afterAll(async () => {
    await worker?.stop();
    await relay.stop();
  });

  function requireWorker(): WorkerDev {
    if (!worker) throw new Error('Worker did not start');
    return worker;
  }

  it('publishes a signed event to the loopback relay', async () => {
    relay.mode = 'accept';
    const event = signedEvent('signed');
    const result = await post(requireWorker(), '/publish', publishBody(event, relay.url), {
      'API-TOKEN': TOKEN,
    });
    expect(result.status, result.text).toBe(200);
    expect(result.text).toContain('"ok":true');
    expect(relay.received).toContain(event.id);
  });

  it('rejects an event whose signature was replaced', async () => {
    const event = signedEvent('tampered');
    event.sig = '0'.repeat(128);
    const result = await post(requireWorker(), '/publish', publishBody(event, relay.url), {
      'API-TOKEN': TOKEN,
    });
    expect(result.status, result.text).toBe(400);
    expect(result.text).toContain('signature is invalid');
    expect(relay.received).not.toContain(event.id);
  });

  it('rejects a missing token and a wrong token', async () => {
    const event = signedEvent('auth');
    const missing = await post(requireWorker(), '/publish', publishBody(event, relay.url));
    expect(missing.status, missing.text).toBe(403);
    expect(missing.text).toContain('INVALID_TOKEN');

    const wrong = await post(requireWorker(), '/publish', publishBody(event, relay.url), {
      'API-TOKEN': 'not-the-token',
    });
    expect(wrong.status, wrong.text).toBe(403);
    expect(relay.received).not.toContain(event.id);
  });

  it('reports a relay rejection without treating it as accepted', async () => {
    relay.mode = 'reject';
    const event = signedEvent('rejected');
    const result = await post(
      requireWorker(),
      '/publish?policy=aggressive',
      publishBody(event, relay.url),
      { 'API-TOKEN': TOKEN }
    );
    expect(result.status, result.text).toBe(200);
    expect(result.text).toContain('"ok":false');
    relay.mode = 'accept';
  });

  it('recovers the same event after the relay is unreachable, then refuses a duplicate', async () => {
    const closed = await reserveClosedPort();
    const event = signedEvent('recover');
    const outage = await post(
      requireWorker(),
      '/publish?policy=aggressive',
      publishBody(event, `ws://127.0.0.1:${closed}`),
      { 'API-TOKEN': TOKEN }
    );
    expect(outage.status, outage.text).toBe(200);
    expect(outage.text).toContain('"ok":false');

    relay.mode = 'silent';
    const hung = await post(
      requireWorker(),
      '/publish?policy=aggressive',
      publishBody(signedEvent('timeout'), relay.url),
      { 'API-TOKEN': TOKEN }
    );
    expect(hung.status, hung.text).toBe(200);
    expect(hung.text).toContain('"ok":false');

    relay.mode = 'accept';
    const recovered = await post(
      requireWorker(),
      '/publish?policy=aggressive',
      publishBody(event, relay.url),
      { 'API-TOKEN': TOKEN }
    );
    expect(recovered.status, recovered.text).toBe(200);
    expect(recovered.text).toContain('"ok":true');
    expect(relay.received.filter((id) => id === event.id)).toEqual([event.id]);

    const duplicate = await post(
      requireWorker(),
      '/publish?policy=aggressive',
      publishBody(event, relay.url),
      { 'API-TOKEN': TOKEN }
    );
    expect(duplicate.status, duplicate.text).toBe(409);
    expect(relay.received.filter((id) => id === event.id)).toEqual([event.id]);
  });

  it('rejects credentialed relay URLs and oversized bodies', async () => {
    const event = signedEvent('limits');
    const credentialed = await post(
      requireWorker(),
      '/publish',
      publishBody(event, 'wss://user:pass@127.0.0.1:1'),
      { 'API-TOKEN': TOKEN }
    );
    expect(credentialed.status, credentialed.text).toBe(400);
    expect(credentialed.text).toContain('not allowed');

    const oversized = await post(
      requireWorker(),
      '/publish',
      publishBody(event, relay.url, 'x'.repeat(1_100_000)),
      { 'API-TOKEN': TOKEN }
    );
    expect(oversized.status, oversized.text).toBe(400);
    expect(oversized.text).toContain('too large');
    expect(relay.received).not.toContain(event.id);
  });
});

async function reserveClosedPort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('Could not reserve a port');
  }
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) reject(error);
      else resolve();
    });
  });
  return address.port;
}
