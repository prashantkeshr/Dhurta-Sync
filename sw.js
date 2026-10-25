/* ============================================================
   DHURTA Sync — Service Worker v12
   Network-first navigation · SWR vendor assets · Stream proxy
   ============================================================ */

const CACHE        = 'dhurta-v13';
const VENDOR_CACHE = 'dhurta-vendor-v1';

const CORE_ASSETS = [
  './', './index.html', './style.css', './app.js?v=13',
  './manifest.json', './sync.png', './sync.svg',
  './og-image.svg', './robots.txt', './llms.txt', './version.json',
];
const VENDOR_ASSETS = [
  'https://cdn.jsdelivr.net/npm/mqtt@5.3.5/dist/mqtt.min.js',
  'https://cdn.jsdelivr.net/npm/jsqr@1.4.0/dist/jsQR.min.js',
  // qrcode generation uses api.qrserver.com at render time — no library needed
];

/* ── In-memory stream controllers for large-file proxy ── */
const downloadStreams = new Map();

/* ── Install: pre-cache app + vendor assets ── */
self.addEventListener('install', e => {
  e.waitUntil(
    Promise.all([
      caches.open(CACHE).then(c =>
        Promise.allSettled(CORE_ASSETS.map(a => c.add(a)))
      ),
      caches.open(VENDOR_CACHE).then(c =>
        Promise.allSettled(VENDOR_ASSETS.map(a => c.add(a)))
      ),
    ]).then(() => self.skipWaiting())
  );
});

/* ── Activate: purge stale caches, claim all clients ── */
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys
          .filter(k => k !== CACHE && k !== VENDOR_CACHE)
          .map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

/* ── Fetch ── */
self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  /* ── 1. Streaming download proxy (for large files > 100 MB) ── */
  if (url.pathname === '/stream-download/') {
    const id   = url.searchParams.get('id')   || '';
    const name = url.searchParams.get('name') || 'download';
    const mime = url.searchParams.get('mime') || 'application/octet-stream';
    if (!id) return;
    const stream = new ReadableStream({
      start(ctrl) { downloadStreams.set(id, ctrl); },
      cancel()    { downloadStreams.delete(id);    },
    });
    e.respondWith(new Response(stream, {
      headers: {
        'Content-Type': mime,
        'Content-Disposition': `attachment; filename="${encodeURIComponent(name)}"`,
        'Transfer-Encoding': 'chunked',
      },
    }));
    return;
  }

  /* ── 2. Skip MQTT / ntfy brokers ── */
  if (url.hostname.includes('emqx')      ||
      url.hostname.includes('hivemq')    ||
      url.hostname.includes('mosquitto') ||
      url.hostname.includes('ntfy.sh'))  return;

  /* ── 3. Network-first for navigation (never serve stale index.html) ── */
  if (req.mode === 'navigate') {
    e.respondWith(
      fetch(req).then(resp => {
        if (resp && resp.status === 200) {
          caches.open(CACHE).then(c => c.put(req, resp.clone()));
        }
        return resp;
      }).catch(() =>
        caches.match(req)
          .then(c => c || caches.match('./'))
      )
    );
    return;
  }

  /* ── 4. Stale-while-revalidate for CDN vendor assets ── */
  if (url.hostname.includes('jsdelivr') || url.hostname.includes('cdnjs')) {
    e.respondWith(
      caches.open(VENDOR_CACHE).then(async cache => {
        const cached  = await cache.match(req);
        const netProm = fetch(req).then(resp => {
          if (resp && resp.status === 200) cache.put(req, resp.clone());
          return resp;
        }).catch(() => null);
        return cached ?? netProm;
      })
    );
    return;
  }

  /* ── 5. Cache-first for all other static local assets ── */
  e.respondWith(
    caches.match(req).then(cached => {
      if (cached) return cached;
      return fetch(req).then(resp => {
        if (!resp || resp.status !== 200 || resp.type === 'opaque') return resp;
        caches.open(CACHE).then(c => c.put(req, resp.clone()));
        return resp;
      });
    })
  );
});

/* ── Message bus ── */
self.addEventListener('message', e => {
  const d = e.data;
  if (!d) return;

  /* Force-update trigger from app */
  if (d.type === 'SKIP_WAITING') {
    self.skipWaiting();
    return;
  }

  /* Streaming download chunk feed */
  if (d.type === 'STREAM_CHUNK') {
    const ctrl = downloadStreams.get(d.id);
    if (ctrl) {
      try { ctrl.enqueue(d.chunk instanceof Uint8Array ? d.chunk : new Uint8Array(d.chunk)); }
      catch {}
    }
    return;
  }
  if (d.type === 'STREAM_DONE') {
    const ctrl = downloadStreams.get(d.id);
    if (ctrl) { try { ctrl.close(); } catch {} downloadStreams.delete(d.id); }
    return;
  }
  if (d.type === 'STREAM_ERROR') {
    const ctrl = downloadStreams.get(d.id);
    if (ctrl) { try { ctrl.error(new Error('Transfer aborted')); } catch {} downloadStreams.delete(d.id); }
    return;
  }

  /* Manual in-app notification */
  if (d.type === 'SHOW_NOTIFICATION') {
    self.registration.showNotification(d.title || '⚡ Dhurta Sync', {
      body:     d.body   || '',
      icon:    './sync.png',
      badge:   './sync.png',
      tag:      d.tag    || 'dhurta',
      renotify: true,
      vibrate: [150, 80, 150],
      data:    { url: 'https://sync.dhurta.com' },
    });
  }
});

/* ── Web Push ── */
self.addEventListener('push', e => {
  let data = {};
  try { data = e.data.json(); } catch { data = { title:'Dhurta Sync', body: e.data?.text() || '' }; }
  e.waitUntil(self.registration.showNotification(data.title || '⚡ Dhurta Sync', {
    body:     data.body    || 'You have a new message',
    icon:    './sync.png',
    badge:   './sync.png',
    tag:      data.tag     || 'dhurta',
    renotify: true,
    vibrate: [200, 100, 200],
    data:    { url: data.url || 'https://sync.dhurta.com' },
    actions: [
      { action: 'open',    title: 'Open App' },
      { action: 'dismiss', title: 'Dismiss'  },
    ],
  }));
});

self.addEventListener('notificationclick', e => {
  e.notification.close();
  if (e.action === 'dismiss') return;
  const url = e.notification.data?.url || 'https://sync.dhurta.com';
  e.waitUntil(
    clients.matchAll({ type:'window', includeUncontrolled:true }).then(cs => {
      const c = cs.find(c => c.url.includes('sync.dhurta'));
      return c ? c.focus() : clients.openWindow(url);
    })
  );
});
