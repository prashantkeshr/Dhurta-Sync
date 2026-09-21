/* ============================================================
   DHURTA Sync — Service Worker (sw.js)
   Cache-first PWA · Push notification handler
   ============================================================ */

const CACHE = 'dhurta-v7';
const CORE_ASSETS = [
  './',
  './index.html',
  './style.css',
  './app.js',
  './manifest.json',
  './sync.png',
  './sync.svg',
  './og-image.svg',
  './robots.txt',
  './llms.txt',
  'https://cdn.jsdelivr.net/npm/mqtt@5.3.5/dist/mqtt.min.js',
  'https://cdn.jsdelivr.net/npm/jsqr@1.4.0/dist/jsQR.min.js',
];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(cache => cache.addAll(CORE_ASSETS)).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  if (e.request.url.includes('broker.emqx') ||
      e.request.url.includes('hivemq') ||
      e.request.url.includes('mosquitto') ||
      e.request.url.includes('ntfy.sh')) return;

  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached;
      return fetch(e.request).then(resp => {
        if (!resp || resp.status !== 200 || resp.type === 'opaque') return resp;
        const clone = resp.clone();
        caches.open(CACHE).then(cache => cache.put(e.request, clone));
        return resp;
      });
    })
  );
});

/* ── Push notifications (Web Push / ntfy.sh) ── */
self.addEventListener('push', e => {
  let data = {};
  try { data = e.data.json(); } catch { data = { title: 'Dhurta Sync', body: e.data?.text()||'' }; }
  const title = data.title || '⚡ Dhurta Sync';
  const opts = {
    body:    data.body   || 'You have a new message',
    icon:    './sync.png',
    badge:   './sync.png',
    tag:     data.tag    || 'dhurta',
    renotify: true,
    vibrate: [200, 100, 200],
    data:    { url: data.url || 'https://sync.dhurta.com' },
    actions: [
      { action: 'open',    title: 'Open App' },
      { action: 'dismiss', title: 'Dismiss' },
    ],
  };
  e.waitUntil(self.registration.showNotification(title, opts));
});

self.addEventListener('notificationclick', e => {
  e.notification.close();
  if (e.action === 'dismiss') return;
  const url = e.notification.data?.url || 'https://sync.dhurta.com';
  e.waitUntil(
    clients.matchAll({type:'window', includeUncontrolled:true}).then(cs => {
      const c = cs.find(c => c.url.includes('sync.dhurta'));
      if (c) return c.focus();
      return clients.openWindow(url);
    })
  );
});

/* ── Message from page (e.g. show notification manually) ── */
self.addEventListener('message', e => {
  if (e.data?.type === 'SHOW_NOTIFICATION') {
    self.registration.showNotification(e.data.title || '⚡ Dhurta Sync', {
      body:    e.data.body || '',
      icon:    './sync.png',
      badge:   './sync.png',
      tag:     e.data.tag || 'dhurta',
      renotify: true,
      vibrate: [150, 80, 150],
      data: { url: 'https://sync.dhurta.com' },
    });
  }
});
