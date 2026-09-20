/* ============================================================
   DHURTA Sync — Service Worker (sw.js)
   Cache-first PWA for 100% offline launch
   ============================================================ */

const CACHE = 'dhurta-v2';
const CORE_ASSETS = [
  './',
  './index.html',
  './style.css',
  './app.js',
  'https://unpkg.com/mqtt@5.3.5/dist/mqtt.min.js',
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
  // Skip non-GET requests
  if (e.request.method !== 'GET') return;

  // MQTT WebSocket — never cache
  if (e.request.url.includes('broker.emqx') ||
      e.request.url.includes('hivemq') ||
      e.request.url.includes('mosquitto')) return;

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
