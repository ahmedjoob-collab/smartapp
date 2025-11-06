/* v2 – cache static only; never cache HTML pages */
const CACHE_NAME = "sb-cache-v2";
const CORE_ASSETS = [
  "/static/manifest.webmanifest",
  "/static/offline.html",
  // أضف ملفات ثابتة أساسية هنا إن رغبت
];

// Install: pre-cache core
self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(CORE_ASSETS))
  );
  self.skipWaiting();
});

// Activate: clean old caches
self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.map((k) => (k !== CACHE_NAME ? caches.delete(k) : null)))
    )
  );
  self.clients.claim();
});

// Strategy: HTML -> network-only (do not cache), static -> cache-first
self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);

  // نفس الدومين فقط
  if (url.origin !== self.location.origin) return;

  // صفحات HTML: شبكة فقط، بدون تخزين في الكاش
  if (req.mode === "navigate" || (req.headers.get("accept") || "").includes("text/html")) {
    event.respondWith(
      fetch(req).catch(() => caches.match("/static/offline.html"))
    );
    return;
  }

  // باقي الملفات الثابتة: cache-first
  event.respondWith(
    caches.match(req).then((cached) => {
      if (cached) return cached;
      return fetch(req).then((res) => {
        const copy = res.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(req, copy));
        return res;
      });
    })
  );
});
