// Finding d3e282fb6f09: the management UI renders API values as text.
//
// Runs the page's own script (src/ui/index.html) against a stub DOM and a
// stub fetch that answers every list with hostile strings: a node name
// carrying an <img onerror>, quotes that would close an attribute, a
// volume name that would break out of an inline handler. Every rendered
// table and the event list are checked: only the page's own tags and
// attributes appear, and each payload is there escaped, never raw. A
// button's action then gets the hostile name as data.
//
// `node tests/ui/escape.js` — exits non-zero with the reason on a failure.
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const page = fs.readFileSync(path.join(__dirname, '..', '..', 'src', 'ui', 'index.html'), 'utf8');
const script = page.slice(page.indexOf('<script>') + '<script>'.length, page.indexOf('</script>'));

function fail(msg) {
  console.error('FAIL: ' + msg);
  process.exit(1);
}

// No inline handlers anywhere in the page, static or generated.
if (/\son[a-z]+\s*=/i.test(page.replace(script, ''))) fail('an inline on* handler in the markup');
if (/\son[a-z]+\s*=/i.test(script.replace(/addEventListener/g, ''))) fail('an inline on* handler in the script');

const IMG = '<img src=x onerror=alert(1)>';
const QUOTE = 'x" onmouseover="alert(2)';
const TICK = "v'),alert(3),('";
const evil = s => `${s}${IMG}${QUOTE}${TICK}`;

const nodes = {
  nodes: [{
    name: evil('n1'), engine_url: evil('http://e'), tier: evil('t'), labels: { [evil('k')]: evil('v') },
    status: { healthy: false, free_bytes: 1, total_bytes: 2, volumes: 1 },
  }],
};
const pools = {
  pools: [
    { kind: 'policy', name: evil('p'), replicas: 2, rung: evil('r'), healthy: 1, matched: 1, nodes: [evil('pn')], free_bytes: 0, total_bytes: 0 },
    { kind: 'slab', node: evil('sn'), slab: evil('slab'), role: evil('role'), tier: evil('st'), domain: evil('d'), volumes: 1, free_bytes: 0, total_bytes: 0 },
  ],
};
const leg = { node: evil('ln'), state: evil('lost'), message: evil('lm') };
const volumes = {
  volumes: [{
    name: evil('vol'), size_bytes: 1, pool: evil('vp'), legs: [leg, { node: 'b', state: 'created' }], rung: evil('vr'),
    head: evil('vh'), assembly: 'degraded', replicas: 2, fenced: true, epoch: evil('ep'),
    dual_attach: { target_node: evil('dt') },
    replica_sync: [{ node: evil('rs'), sync: { state: 'resyncing', progress_pct: 1 } }],
    export: { state: 'published', coordinates: { nqn: evil('nqn'), traddr: evil('ta'), trsvcid: evil('ts'), nsid: evil('ns') } },
  }, {
    name: evil('pend'), size_bytes: 1, legs: [leg, leg], rung: 'node', assembly: 'pending_engine_support',
    export: { state: 'failed', message: evil('em') },
  }],
};
const events = { events: [{ severity: evil('sev'), message: evil('msg'), time: { secs_since_epoch: 1 } }] };
const inventory = {
  volumes: [{
    name: evil('nv'), sealed: true, kind: evil('kind'), in_use: true, attachments: [{ transport: evil('tr') }],
    virtual_size_bytes: 1, allocated_bytes: 1, role: evil('role'), slabs: [],
    placement: {
      slabs: [{ id: evil('sid'), drive: { serial: evil('ser') }, node: evil('sno'), state: evil('bad') }],
      arrays: [{ members: [{ drive: { path: evil('mp') }, node: evil('mn'), state: evil('ms') }] }],
      rebuild: evil('rb'),
    },
    consumer: { kind: evil('ck'), namespace: evil('cns'), name: evil('cn') },
    pv: { pv: evil('pv'), phase: evil('ph'), volume_kind: evil('vk'), claim: { namespace: evil('pns'), name: evil('pcn'), bound: false, phase: evil('cp') } },
  }],
};

const els = {};
const el = id => els[id] || (els[id] = {
  id, innerHTML: '', textContent: '', value: '', style: {}, listeners: {},
  addEventListener(t, f) { this.listeners[t] = f; },
});
const docListeners = {};
const requests = [];
const answer = body => ({ ok: true, status: 200, statusText: 'OK', json: async () => body });
const sandbox = {
  location: { pathname: '/ui/' },
  document: {
    getElementById: el,
    addEventListener(t, f) { docListeners[t] = f; },
  },
  sessionStorage: { getItem: () => '', setItem: () => {} },
  prompt: () => null,
  confirm: () => true,
  setInterval: () => 0,
  setTimeout: () => 0,
  console,
  fetch: async (url, opts) => {
    requests.push({ url, method: (opts && opts.method) || 'GET' });
    const p = url.replace(/^\//, '');
    if (p === 'api/v1/health') return answer({ version: evil('ver') });
    if (p === 'api/v1/nodes') return answer(nodes);
    if (p === 'api/v1/pools') return answer(pools);
    if (p === 'api/v1/volumes') return answer(volumes);
    if (p === 'api/v1/events') return answer(events);
    if (/^api\/v1\/nodes\/[^/]+\/inventory$/.test(p)) return answer(inventory);
    return answer({});
  },
};
vm.createContext(sandbox);
vm.runInContext(script, sandbox);

// The page's own markup: these tags and attributes, nothing else.
const TAGS = new Set(['tr', 'td', 'span', 'button', 'br', 'div', 'option']);
const ATTRS = new Set(['class', 'title', 'colspan', 'data-act', 'data-name', 'data-from', 'value']);

function check(id, html) {
  if (!html) fail(`${id}: nothing rendered`);
  // Every tag, with its attributes, as a browser would tokenize it.
  const tagRe = /<\/?([a-zA-Z][a-zA-Z0-9-]*)((?:\s+[^\s"'>\/=]+(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]+))?)*)\s*\/?>/g;
  let m;
  let stripped = html;
  while ((m = tagRe.exec(html))) {
    const tag = m[1].toLowerCase();
    if (!TAGS.has(tag)) fail(`${id}: a <${tag}> was rendered: ${m[0]}`);
    const attrRe = /([^\s"'>\/=]+)(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]+))?/g;
    let a;
    while ((a = attrRe.exec(m[2]))) {
      if (!ATTRS.has(a[1].toLowerCase())) fail(`${id}: attribute ${a[1]} on <${tag}>: ${m[0]}`);
    }
    stripped = stripped.replace(m[0], '');
  }
  // What is left is text: no markup may hide in it.
  if (/[<>]/.test(stripped)) fail(`${id}: a raw < or > outside the page's tags: ${stripped}`);
  if (html.includes(IMG)) fail(`${id}: the <img> payload is there raw`);
  if (!html.includes('&lt;img src=x onerror=alert(1)&gt;')) fail(`${id}: the <img> payload is not shown, escaped`);
  if (html.includes('x" onmouseover')) fail(`${id}: the quote payload closed an attribute`);
}

(async () => {
  // refresh() runs at load; let its awaits settle.
  for (let i = 0; i < 50; i++) await new Promise(r => setImmediate(r));
  if (els.err && els.err.textContent) fail('refresh failed: ' + els.err.textContent);
  for (const id of ['nodes', 'pools', 'nvols', 'vols', 'evlist']) check(id, els[id].innerHTML);
  const opts = els['v-pool'].innerHTML;
  if (opts.includes(IMG) || !opts.includes('&lt;img')) fail('v-pool: the pool name is not escaped');
  if (!els.head.textContent.includes(IMG)) fail('head: shown as text');

  // A button's action carries the hostile name as data, to an encoded URL.
  const click = docListeners.click;
  if (!click) fail('no click listener');
  const name = evil('vol');
  requests.length = 0;
  click({ target: { closest: () => ({ dataset: { act: 'del', name } }) } });
  click({ target: { closest: () => ({ dataset: { act: 'move', name, from: evil('ln') } }) } });
  for (let i = 0; i < 20; i++) await new Promise(r => setImmediate(r));
  const want = `/api/v1/volumes/${encodeURIComponent(name)}`;
  if (!requests.some(r => r.url === want && r.method === 'DELETE')) fail('delete not sent for the name: ' + JSON.stringify(requests));
  if (!requests.some(r => r.url === want + '/move' && r.method === 'POST')) fail('move not sent for the name: ' + JSON.stringify(requests));
  // The rendered buttons name the action, and carry the name escaped.
  if (!/data-act="del" data-name="vol&lt;img/.test(els.vols.innerHTML)) fail('the delete button does not carry the name as data');
  console.log('ok: every API value rendered as text');
})().catch(e => fail(e.stack || String(e)));
