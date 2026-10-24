// Dependency-free DOM simulation for the embedded panel. This is not a real-browser test.
// An OS-level child timeout also catches synchronous/microtask loops that prevent
// an in-process setTimeout watchdog from firing.
if (process.argv[2] !== '--child') {
  const result = require('node:child_process').spawnSync(process.execPath, [__filename, '--child'], { stdio: 'inherit', timeout: 15000 });
  if (result.error) console.error('Panel test terminated:', result.error.code);
  process.exit(result.status === null ? 1 : result.status);
}
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const HTML = fs.readFileSync(path.join(__dirname, '../internal/panel/assets/index.html'), 'utf8');
let JS = HTML.match(/<script>([\s\S]*?)<\/script>/)[1];
if ((HTML.match(/__CPA_DISTILL_API_BASE__/g) || []).length !== 1) throw new Error('placeholder not unique');
JS = JS.replace('__CPA_DISTILL_API_BASE__', 'http://api.test/base');

class Node {
  constructor(tag) {
    this.tagName = String(tag || '').toUpperCase();
    this.children = [];
    this.listeners = {};
    this.attrs = {};
    this.style = {};
    this._text = '';
    this._class = '';
    this.hidden = false;
    this.disabled = false;
    this.value = '';
    this.type = '';
    this.checked = false;
    this.title = '';
    this.parentNode = null;
  }
  get firstChild() { return this.children[0] || null; }
  set className(v) { this._class = String(v); }
  get className() { return this._class; }
  set textContent(v) { this._text = String(v); this.children = []; }
  get textContent() { return this._text; }
  setAttribute(k, v) { this.attrs[k] = v; }
  getAttribute(k) { return this.attrs[k]; }
  appendChild(c) { c.parentNode = this; this.children.push(c); return c; }
  removeChild(c) { const i = this.children.indexOf(c); if (i >= 0) this.children.splice(i, 1); return c; }
  addEventListener(t, f) { (this.listeners[t] = this.listeners[t] || []).push(f); }
  dispatch(t, ev) { (this.listeners[t] || []).forEach((f) => f(ev || { preventDefault() {}, stopPropagation() {} })); }
  focus() {}
  click() { this.dispatch('click'); }
  get classList() {
    const self = this;
    return {
      add(c) { self._class = (self._class + ' ' + c).trim(); },
      remove(c) { self._class = self._class.split(/\s+/).filter((x) => x && x !== c).join(' '); },
      contains(c) { return self._class.split(/\s+/).indexOf(c) !== -1; },
    };
  }
  text() {
    let out = this._text;
    for (const c of this.children) out += ' ' + c.text();
    return out;
  }
}

const registry = new Map();
const ids = ['gate','gate-form','gate-key','gate-show','gate-remember','gate-connect','gate-status','gate-error','app','status-bar','status-refresh','disconnect','filter-from','filter-to','filter-key','filter-model','filter-page-size','search','reset','result-summary','search-status','search-error','records-body','empty-hint','page-first','page-prev','page-next','page-last','page-info','drawer','drawer-backdrop','drawer-body','drawer-close','toasts'];
function resetRegistry() {
  for (const id of ids) {
    const node = new Node('div');
    const tag = HTML.match(new RegExp('<[^>]*\\bid="' + id + '"[^>]*>'));
    node.hidden = !!(tag && /\bhidden\b/.test(tag[0]));
    registry.set(id, node);
  }
  registry.get('gate-form').tagName = 'FORM';
}
resetRegistry();

const documentStub = {
  getElementById: (id) => registry.get(id) || null,
  createElement: (tag) => new Node(tag),
  addEventListener() {},
  body: new Node('body'),
};

const calls = [];
function b64(s) { return Buffer.from(s, 'utf8').toString('base64'); }
function b64bytes(arr) { return Buffer.from(arr).toString('base64'); }

// 详情接口现在返回原始字节（base64）而非事件列表：request_body / response_body / websocket_body。
const clientRequestBody = '{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}';
const bigText = 'X'.repeat(300 * 1024);
// 存储侧已按 ChunkIndex 顺序连接流式分片；末尾追加超长内容以验证预览截断。
const streamResponseBody = 'data: {"choices":[{"delta":{"content":"你好"}}]}\n\ndata: {"choices":[{"delta":{"content":"世界"}}]}\n\ndata: [DONE]\n\n' + bigText;
const binaryWebSocketBody = [0xff, 0xfe, 0x00, 0x01];

const bodies = {
  request_body: b64(clientRequestBody),
  response_body: b64(streamResponseBody),
  websocket_body: b64bytes(binaryWebSocketBody),
};

const record = { session_id: 's1', request_id: 'r1', client_key_id: 'k1', model: 'gpt-4o-2024', requested_model: 'gpt-4o', source_format: 'openai', stream: true, started_at: '2024-05-01T10:00:00Z', completed_at: '2024-05-01T10:00:02Z', event_count: 8 };

const responses = {
  'GET /base/records/options': { keys: [{ id: 'k1', label: 'sk-****abcd' }], models: ['gpt-4o'] },
  'GET /base/status': { pending_requests: 1, buffer_bytes: 2048, dropped: 0, write_failures: 0, queue_length: 2 },
  'POST /base/records/search': { items: [record], total: 41, page: 1, page_size: 20 },
  'POST /base/records/detail': { record, bodies },
};

let failMode = null;
let delayStatus = null;
const fetchStub = (url, init) => {
  const method = (init && init.method) || 'GET';
  const key = method + ' ' + String(url).replace('http://api.test', '');
  calls.push({ key, init, url });
  if (delayStatus && key === 'GET /base/status') {
    const hold = delayStatus;
    delayStatus = null;
    return new Promise(hold);
  }
  if (failMode === 'network') return Promise.reject(new Error('offline'));
  if (['unauthorized', 'forbidden', 'server'].includes(failMode)) {
    const status = { unauthorized: 401, forbidden: 403, server: 500 }[failMode];
    return Promise.resolve({ ok: false, status, text: () => Promise.resolve(JSON.stringify({ error: 'request failed' })) });
  }
  const body = responses[key];
  if (!body) return Promise.resolve({ ok: false, status: 404, text: () => Promise.resolve('not found') });
  return Promise.resolve({ ok: true, status: 200, text: () => Promise.resolve(JSON.stringify(body)) });
};

const timers = [];
const stored = new Map();
let storageBlocked = false;
let windowListeners = {};
const storageStub = {
  getItem: (key) => stored.has(key) ? stored.get(key) : null,
  setItem: (key, value) => stored.set(key, String(value)),
  removeItem: (key) => stored.delete(key),
};
const windowStub = {
  get localStorage() { if (storageBlocked) throw new Error('Storage blocked'); return storageStub; },
  fetch: fetchStub,
  atob: (s) => Buffer.from(s, 'base64').toString('binary'),
  setTimeout: (fn, ms) => setTimeout(fn, 0),
  setInterval: (fn, ms) => { const t = setInterval(fn, ms); timers.push(t); return t; },
  clearInterval: (t) => clearInterval(t),
  addEventListener(name, fn) { (windowListeners[name] = windowListeners[name] || []).push(fn); },
  URL: { createObjectURL: () => 'blob:test', revokeObjectURL() {} },
  location: { href: 'http://panel.test/' },
};

const blobs = [];
const sandbox = {
  window: windowStub,
  document: documentStub,
  console,
  TextEncoder,
  TextDecoder,
  Blob: class Blob { constructor(parts, opts) { this.parts = parts; this.type = opts && opts.type; blobs.push(this); } },
  AbortController,
  Promise,
  JSON,
  Date,
  Math,
  Object,
  Array,
  String,
  Number,
  Boolean,
  Error,
  RegExp,
  isFinite,
  isNaN,
  parseInt,
  parseFloat,
};
sandbox.globalThis = sandbox;
windowStub.window = windowStub;

const results = [];
function check(name, cond, extra) {
  results.push({ name, ok: !!cond, extra: extra === undefined ? '' : String(extra).slice(0, 200) });
  console.log((cond ? 'PASS  ' : 'FAIL  ') + name + (cond ? '' : '  => ' + (extra === undefined ? '' : String(extra).slice(0, 200))));
}

const tick = () => new Promise((r) => setTimeout(r, 0));
const settle = async () => { for (let i = 0; i < 12; i += 1) await tick(); };

(async () => {
  vm.createContext(sandbox);
  vm.runInContext(JS, sandbox, { filename: 'panel.js', timeout: 2000 });
  await settle();

  // 1. 初始状态：占位符已替换，闸门可见
  check('初始闸门可见', registry.get('gate').hidden === false);
  check('初始错误为空', registry.get('gate-error').hidden === true);

  // 2. 连接
  registry.get('gate-key').value = 'secret-management-key';
  registry.get('gate-form').dispatch('submit');
  await settle();

  check('连接后进入面板', registry.get('app').hidden === false && registry.get('gate').hidden === true);
  check('连接后清空 key 输入框', registry.get('gate-key').value === '');
  const optionsCall = calls.find((c) => c.key === 'GET /base/records/options');
  check('options 请求带 Authorization', optionsCall && optionsCall.init.headers.Authorization === 'Bearer secret-management-key', optionsCall && optionsCall.init.headers.Authorization);
  check('Key 下拉使用脱敏标签', registry.get('filter-key').children.map((c) => c.textContent).join('|').includes('sk-****abcd'));
  check('Key 下拉 value 为 id', registry.get('filter-key').children[1].attrs.value === 'k1');
  check('模型下拉含模型', registry.get('filter-model').children.map((c) => c.textContent).join('|').includes('gpt-4o'));
  check('状态栏已填充', registry.get('status-bar').textContent.includes('待处理 1'), registry.get('status-bar').textContent);

  // 3. 查询
  const searchCall = calls.filter((c) => c.key === 'POST /base/records/search').pop();
  const searchBody = JSON.parse(searchCall.init.body);
  check('查询体 page=1 page_size=20', searchBody.page === 1 && searchBody.page_size === 20, searchCall.init.body);
  check('查询体不含空筛选字段', searchBody.from === undefined && searchBody.to === undefined && searchBody.key_id === undefined && searchBody.model === undefined, searchCall.init.body);
  check('表格渲染 1 行', registry.get('records-body').children.length === 1);
  check('分页信息含总数', registry.get('page-info').textContent.includes('共 41 条'), registry.get('page-info').textContent);
  check('行内容含脱敏 Key 标签', registry.get('records-body').children[0].text().includes('sk-****abcd'));
  check('行内容含 requested_model 与上游模型', registry.get('records-body').children[0].text().includes('gpt-4o') && registry.get('records-body').children[0].text().includes('gpt-4o-2024'));

  // 4. 筛选变化重置到第 1 页
  registry.get('filter-key').value = 'k1';
  registry.get('filter-key').dispatch('change');
  await settle();
  const filtered = JSON.parse(calls.filter((c) => c.key === 'POST /base/records/search').pop().init.body);
  check('筛选变化带上 key_id 且 page=1', filtered.key_id === 'k1' && filtered.page === 1, JSON.stringify(filtered));

  // 5. 时间筛选转换
  registry.get('filter-from').value = '2024-05-01T18:00';
  registry.get('filter-to').value = '2024-05-01T19:00';
  registry.get('filter-from').dispatch('change');
  await settle();
  const timed = JSON.parse(calls.filter((c) => c.key === 'POST /base/records/search').pop().init.body);
  check('from/to 转为 UTC RFC3339', /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(timed.from) && timed.from.endsWith('Z') && timed.to > timed.from, JSON.stringify(timed));
  check('本地时间按本地时区换算', new Date(timed.from).getTime() === new Date('2024-05-01T18:00').getTime(), timed.from);

  // 6. 时间非法（from >= to）应报错且不发请求
  const beforeCount = calls.length;
  registry.get('filter-to').value = '2024-05-01T17:00';
  registry.get('filter-to').dispatch('change');
  await settle();
  check('from>=to 时不发请求', calls.length === beforeCount, calls.length + ' vs ' + beforeCount);
  check('from>=to 显示错误', registry.get('search-error').hidden === false && registry.get('search-error').textContent.includes('必须早于'), registry.get('search-error').textContent);

  // 7. 分页
  registry.get('filter-to').value = '';
  registry.get('page-next').dispatch('click');
  await settle();
  const paged = JSON.parse(calls.filter((c) => c.key === 'POST /base/records/search').pop().init.body);
  check('下一页 page=2', paged.page === 2, JSON.stringify(paged));

  // 8. 越界纠正：后端返回 page 超过总页数
  const beforeCorrection = calls.filter((c) => c.key === 'POST /base/records/search').length;
  responses['POST /base/records/search'] = { items: [record], total: 5, page: 9, page_size: 20 };
  registry.get('page-next').dispatch('click');
  await settle();
  const fixed = JSON.parse(calls.filter((c) => c.key === 'POST /base/records/search').pop().init.body);
  check('越界后自动回到最后一页', fixed.page === 1, JSON.stringify(fixed));
  check('异常分页最多纠正一次', calls.filter((c) => c.key === 'POST /base/records/search').length - beforeCorrection === 2 && registry.get('search-error').textContent.includes('不一致'));
  responses['POST /base/records/search'] = { items: [record], total: 41, page: 1, page_size: 20 };

  // 9. 每页条数
  registry.get('filter-page-size').value = '100';
  registry.get('filter-page-size').dispatch('change');
  await settle();
  const sized = JSON.parse(calls.filter((c) => c.key === 'POST /base/records/search').pop().init.body);
  check('page_size=100 生效', sized.page_size === 100, JSON.stringify(sized));

  // 10. 详情：原始字节而非事件列表
  registry.get('records-body').children[0].dispatch('click');
  await settle();
  check('详情抽屉打开', registry.get('drawer').hidden === false);
  const detailText = registry.get('drawer-body').text();
  check('详情含基础信息', detailText.includes('基础信息') && detailText.includes('s1'));
  check('详情含请求正文原文', detailText.includes('"messages"'));
  check('详情含响应正文文本', detailText.includes('你好') && detailText.includes('世界'));
  check('详情含 WebSocket 正文说明', detailText.includes('上游 WebSocket') && detailText.includes('不是客户端下游'));
  check('非文本正文有说明', detailText.includes('不是合法 UTF-8'));
  check('超长预览被截断并提示', detailText.includes('仅显示前') && !detailText.includes(bigText.slice(0, 400 * 1024)), detailText.length);
  check('详情不再展示事件语义', !detailText.includes('原始事件') && !detailText.includes('sequence') && !detailText.includes('ChunkIndex -'), detailText.slice(0, 120));
  const detailCall = calls.filter((c) => c.key === 'POST /base/records/detail').pop();
  check('详情请求体含 session/request', detailCall.init.body.includes('"session_id":"s1"') && detailCall.init.body.includes('"request_id":"r1"'), detailCall.init.body);
  check('详情请求带 Authorization', detailCall.init.headers.Authorization === 'Bearer secret-management-key');

  // 11. 下载按钮下载的是原始字节，不是事件 JSON
  const buttons = [];
  (function walk(n) { if (n.tagName === 'BUTTON') buttons.push(n.textContent); n.children.forEach(walk); })(registry.get('drawer-body'));
  check('存在下载原始正文按钮', buttons.filter((b) => b === '下载原始正文').length === 3, buttons.join('|'));
  check('不再存在事件 JSON 下载按钮', !buttons.some((b) => b.includes('事件 JSON')), buttons.join('|'));

  const blobsBefore = blobs.length;
  const downloadButton = (function find(n) {
    if (n.tagName === 'BUTTON' && n.textContent === '下载原始正文') return n;
    for (const c of n.children) { const hit = find(c); if (hit) return hit; }
    return null;
  })(registry.get('drawer-body'));
  downloadButton.dispatch('click');
  const rawBlob = blobs[blobs.length - 1];
  check('下载使用原始字节 Blob', blobs.length === blobsBefore + 1 && !!rawBlob && rawBlob.type === 'application/octet-stream');
  check('下载内容为请求正文原始字节', !!rawBlob && !!rawBlob.parts[0] && Buffer.from(rawBlob.parts[0]).toString('utf8') === clientRequestBody, rawBlob && rawBlob.parts.length);

  // 12. 401 提示
  registry.get('drawer-close').dispatch('click');
  failMode = 'unauthorized';
  registry.get('search').dispatch('click');
  await settle();
  check('401 明确提示且不泄露 Key', registry.get('gate-error').textContent.includes('401') && !registry.get('gate-error').textContent.includes('secret-management-key'), registry.get('gate-error').textContent);
  failMode = null;

  // 13. 断开清空
  registry.get('disconnect').dispatch('click');
  await settle();
  check('断开后回到闸门', registry.get('app').hidden === true && registry.get('gate').hidden === false);
  check('断开后清空表格', registry.get('records-body').children.length === 0);
  check('断开后清空状态栏', registry.get('status-bar').textContent === '');
  check('断开后清空 Key 下拉', registry.get('filter-key').children.length === 0);
  check('断开后抽屉关闭', registry.get('drawer').hidden === true && registry.get('drawer-body').children.length === 0);

  // 14. 无 Key 泄露到 URL / DOM 全文
  const allText = registry.get('app').text() + registry.get('gate').text() + registry.get('drawer-body').text();
  check('DOM 中无管理 Key 明文', !allText.includes('secret-management-key'));
  check('所有请求 URL 不含 Key', calls.every((c) => !String(c.url).includes('secret') && !String(c.url).includes('key=')), calls.map((c) => c.url).join('|'));
  check('所有请求都带 Bearer 头', calls.every((c) => String(c.init.headers.Authorization || '').indexOf('Bearer ') === 0));

  check('未勾选不持久化密钥', stored.size === 0);
  check('选项文字准确', /id="gate-remember"[^>]*>\s*记住登陆<\/label>/.test(HTML));

  const storageKey = 'cpa-distill:management-key:v1:http://api.test/base';
  async function reloadPage(script = JS) {
    timers.splice(0).forEach(clearInterval);
    windowListeners = {};
    resetRegistry();
    vm.runInContext(script, sandbox, { filename: 'panel.js', timeout: 2000 });
    await settle();
  }
  async function login(key, remember) {
    registry.get('gate-key').value = key;
    registry.get('gate-remember').checked = remember;
    registry.get('gate-form').dispatch('submit');
    await settle();
  }

  await login('remembered-secret', true);
  check('认证成功后记住登陆', JSON.parse(stored.get(storageKey) || '{}').key === 'remembered-secret');
  check('保存内容不设有效期', Object.keys(JSON.parse(stored.get(storageKey))).join(',') === 'key');
  await reloadPage();
  check('刷新自动登陆', !registry.get('app').hidden && calls[calls.length - 1].init.headers.Authorization === 'Bearer remembered-secret');
  check('自动登陆不回填密钥输入框', registry.get('gate-key').value === '');

  (windowListeners.pagehide || []).forEach((fn) => fn({}));
  check('离开页面保留记住的登陆', stored.has(storageKey) && registry.get('app').hidden);
  (windowListeners.pageshow || []).forEach((fn) => fn({ persisted: true }));
  await settle();
  check('浏览器返回缓存页自动恢复登陆', !registry.get('app').hidden);

  failMode = 'network';
  await reloadPage();
  check('网络故障不清除凭证', stored.has(storageKey) && !registry.get('gate').hidden);
  failMode = null;
  registry.get('gate-form').dispatch('submit');
  await settle();
  check('网络恢复可不重新输入而重试', !registry.get('app').hidden);

  failMode = 'server';
  await reloadPage();
  check('服务端故障不清除凭证', stored.has(storageKey));
  failMode = null;
  await reloadPage();
  registry.get('disconnect').dispatch('click');
  check('退出并忘记删除凭证', !stored.has(storageKey) && registry.get('app').hidden);
  const beforeForgotReload = calls.length;
  await reloadPage();
  check('忘记后不再自动请求', calls.length === beforeForgotReload);

  await login('invalid-secret', true);
  failMode = 'unauthorized';
  await reloadPage();
  check('自动登陆401清除凭证', !stored.has(storageKey) && !registry.get('gate').hidden && registry.get('gate-error').textContent.includes('401'));
  failMode = null;
  await login('forbidden-secret', true);
  failMode = 'forbidden';
  registry.get('search').dispatch('click');
  await settle();
  check('使用中403清除凭证和记录', !stored.has(storageKey) && registry.get('app').hidden && registry.get('records-body').children.length === 0);
  failMode = null;

  stored.set(storageKey, '{bad JSON');
  await reloadPage();
  check('损坏的凭证被清除', !stored.has(storageKey) && !registry.get('gate').hidden);
  stored.set(storageKey, JSON.stringify({ key: 'other-base-secret' }));
  const beforeOtherBase = calls.length;
  await reloadPage(JS.replace('http://api.test/base', 'http://api.test/other-base'));
  check('不同API路径隔离保存的凭证', calls.length === beforeOtherBase && stored.has(storageKey));
  stored.clear();
  storageBlocked = true;
  await reloadPage();
  await login('memory-only-secret', true);
  check('存储不可用仍能登陆', !registry.get('app').hidden && stored.size === 0);
  registry.get('disconnect').dispatch('click');
  storageBlocked = false;
  await login('unremembered-secret', false);
  await reloadPage();
  check('不记住时刷新需要重新登陆', !registry.get('gate').hidden && stored.size === 0);

  let finishOldRequest;
  delayStatus = (resolve) => { finishOldRequest = resolve; };
  await login('same-key-new-session', true);
  registry.get('disconnect').dispatch('click');
  await login('same-key-new-session', true);
  finishOldRequest({ ok: false, status: 401, text: () => Promise.resolve('{}') });
  await settle();
  check('旧会话的迟到401不会退出新会话', !registry.get('app').hidden && stored.has(storageKey));

  const failed = results.filter((r) => !r.ok);
  console.log('\n' + (results.length - failed.length) + '/' + results.length + ' checks passed');
  timers.forEach((t) => clearInterval(t));
  process.exit(failed.length ? 1 : 0);
})();
