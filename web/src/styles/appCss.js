export const APP_CSS = `
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { background: #05020a; color: #fff; font-family: 'Syne', sans-serif; overflow-x: hidden; }
  ::-webkit-scrollbar { width: 3px; height: 3px; }
  ::-webkit-scrollbar-thumb { background: rgba(255,106,0,0.3); border-radius: 2px; }
  ::-webkit-scrollbar-track { background: transparent; }
  input, select, button, textarea { font-family: 'Syne', sans-serif; }

  @keyframes pulse-dot { 0%,100%{box-shadow:0 0 0 0 rgba(255,106,0,.7)} 50%{box-shadow:0 0 0 6px rgba(255,106,0,0)} }
  @keyframes slideUp { from{opacity:0;transform:translateY(16px)} to{opacity:1;transform:none} }
  @keyframes fadeIn { from{opacity:0} to{opacity:1} }
  @keyframes spin { to{transform:rotate(360deg)} }
  @keyframes flicker { 0%,100%{opacity:1} 93%{opacity:.4} 94%{opacity:1} 96%{opacity:.7} 97%{opacity:1} }
  @keyframes gradShift { 0%,100%{background-position:0% 50%} 50%{background-position:100% 50%} }
  @keyframes ringExpand { 0%{transform:translate(-50%,-50%) scale(.8);opacity:.7} 100%{transform:translate(-50%,-50%) scale(2.6);opacity:0} }
  @keyframes floatUp { 0%{transform:translateY(0) scale(.8);opacity:0} 10%{opacity:1} 90%{opacity:.6} 100%{transform:translateY(-18vh) scale(1.3);opacity:0} }
  @keyframes textGlow { 0%,100%{text-shadow:0 0 20px rgba(255,106,0,.3)} 50%{text-shadow:0 0 40px rgba(255,106,0,.7),0 0 80px rgba(255,69,0,.2)} }
  @keyframes borderPulse { 0%,100%{border-color:rgba(255,106,0,.15)} 50%{border-color:rgba(255,106,0,.5);box-shadow:0 0 20px rgba(255,106,0,.1)} }

  .btn-fire {
    background: linear-gradient(135deg,#ff6a00,#ee2200);
    border:none; color:#fff; cursor:pointer; border-radius:10px;
    font-weight:700; letter-spacing:.3px; position:relative; overflow:hidden;
    transition:transform .15s, box-shadow .15s;
    box-shadow:0 4px 20px rgba(255,106,0,.3);
  }
  .btn-fire::after { content:''; position:absolute; inset:0; background:linear-gradient(135deg,rgba(255,255,255,.12),transparent); pointer-events:none; }
  .btn-fire:hover:not(:disabled) { transform:translateY(-1px); box-shadow:0 8px 28px rgba(255,106,0,.45); }
  .btn-fire:active:not(:disabled) { transform:none; }
  .btn-fire:disabled { opacity:.5; cursor:not-allowed; }

  .btn-ghost {
    background:rgba(255,255,255,.04); border:1px solid rgba(255,255,255,.08);
    color:rgba(255,255,255,.6); cursor:pointer; border-radius:8px; font-weight:600;
    transition:all .15s;
  }
  .btn-ghost:hover { background:rgba(255,106,0,.1); border-color:rgba(255,106,0,.3); color:#ff8c42; }

  .glass { background:rgba(255,255,255,.03); border:1px solid rgba(255,255,255,.07); border-radius:16px; position:relative; overflow:hidden; }
  .glass::before { content:''; position:absolute; inset:0; background:linear-gradient(135deg,rgba(255,255,255,.04) 0%,transparent 60%); pointer-events:none; }

  .input-f {
    width:100%; background:rgba(255,255,255,.05); border:1px solid rgba(255,255,255,.1);
    border-radius:10px; color:#fff; padding:11px 14px; font-size:14px; outline:none;
    transition:border-color .2s, box-shadow .2s;
  }
  .input-f:focus { border-color:rgba(255,106,0,.5); box-shadow:0 0 0 3px rgba(255,106,0,.1); }
  .input-f::placeholder { color:rgba(255,255,255,.2); }
  .input-f:disabled { opacity:.5; cursor:not-allowed; }
  select.input-f { background-color:rgba(255,255,255,.05); color:#fff; color-scheme:dark; }
  select.input-f option { background:#12080f; color:#f8e6d8; }
  .ember-range { accent-color:#ff7a2f; }
  .ember-range::-webkit-slider-thumb { background:#ff7a2f; border:1px solid rgba(0,0,0,.35); box-shadow:0 0 0 2px rgba(255,122,47,.25); }
  .ember-range::-moz-range-thumb { background:#ff7a2f; border:1px solid rgba(0,0,0,.35); box-shadow:0 0 0 2px rgba(255,122,47,.25); }
  .ember-range::-webkit-slider-runnable-track { background:linear-gradient(90deg,rgba(255,122,47,.7),rgba(255,78,0,.45)); height:6px; border-radius:999px; }
  .ember-range::-moz-range-track { background:linear-gradient(90deg,rgba(255,122,47,.7),rgba(255,78,0,.45)); height:6px; border-radius:999px; }

  .ember-toggle { position:relative; width:44px; height:24px; display:inline-flex; flex:0 0 auto; }
  .ember-toggle input { position:absolute; inset:0; opacity:0; cursor:pointer; margin:0; }
  .ember-toggle-track {
    position:absolute; inset:0; border-radius:999px;
    background:rgba(255,255,255,.15); border:1px solid rgba(255,255,255,.22);
    transition:all .18s ease;
  }
  .ember-toggle-track::after {
    content:''; position:absolute; top:2px; left:2px; width:18px; height:18px; border-radius:50%;
    background:#fff; box-shadow:0 2px 8px rgba(0,0,0,.35); transition:transform .18s ease, background .18s ease;
  }
  .ember-toggle input:checked + .ember-toggle-track {
    background:linear-gradient(135deg,#ff7a2f,#ff4e00); border-color:rgba(255,120,55,.75);
  }
  .ember-toggle input:checked + .ember-toggle-track::after {
    transform:translateX(20px); background:#fff7ef;
  }

  .tx-link { color:inherit; text-decoration:none; opacity:.6; transition:opacity .15s; font-family:'JetBrains Mono',monospace; }
  .tx-link:hover { opacity:1; text-decoration:underline; color:#ff8c42; }

  .tag-on { background:rgba(255,106,0,.12); border:1px solid rgba(255,106,0,.28); color:#ff8c42; padding:2px 10px; border-radius:20px; font-size:11px; font-weight:700; display:flex; align-items:center; gap:5px; }
  .tag-off { background:rgba(255,255,255,.05); border:1px solid rgba(255,255,255,.1); color:rgba(255,255,255,.3); padding:2px 10px; border-radius:20px; font-size:11px; font-weight:600; }

  .nav-blur { backdrop-filter:blur(20px) saturate(180%); -webkit-backdrop-filter:blur(20px); background:rgba(5,2,10,.75); border-bottom:1px solid rgba(255,255,255,.06); }
  .nav-grid { display:grid; grid-template-columns:auto 1fr auto; align-items:center; gap:18px; min-height:64px; padding:0 24px; }
  .brand-btn { display:flex; align-items:center; gap:12px; background:none; border:none; color:inherit; cursor:pointer; padding:0; }
  .nav-links { display:flex; align-items:center; justify-content:center; gap:18px; min-width:0; overflow-x:auto; overflow-y:visible; white-space:nowrap; scrollbar-width:none; }
  .nav-links::-webkit-scrollbar { display:none; }
  .nav-link-btn {
    background:none; border:none; color:rgba(255,255,255,.55); cursor:pointer;
    font-size:14px; font-weight:600; padding:8px 0; border-bottom:2px solid transparent;
    transition:color .15s ease, border-color .15s ease;
  }
  .nav-link-btn:hover { color:rgba(255,255,255,.9); }
  .nav-link-btn.active { color:#ff8c42; border-bottom-color:#ff6a00; }
  .nav-link-btn.disabled { color:rgba(255,255,255,.35); cursor:default; }
  .nav-socials { display:flex; align-items:center; gap:8px; margin-left:6px; padding-left:10px; border-left:1px solid rgba(255,255,255,.08); }
  .nav-social-btn {
    width:26px; height:26px; border-radius:8px; display:inline-flex; align-items:center; justify-content:center;
    background:rgba(255,255,255,.03); border:1px solid rgba(255,255,255,.1); color:rgba(255,255,255,.72);
    transition:all .15s ease; text-decoration:none;
  }
  .nav-social-btn:hover { color:#ff9f5a; border-color:rgba(255,106,0,.36); background:rgba(255,106,0,.09); transform:translateY(-1px); }
  .nav-social-btn svg { width:14px; height:14px; display:block; fill:currentColor; }

  .lang-fab {
    position: fixed;
    left: 14px;
    bottom: 14px;
    z-index: 260;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
  }
  .lang-fab-trigger {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    border: 1px solid rgba(255,255,255,.16);
    border-radius: 999px;
    padding: 9px 12px;
    background: rgba(8,4,14,.9);
    color: rgba(255,255,255,.9);
    font-size: 12px;
    font-weight: 700;
    cursor: pointer;
    backdrop-filter: blur(8px);
    box-shadow: 0 8px 24px rgba(0,0,0,.4);
    transition: border-color .15s ease, transform .15s ease, box-shadow .15s ease;
  }
  .lang-fab-trigger:hover {
    border-color: rgba(255,106,0,.45);
    transform: translateY(-1px);
    box-shadow: 0 10px 26px rgba(0,0,0,.46), 0 0 0 1px rgba(255,106,0,.14) inset;
  }
  .lang-fab-menu {
    margin-bottom: 8px;
    width: min(270px, calc(100vw - 28px));
    background: rgba(8,4,14,.96);
    border: 1px solid rgba(255,255,255,.14);
    border-radius: 14px;
    padding: 8px;
    box-shadow: 0 16px 36px rgba(0,0,0,.48);
  }
  .lang-fab-title {
    padding: 2px 8px 8px;
    font-size: 10px;
    font-weight: 800;
    letter-spacing: .85px;
    text-transform: uppercase;
    color: #ff9f5a;
  }
  .lang-fab-list {
    max-height: 270px;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .lang-fab-option {
    width: 100%;
    border: 1px solid transparent;
    border-radius: 9px;
    padding: 8px 9px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: rgba(255,255,255,.03);
    color: rgba(255,255,255,.85);
    cursor: pointer;
    font-size: 12px;
    font-weight: 600;
    transition: all .15s ease;
  }
  .lang-fab-option:hover {
    border-color: rgba(255,106,0,.35);
    background: rgba(255,106,0,.08);
    color: #fff;
  }
  .lang-fab-option.selected {
    border-color: rgba(255,106,0,.44);
    background: rgba(255,106,0,.12);
    color: #fff;
  }
  @media (max-width: 760px) {
    .lang-fab { left: 10px; bottom: 10px; }
    .lang-fab-trigger { padding: 8px 10px; font-size: 11px; }
  }

  .paper-layout { display:grid; grid-template-columns:250px minmax(0,1fr); gap:18px; align-items:start; }
  .hub-layout { display:grid; grid-template-columns:250px minmax(0,1fr); gap:14px; align-items:start; }

  .deploy-grid-2 { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
  .deploy-grid-3 { display:grid; grid-template-columns:1fr 1fr 1fr; gap:12px; }
  .deploy-panel {
    width: min(980px, 98vw);
    max-height: calc(100vh - 20px);
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  .deploy-header {
    padding: 16px 20px 12px;
    border-bottom: 1px solid rgba(255,255,255,.06);
    background: linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,0));
    flex: 0 0 auto;
  }
  .deploy-body {
    padding: 12px 20px 8px;
    overflow-y: auto;
    min-height: 0;
    flex: 1 1 auto;
  }
  .deploy-actions {
    display:flex;
    justify-content:flex-end;
    gap:10px;
    padding: 12px 20px 16px;
    border-top: 1px solid rgba(255,255,255,.06);
    background: linear-gradient(0deg, rgba(255,255,255,.02), rgba(255,255,255,0));
    flex: 0 0 auto;
  }
  @media (max-height: 760px) {
    .deploy-header { padding: 12px 18px 10px; }
    .deploy-body { padding: 10px 18px 8px; }
    .deploy-actions { padding: 10px 18px 12px; }
  }
  @media (max-width: 760px) {
    .deploy-grid-2, .deploy-grid-3 { grid-template-columns:1fr; }
    .deploy-panel { width: min(98vw, 620px); }
    .deploy-header, .deploy-body, .deploy-actions { padding-left: 14px; padding-right: 14px; }
    .deploy-actions { flex-direction:column-reverse; }
    .deploy-actions > button { width:100%; }
  }

  @media (max-width: 980px) {
    .paper-layout { grid-template-columns:1fr; }
    .paper-nav { position:relative !important; top:auto !important; }
    .hub-layout { grid-template-columns:1fr; }
    .hub-aside { position:relative !important; top:auto !important; }
  }
  @media (max-width: 920px) {
    .nav-grid { grid-template-columns:auto auto; gap:12px; }
    .nav-links { display:none; }
  }

  .mono { font-family:'JetBrains Mono',monospace; }
  .log-scroll { max-height:220px; overflow-y:auto; }
  .log-row:not(:last-child) { border-bottom:1px solid rgba(255,255,255,.04); }
  .feed-row:not(:last-child) { border-bottom:1px solid rgba(255,255,255,.04); }

  @keyframes ticker { 0%{transform:translateX(0)} 100%{transform:translateX(-50%)} }
  .ticker-track { display:flex; animation:ticker 28s linear infinite; width:max-content; }
  .ticker-track:hover { animation-play-state:paused; }
  .ticker-wrap { overflow:hidden; width:100%; }
`;
