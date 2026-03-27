#!/usr/bin/env python3
import argparse
import copy
import json
import os
import struct
import subprocess
import threading
import time
from collections import Counter, deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Deque, Dict, List, Tuple
from urllib.parse import urlparse

BOOT_STAGE_MAP = {
    0: "ROM",
    1: "BOOTLOADER",
    2: "KERNEL",
    3: "DRIVERS",
    4: "SERVICES",
    5: "APP_READY",
}

NET_STATE_MAP = {
    0: "DOWN",
    1: "SCANNING",
    2: "ASSOCIATING",
    3: "ONLINE",
}

ALERT_LEVEL_MAP = {
    1: "INFO",
    2: "WARN",
    3: "CRITICAL",
}

HTML_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Optbinlog Live Dashboard</title>
  <style>
/* ── 设计变量 ─────────────────────────────────── */
:root {
  --bg:        #f0f2f5;
  --card:      rgba(255,255,255,0.82);
  --ink:       #111827;
  --sub:       #6b7280;
  --line:      rgba(17,24,39,0.08);
  --line-md:   rgba(17,24,39,0.14);
  --accent:    #0071e3;
  --accent-s:  rgba(0,113,227,0.12);
  --warn:      #b26a00;
  --danger:    #c8312d;
  --ok:        #168a45;
  --shadow-sm: 0 1px 4px rgba(0,0,0,0.06), 0 4px 16px rgba(0,0,0,0.06);
  --shadow-md: 0 2px 8px rgba(0,0,0,0.06), 0 12px 32px rgba(0,0,0,0.08);
  --r-card:    16px;
  --r-inner:   10px;
  --r-btn:     9px;
  --tf:        0.14s cubic-bezier(0.4,0,0.2,1);
  --ts:        0.24s cubic-bezier(0.4,0,0.2,1);
  --accent-gradient: linear-gradient(135deg,#0071e3,#2563eb);
  --ok-gradient:     linear-gradient(135deg,#16a34a,#15803d);
  --warn-gradient:   linear-gradient(135deg,#d97706,#b45309);
  --danger-gradient: linear-gradient(135deg,#dc2626,#b91c1c);
  --transition-fast: var(--tf);
  --transition-std:  var(--ts);
}
*{ box-sizing:border-box; }

/* ── 页面底色 ─────────────────────────────────── */
body {
  margin:0;
  font-family: "SF Pro Text",-apple-system,BlinkMacSystemFont,"PingFang SC","Helvetica Neue",sans-serif;
  color:var(--ink);
  background:
    radial-gradient(900px 500px at 110% -8%, rgba(0,113,227,0.10) 0%, transparent 55%),
    radial-gradient(700px 480px at -10% 110%, rgba(99,102,241,0.08) 0%, transparent 58%),
    var(--bg);
  font-size:13px;
}

.wrap { max-width:1560px; margin:0 auto; padding:18px 16px 28px; }

/* ── 顶栏 ─────────────────────────────────────── */
.ob-topbar {
  display:flex; align-items:flex-start; justify-content:space-between;
  margin-bottom:14px; gap:12px; flex-wrap:wrap;
}
h1 {
  margin:0; font-size:26px; font-weight:700; letter-spacing:-.025em;
  font-family:"SF Pro Display",-apple-system,"PingFang SC",sans-serif;
  color:var(--ink);
}
.ob-sub { color:var(--sub); font-size:12px; margin-top:3px; }
.ob-topbar-right {
  display:flex;
  flex-direction:column;
  align-items:flex-end;
  gap:6px;
  min-width: 440px;
}
#dv2_phase_bar {
  width: min(560px, 54vw);
  min-width: 420px;
  justify-content: flex-end;
}

/* ── 三栏网格 ─────────────────────────────────── */
.ob-grid {
  display:grid;
  grid-template-columns: minmax(300px, 320px) 1fr 1fr;
  gap:12px;
  align-items:start;
}
.ob-col-left  { display:flex; flex-direction:column; gap:10px; min-width:0; }
.ob-col-mid   { display:flex; flex-direction:column; gap:10px; min-width:0; }
.ob-col-right { display:flex; flex-direction:column; gap:10px; min-width:0; }

/* ── 通用卡片 ─────────────────────────────────── */
.card {
  background:var(--card);
  border:1px solid var(--line);
  border-radius:var(--r-card);
  padding:14px 16px;
  box-shadow:var(--shadow-sm);
  backdrop-filter:saturate(180%) blur(18px);
}

/* ── 卡片标签（取代 h2，更紧凑） ─────────────── */
.ob-card-label {
  font-size:11px; font-weight:700; letter-spacing:.06em;
  text-transform:uppercase; margin-bottom:10px;
  display:flex; align-items:center; gap:6px;
  white-space:nowrap;
  flex-shrink:0;
}
.ob-card-label::before {
  content:""; width:6px; height:6px; border-radius:50%; flex-shrink:0;
}
/* 各模块颜色标识 */
.ob-label-ctrl    { color:#0058b3; }
.ob-label-ctrl::before    { background:#0071e3; box-shadow:0 0 0 3px rgba(0,113,227,0.18); }
.ob-label-status  { color:#0f5f74; }
.ob-label-status::before  { background:#0ea5b7; box-shadow:0 0 0 3px rgba(14,165,183,0.16); }
.ob-label-state   { color:#0058b3; }
.ob-label-state::before   { background:#0071e3; box-shadow:0 0 0 3px rgba(0,113,227,0.18); }
.ob-label-timeline{ color:#166534; }
.ob-label-timeline::before{ background:#16a34a; box-shadow:0 0 0 3px rgba(22,163,74,0.18); }
.ob-label-log     { color:#166534; }
.ob-label-log::before     { background:#16a34a; box-shadow:0 0 0 3px rgba(22,163,74,0.18); }
.ob-label-schema  { color:#334155; }
.ob-label-schema::before  { background:#64748b; box-shadow:0 0 0 3px rgba(100,116,139,0.16); }

/* 卡片边框颜色 */
#card_timeline { border-color:rgba(0,113,227,0.22); }
#card_incident { border-color:rgba(14,116,144,0.20); }
#card_state    { border-color:rgba(0,113,227,0.18); }
.ob-card-timeline { border-color:rgba(22,138,69,0.20); }
#card_log      { border-color:rgba(22,138,69,0.20); }
#card_schema   { border-color:rgba(100,116,139,0.20); }

/* ── 控制台内部 ──────────────────────────────── */
.ob-ctrl-section {
  padding:9px 10px;
  border-radius:var(--r-inner);
  background:rgba(0,0,0,0.025);
  border:1px solid var(--line);
  margin-bottom:8px;
}
.ob-ctrl-section:last-child { margin-bottom:0; }
.ob-ctrl-section-warn {
  background:rgba(178,106,0,0.04);
  border-color:rgba(178,106,0,0.16);
}
.ob-ctrl-section-danger {
  background:rgba(200,49,45,0.04);
  border-color:rgba(200,49,45,0.16);
}
.ob-ctrl-section-title {
  font-size:10px; font-weight:700; letter-spacing:.05em;
  text-transform:uppercase; color:var(--sub);
  margin-bottom:7px;
}
.ob-title-warn   { color:#8f5400; }
.ob-title-danger { color:#9d2120; }

.ob-ctrl-row {
  display:flex; align-items:flex-start; flex-wrap:wrap; gap:6px;
  margin-bottom:5px;
  min-width: 0;
}
.ob-ctrl-row:last-child { margin-bottom:0; }
.ob-ctrl-row-sub { opacity:.85; }
.ob-ctrl-label { font-size:11px; color:var(--sub); white-space:nowrap; }

.ob-btn-group { display:flex; gap:4px; flex-wrap:wrap; width:100%; }
.ob-btn-group > button { flex:1 1 88px; min-width:88px; }
.ob-btn-full  { flex:1 1 108px; min-width:108px; }

/* ── 异常状态内部 ────────────────────────────── */
.ob-status-phase {
  display:flex; gap:6px; flex-wrap:wrap;
}
.ob-status-pill {
  flex:1 1 120px; min-width:0; padding:6px 10px; border-radius:var(--r-inner);
  background:rgba(255,255,255,0.7); border:1px solid var(--line);
}
.ob-pill-k { font-size:10px; color:var(--sub); display:block; margin-bottom:2px; }
.ob-pill-v {
  font-size:14px; font-weight:700; letter-spacing:-.01em; color:var(--ink);
  line-height: 1.2; overflow-wrap: anywhere;
}

/* ── 设备状态网格 ────────────────────────────── */
.ob-state-grid {
  display:grid;
  grid-template-columns:1fr 1fr;
  gap:5px 10px;
  margin-bottom:10px;
}
.ob-kv {
  display:flex; align-items:center; justify-content:space-between;
  padding:4px 8px; border-radius:8px;
  background:rgba(255,255,255,0.6); border:1px solid var(--line);
}
.ob-kv-bar { flex-direction:column; align-items:stretch; gap:3px; }
.ob-k { font-size:10px; color:var(--sub); white-space:nowrap; }
.ob-v { font-size:12px; font-weight:650; color:var(--ink);
        font-variant-numeric:tabular-nums; }
.ob-v-mono { font-family:"SF Mono",Menlo,Monaco,Consolas,monospace; }

/* ── 优化指标行 ──────────────────────────────── */
.ob-perf-row {
  display:flex; align-items:center; flex-wrap:wrap; gap:0;
  padding:7px 10px; border-radius:var(--r-inner);
  background:rgba(0,113,227,0.05); border:1px solid rgba(0,113,227,0.12);
  margin-bottom:8px;
}
.ob-perf-item {
  display:flex; flex-direction:column; align-items:center;
  padding:0 8px;
  min-width: 68px;
}
.ob-perf-k { font-size:9px; color:var(--sub); text-align:center; white-space:nowrap; }
.ob-perf-v { font-size:12px; font-weight:700; color:var(--ink);
             font-variant-numeric:tabular-nums; white-space:nowrap; }
.ob-perf-sep {
  width:1px; height:24px; background:var(--line); flex-shrink:0; margin:0 2px;
}
.ob-counter { font-size:10px; color:var(--sub); margin-top:4px;
              font-family:"SF Mono",Menlo,Monaco,Consolas,monospace; }

/* ── 进度条 ──────────────────────────────────── */
.bar {
  width:100%; height:8px; border-radius:999px;
  background:#e5e9f0; border:1px solid #d8dee8; overflow:hidden;
}
.bar > span {
  display:block; height:100%;
  background:var(--accent-gradient);
  transition:width var(--ts), background var(--ts);
  border-radius:999px;
}
.bar[data-level="warn"]   > span { background:var(--warn-gradient); }
.bar[data-level="danger"] > span {
  background:var(--danger-gradient);
  animation:dv2-pulse-bar 1.4s ease-in-out infinite;
}
@keyframes dv2-pulse-bar { 0%,100%{opacity:1} 50%{opacity:.65} }

/* ── 时间线卡片 ──────────────────────────────── */
.ob-card-timeline { flex:1; min-height:0; }
.timeline {
  overflow:auto; border:1px solid var(--line); border-radius:var(--r-inner);
  background:rgba(255,255,255,0.92); max-height:420px;
}
table { width:100%; border-collapse:collapse; font-size:11px; }
th,td { border-bottom:1px solid rgba(17,24,39,0.06); padding:7px 8px; text-align:left; }
th { position:sticky; top:0; z-index:1; background:#f8fafc;
     font-size:10px; font-weight:700; letter-spacing:.03em; color:var(--sub); }
tr.active-row td  { background:rgba(0,113,227,0.09); }
tr.row-fault td   { background:rgba(200,49,45,0.09); }
tr.row-diag td    { background:rgba(178,106,0,0.08); }
tr.row-recover td { background:rgba(22,138,69,0.09); }
tr.row-powercut td{ background:rgba(173,50,70,0.09); }
tr.row-repair td  { background:rgba(0,113,227,0.10); }
tr.row-repair_fail td { background:rgba(200,49,45,0.09); }
.dv2-tl-fault-left   { border-left:3px solid var(--danger); }
.dv2-tl-recover-left { border-left:3px solid var(--ok); }
.dv2-tl-new          { animation:dv2-tl-enter .18s ease; }
@keyframes dv2-tl-enter { from{opacity:0;transform:translateX(-4px)} to{opacity:1;transform:translateX(0)} }
@keyframes dv2-live-pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.35;transform:scale(.65)} }

/* ── 标注条 ──────────────────────────────────── */
.mark-strip {
  border:1px dashed #cbd5e1; background:rgba(248,250,252,0.92);
  border-radius:var(--r-inner); padding:5px 8px;
  display:flex; flex-wrap:wrap; gap:5px; min-height:32px; align-items:center;
}
.mark {
  display:inline-flex; align-items:center; gap:4px;
  border:1px solid #d6dee9; border-radius:999px;
  padding:2px 7px; font-size:10px; background:#fff;
}

/* ── 日志联动 ────────────────────────────────── */
.ob-log-col-label {
  font-size:10px; font-weight:700; color:var(--sub);
  letter-spacing:.03em; margin-bottom:4px; display:flex; align-items:center; flex-wrap:wrap; gap:4px;
}
.ob-timeline-tools {
  border:1px solid var(--line);
  border-radius:var(--r-inner);
  background:rgba(255,255,255,0.72);
  padding:7px 8px;
  margin-bottom:8px;
}
.ob-tline-row {
  display:flex;
  align-items:center;
  gap:6px;
  flex-wrap:wrap;
  min-width:0;
}
.ob-tline-row + .ob-tline-row {
  margin-top:6px;
  padding-top:6px;
  border-top:1px dashed rgba(17,24,39,0.10);
}
.ob-tline-row .mono {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.map-list {
  border:1px solid var(--line); border-radius:var(--r-inner);
  background:rgba(255,255,255,0.95); overflow:auto; padding:5px;
}
.map-row {
  padding:5px 7px; border-radius:7px; margin-bottom:3px;
  border:1px solid transparent; cursor:pointer;
  white-space:pre-wrap; word-break:break-word;
  font-family:"SF Mono",Menlo,Monaco,Consolas,monospace;
  font-size:10px; line-height:1.35;
  transition:background .12s ease, border-color .12s ease;
}
.map-row[data-side="dec"] {
  font-family:"SF Pro Text",-apple-system,"PingFang SC",sans-serif;
  font-size:11px; line-height:1.5;
}
.map-row:hover    { background:#f3f6fb; border-color:#cfd8e6; }
.map-row.active   { background:rgba(0,113,227,0.09); border-color:rgba(0,113,227,0.32); }
.map-row.locked   { background:rgba(0,113,227,0.15); border-color:rgba(0,113,227,0.50); }
.map-row.head     { font-weight:700; color:#3e3934; cursor:default; }
.map-row.anomaly-fault    { border-color:rgba(200,49,45,0.30); background:rgba(200,49,45,0.09); }
.map-row.anomaly-diag     { border-color:rgba(178,106,0,0.30); background:rgba(178,106,0,0.07); }
.map-row.anomaly-recover  { border-color:rgba(22,138,69,0.30); background:rgba(22,138,69,0.09); }
.map-row.anomaly-powercut { border-color:rgba(173,50,70,0.30); background:rgba(173,50,70,0.09); }
.map-row.anomaly-repair   { border-color:rgba(0,113,227,0.32); background:rgba(0,113,227,0.09); }
.map-row.anomaly-repair_fail { border-color:rgba(200,49,45,0.30); background:rgba(200,49,45,0.09); }

/* ── Schema 卡片内部分区 ─────────────────────── */
.ob-schema-section {
  margin-bottom:12px; padding-bottom:12px;
  border-bottom:1px solid var(--line);
}
.ob-schema-section:last-child { margin-bottom:0; padding-bottom:0; border-bottom:none; }
.ob-schema-sec-title {
  font-size:10px; font-weight:700; letter-spacing:.04em;
  text-transform:uppercase; color:var(--sub); margin-bottom:6px;
}

/* ── 徽章 ────────────────────────────────────── */
.badge {
  display:inline-block; border-radius:999px; padding:1px 6px;
  font-size:10px; line-height:1.4; border:1px solid #d7deea;
  background:#f4f7fb; color:#344054; white-space:nowrap;
}
.badge-fault      { background:rgba(200,49,45,0.09); border-color:rgba(200,49,45,0.32); color:#9d2120; }
.badge-diag       { background:rgba(178,106,0,0.09); border-color:rgba(178,106,0,0.30); color:#8f5400; }
.badge-recover    { background:rgba(22,138,69,0.09); border-color:rgba(22,138,69,0.30); color:#176e3d; }
.badge-powercut   { background:rgba(173,50,70,0.09); border-color:rgba(173,50,70,0.32); color:#8c2a3b; }
.badge-repair     { background:rgba(0,113,227,0.09); border-color:rgba(0,113,227,0.32); color:#0058b3; }
.badge-repair_fail{ background:rgba(200,49,45,0.09); border-color:rgba(200,49,45,0.32); color:#9d2120; }
.badge-control    { background:#eef2f8; border-color:#c8d2e2; color:#374151; }

/* ── 异常状态框 ──────────────────────────────── */
.incident {
  border-left:4px solid var(--ok); border-radius:var(--r-inner);
  padding:8px 10px; background:rgba(22,138,69,0.07);
}
.incident.warn   { border-left-color:var(--warn); background:rgba(178,106,0,0.09); }
.incident.danger { border-left-color:var(--danger); background:rgba(200,49,45,0.09); }
.incident .title { font-size:12px; font-weight:700; margin-bottom:5px; }
.incident ul { margin:0; padding-left:16px; font-size:11px; }

/* ── 按钮 ────────────────────────────────────── */
button, select, input[type="number"] {
  border:1px solid var(--line);
  background:rgba(255,255,255,0.92);
  border-radius:var(--r-btn);
  padding:6px 9px;
  font-size:11px;
  color:var(--ink);
  font-family:inherit;
  cursor:pointer;
  transition:transform var(--tf), box-shadow var(--tf),
             background-color .18s ease, border-color .18s ease;
  line-height: 1.25;
}
button { white-space: normal; min-height: 30px; overflow-wrap: anywhere; }
select, input[type="number"] { white-space: nowrap; }
input[type="number"] { cursor:text; }
.ob-input-sm { padding:4px 6px; font-size:11px; min-height: 28px; }
.ob-btn-sm   { padding:4px 8px; font-size:10px; min-height: 28px; }
.ob-select-sm{ padding:4px 6px; font-size:10px; min-height: 28px; }
button:hover, select:hover {
  transform:translateY(-1px);
  box-shadow:0 4px 14px rgba(17,24,39,0.10);
  border-color:var(--line-md); background:#fff;
}
button:active { transform:translateY(0); box-shadow:inset 0 1px 2px rgba(17,24,39,0.14); }
button:disabled { opacity:.5; cursor:not-allowed; transform:none; }

button.primary {
  background:var(--accent-s); border-color:rgba(0,113,227,0.32);
  color:#004a95; font-weight:700;
}
button.warn {
  background:rgba(178,106,0,0.10); border-color:rgba(178,106,0,0.26); color:#8e5100;
}
button.danger {
  background:rgba(200,49,45,0.10); border-color:rgba(200,49,45,0.28); color:#8e211e;
}
button.done { background:rgba(22,138,69,0.14); border-color:rgba(22,138,69,0.34); }
button.fail { background:rgba(200,49,45,0.12); border-color:rgba(200,49,45,0.30); }
button.pending { opacity:.72; cursor:wait; }

button.dv2-loading { position:relative; color:transparent!important; pointer-events:none; }
button.dv2-loading::after {
  content:""; position:absolute; width:12px; height:12px;
  border:2px solid #9ca3af; border-top-color:transparent;
  border-radius:50%; top:50%; left:50%;
  transform:translate(-50%,-50%);
  animation:dv2-btn-spin .5s linear infinite;
}
@keyframes dv2-btn-spin { to{transform:translate(-50%,-50%) rotate(360deg)} }
button.dv2-flash-ok   { animation:dv2-flash-ok   .32s ease; }
button.dv2-flash-fail { animation:dv2-flash-fail  .32s ease; }
@keyframes dv2-flash-ok   { 0%{background:rgba(22,163,74,0.30)}  100%{background:inherit} }
@keyframes dv2-flash-fail { 0%{background:rgba(220,38,38,0.30)}  100%{background:inherit} }

/* ── pre / mono ──────────────────────────────── */
pre {
  margin:0; background:rgba(255,255,255,0.95);
  border:1px solid var(--line); border-radius:var(--r-inner);
  padding:8px; max-height:200px; overflow:auto;
  white-space:pre-wrap; word-break:break-word;
}
.mono { font-family:"SF Mono",Menlo,Monaco,Consolas,monospace; font-size:11px; }

/* ── 阶段进度条 ──────────────────────────────── */
.dv2-phase-node {
  display:flex; flex-direction:column; align-items:center;
  position:relative; padding-bottom:18px; flex-shrink:0; min-width:56px;
}
.dv2-phase-dot {
  width:8px; height:8px; border-radius:50%;
  background:var(--line-md); flex-shrink:0;
  transition:background var(--ts), box-shadow var(--ts);
}
.dv2-phase-dot.done   { background:var(--ok); }
.dv2-phase-dot.active { background:var(--accent); box-shadow:0 0 0 3px rgba(0,113,227,0.18); }
.dv2-phase-label {
  position:absolute; top:13px; left:50%; transform:translateX(-50%);
  font-size:10px; color:var(--sub); white-space:nowrap;
  transition:color var(--ts);
}
.dv2-phase-label.active { color:var(--accent); font-weight:700; }
.dv2-phase-label.done   { color:var(--ok); }
.dv2-phase-line {
  flex:1; min-width:24px; height:2px; background:var(--line); margin-top:-4px; margin-bottom:18px;
  transition:background var(--ts);
}
.dv2-phase-line.done { background:var(--ok); }

/* ── Toast ───────────────────────────────────── */
.dv2-toast-wrap {
  position:fixed; bottom:18px; right:18px;
  display:flex; flex-direction:column; gap:7px;
  z-index:9999; pointer-events:none;
}
.dv2-toast {
  background:rgba(22,22,24,0.92); color:#fff;
  border-radius:12px; padding:9px 14px; font-size:12px;
  backdrop-filter:blur(16px);
  box-shadow:0 8px 28px rgba(0,0,0,0.26);
  animation:dv2-toast-in .18s ease;
  border-left:3px solid var(--ok); max-width:320px;
}
.dv2-toast.fail { border-left-color:var(--danger); }
@keyframes dv2-toast-in { from{opacity:0;transform:translateY(5px)} to{opacity:1;transform:translateY(0)} }

/* ── 快捷键帮助条 ────────────────────────────── */
.ob-shortcut-bar {
  margin-top:12px; padding:7px 12px; border-radius:10px;
  background:rgba(0,0,0,0.035); font-size:10px; color:var(--sub);
  cursor:pointer; user-select:none;
}
.ob-shortcut-bar span { margin-left:8px; }

/* ── 响应式 ──────────────────────────────────── */
@media (max-width:1100px) {
  .ob-grid { grid-template-columns:1fr; }
  .ob-col-left,.ob-col-mid,.ob-col-right { grid-column:1; }
  .ob-topbar-right {
    width:100%;
    min-width:0;
    align-items:flex-start;
  }
  #dv2_phase_bar {
    width:100%;
    min-width:0;
    justify-content:flex-start;
  }
}
@media (max-width:1380px) {
  .ob-perf-sep { display:none; }
  .ob-perf-row { gap:6px; }
}
  </style>
</head>
<body>
  <div class="dv2-toast-wrap" id="dv2_toast_wrap"></div>
  <div class="wrap">

    <!-- ══════════════════════════════════════════════════════════════
         顶栏：标题 + 阶段进度条
    ══════════════════════════════════════════════════════════════ -->
    <div class="ob-topbar">
      <div>
        <h1>Optbinlog Live Dashboard</h1>
        <div class="ob-sub">Binary Runtime Logger · 实时流程 · 异常闭环 · 结构可视化</div>
      </div>
      <div class="ob-topbar-right">
        <div id="server_time" class="ob-sub" style="font-size:11px"></div>
        <!-- 阶段进度条 -->
        <div id="dv2_phase_bar"
             style="display:flex;align-items:center;gap:0;padding:0 2px"></div>
      </div>
    </div>

    <!-- ══════════════════════════════════════════════════════════════
         三栏主布局
    ══════════════════════════════════════════════════════════════ -->
    <div class="ob-grid">

      <!-- ╔══════════════════════════════════════════════════════════╗
           ║  左列：控制台                                            ║
           ╚══════════════════════════════════════════════════════════╝ -->
      <div class="ob-col-left">

        <!-- 【控制台】卡片 -->
        <div class="card ob-card-ctrl" id="card_timeline">
          <div class="ob-card-label ob-label-ctrl">控制台</div>

          <!-- 设备运行组 -->
          <div class="ob-ctrl-section">
            <div class="ob-ctrl-section-title">设备运行</div>
            <div class="ob-ctrl-row">
              <div class="ob-btn-group">
                <button class="danger" id="device_pause">⏸ 暂停</button>
                <button class="primary" id="device_resume">▶ 继续</button>
                <button class="warn" id="device_step">+1 写入</button>
              </div>
            </div>
            <div class="ob-ctrl-row ob-ctrl-row-sub">
              <span class="mono" style="font-size:10px;color:var(--sub)">
                事件展示相关控件已移至中栏“事件时间线”
              </span>
            </div>
          </div>

          <!-- 异常注入组 -->
          <div class="ob-ctrl-section ob-ctrl-section-warn">
            <div class="ob-ctrl-section-title ob-title-warn">异常注入</div>
            <div class="ob-ctrl-row">
              <button class="danger ob-btn-full" id="inject_fault">⚠ 注入故障</button>
              <button class="warn ob-btn-full" id="inject_diag">🔍 诊断重试</button>
              <button class="primary ob-btn-full" id="inject_recover">✓ 注入恢复</button>
            </div>
            <div id="inject_result" style="font-size:10px;color:var(--sub);margin-top:4px;min-height:14px"></div>
          </div>

          <!-- 断电控制组 -->
          <div class="ob-ctrl-section ob-ctrl-section-danger">
            <div class="ob-ctrl-section-title ob-title-danger">断电 · 恢复</div>
            <div class="ob-ctrl-row" style="gap:5px">
              <span class="ob-ctrl-label">截断</span>
              <input id="powercut_drop_bytes" type="number" min="1" step="1" value="7"
                     class="ob-input-sm" style="width:44px" />
              <span class="ob-ctrl-label">B</span>
              <button class="danger ob-btn-full" id="simulate_powercut">⚡ 断电</button>
              <button class="primary ob-btn-full" id="repair_powercut">🔧 修复</button>
            </div>
          </div>

        </div><!-- /card_timeline -->

        <!-- 【异常状态】卡片 -->
        <div class="card ob-card-status" id="card_incident">
          <div class="ob-card-label ob-label-status">异常状态</div>
          <div class="ob-status-phase">
            <span class="ob-status-pill ob-pill-phase">
              <span class="ob-pill-k">阶段</span>
              <span id="phase" class="ob-pill-v">--</span>
            </span>
            <span class="ob-status-pill ob-pill-incident">
              <span class="ob-pill-k">异常</span>
              <span id="incident_state" class="ob-pill-v">--</span>
            </span>
          </div>
          <div id="incident_box" class="incident" style="margin-top:8px">
            <div class="title">等待异常事件...</div>
            <ul><li>尚未检测到告警</li></ul>
          </div>
        </div>

      </div><!-- /ob-col-left -->

      <!-- ╔══════════════════════════════════════════════════════════╗
           ║  中列：状态模块                                           ║
           ╚══════════════════════════════════════════════════════════╝ -->
      <div class="ob-col-mid">

        <!-- 【设备状态 + 优化指标】卡片 -->
        <div class="card ob-card-state" id="card_state">
          <div class="ob-card-label ob-label-state">设备状态</div>

          <!-- 状态网格：紧凑 2×N -->
          <div class="ob-state-grid">
            <div class="ob-kv"><span class="ob-k">Boot</span><span id="boot" class="ob-v">--</span></div>
            <div class="ob-kv"><span class="ob-k">Network</span><span id="net" class="ob-v">--</span></div>
            <div class="ob-kv"><span class="ob-k">Uptime</span><span id="uptime" class="ob-v ob-v-mono">--</span></div>
            <div class="ob-kv"><span class="ob-k">RSSI</span><span id="rssi" class="ob-v">--</span></div>
            <div class="ob-kv ob-kv-bar">
              <span class="ob-k">Battery</span>
              <div style="display:flex;align-items:center;gap:6px;min-width:0">
                <span id="soc" class="ob-v" style="flex-shrink:0">--</span>
                <div class="bar" style="flex:1"><span id="soc_bar" style="width:0%"></span></div>
              </div>
            </div>
            <div class="ob-kv ob-kv-bar">
              <span class="ob-k">Latency</span>
              <div style="display:flex;align-items:center;gap:6px;min-width:0">
                <span id="latency" class="ob-v" style="flex-shrink:0">--</span>
                <div class="bar" style="flex:1"><span id="latency_bar" style="width:0%"></span></div>
              </div>
            </div>
          </div>

          <!-- 优化指标一行（简洁版，不重复） -->
          <div class="ob-perf-row">
            <div class="ob-perf-item">
              <span class="ob-perf-k">Binary</span>
              <strong id="dv2_perf_bin" class="ob-perf-v">--</strong>
            </div>
            <div class="ob-perf-sep"></div>
            <div class="ob-perf-item">
              <span class="ob-perf-k">Text 估算</span>
              <strong id="dv2_perf_txt" class="ob-perf-v">--</strong>
            </div>
            <div class="ob-perf-sep"></div>
            <div class="ob-perf-item">
              <span class="ob-perf-k">压缩比</span>
              <strong id="dv2_perf_ratio" class="ob-perf-v" style="color:var(--ok)">--</strong>
            </div>
            <div class="ob-perf-sep"></div>
            <div class="ob-perf-item">
              <span class="ob-perf-k">吞吐</span>
              <strong id="dv2_perf_rps_label" class="ob-perf-v">-- r/s</strong>
            </div>
            <div class="ob-perf-sep"></div>
            <div class="ob-perf-item">
              <span class="ob-perf-k">解码</span>
              <strong id="dv2_perf_dec" class="ob-perf-v">--</strong>
            </div>
            <div class="ob-perf-sep"></div>
            <div class="ob-perf-item">
              <span class="ob-perf-k">记录</span>
              <strong id="records" class="ob-perf-v">--</strong>
            </div>
            <!-- Binary vs Text 微型对比条（替代原先独立面板） -->
            <div style="flex:1;min-width:80px">
              <div id="dv2_perf_compare" style="display:flex;flex-direction:column;gap:3px"></div>
            </div>
          </div>

          <!-- 计数器（折叠，次要信息） -->
          <div id="counter" class="ob-counter">--</div>
        </div>

        <!-- 【事件时间线】卡片 -->
        <div class="card ob-card-timeline">
          <div class="ob-card-label ob-label-timeline">事件时间线</div>
          <div class="ob-timeline-tools">
            <div class="ob-tline-row">
              <button id="toggle_refresh" class="ob-btn-sm">⏸ 轮询</button>
              <select id="refresh_ms" class="ob-select-sm">
                <option value="250">250ms</option>
                <option value="400">400ms</option>
                <option value="800" selected>800ms</option>
                <option value="1200">1200ms</option>
              </select>
              <select id="view_mode" class="ob-select-sm">
                <option value="auto" selected>自动跟随</option>
                <option value="step">逐步查看</option>
              </select>
              <button id="step_prev" class="ob-btn-sm">◀</button>
              <button id="step_next" class="ob-btn-sm">▶</button>
              <span class="mono" id="step_info" style="font-size:10px;color:var(--sub)">--</span>
              <label style="font-size:10px;color:var(--sub)">Tag
                <select id="tag_filter" class="ob-select-sm" style="margin-left:3px">
                  <option value="">全部</option>
                </select>
              </label>
              <a href="/api/decoded.jsonl" target="_blank" rel="noopener"
                 style="font-size:10px;color:var(--accent);margin-left:auto">↓ JSONL</a>
            </div>
            <div class="ob-tline-row">
              <span class="ob-ctrl-label">间隔</span>
              <input id="device_interval" type="number" min="0" step="10" value="180"
                     class="ob-input-sm" style="width:64px" />
              <span class="ob-ctrl-label">ms</span>
              <button id="device_interval_apply" class="ob-btn-sm">应用</button>
              <span class="mono" id="device_state" style="font-size:10px;color:var(--sub);margin-left:auto;max-width:340px"></span>
            </div>
          </div>
          <div id="timeline_marks" class="mark-strip" style="margin-bottom:8px"></div>
          <div class="timeline" id="timeline_box">
            <table>
              <thead>
                <tr>
                  <th style="width:14px;padding:6px 4px"></th>
                  <th>#</th><th>uptime</th><th>tag</th><th>标注</th><th>summary</th>
                </tr>
              </thead>
              <tbody id="timeline"></tbody>
            </table>
          </div>
        </div>

      </div><!-- /ob-col-mid -->

      <!-- ╔══════════════════════════════════════════════════════════╗
           ║  右列：数据与结构展示                                     ║
           ╚══════════════════════════════════════════════════════════╝ -->
      <div class="ob-col-right">

        <!-- 【日志联动】卡片 -->
        <div class="card ob-card-log" id="card_log">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
            <div class="ob-card-label ob-label-log" style="margin-bottom:0">日志联动</div>
            <div style="display:flex;gap:12px;font-size:10px;color:var(--sub)">
              <span>shared <span class="mono" id="shared_head_meta">--</span></span>
              <span>run.bin <span class="mono" id="log_tail_meta">--</span></span>
              <span class="mono" id="frame_parse_error" style="transition:color .2s"></span>
            </div>
          </div>

          <!-- shared.bin 十六进制（折叠） -->
          <details style="margin-bottom:6px">
            <summary style="font-size:10px;color:var(--sub);cursor:pointer;
                            user-select:none">shared.bin 原始头部 ▸</summary>
            <pre class="mono" id="shared_hex"
                 style="margin-top:4px;max-height:80px;font-size:9px;
                        line-height:1.4;border-radius:8px;padding:6px;
                        background:rgba(0,0,0,0.03);border:1px solid var(--line)">--</pre>
          </details>

          <!-- 双列联动 -->
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px">
            <div>
              <div class="ob-log-col-label">Binary Frames
                <span class="mono" id="readable_meta"
                      style="font-size:10px;color:var(--sub);margin-left:4px">--</span>
              </div>
              <div class="map-list" id="binary_map_list" style="height:300px">--</div>
            </div>
            <div>
              <div class="ob-log-col-label">Decoded Log</div>
              <div class="map-list" id="decoded_map_list" style="height:300px">--</div>
            </div>
          </div>

          <!-- 断电状态区（默认隐藏） -->
          <div id="powercut_status_row" style="margin-top:8px;display:none">
            <div style="padding:8px 10px;border-radius:10px;
                        border:1px solid rgba(200,49,45,0.28);
                        background:rgba(200,49,45,0.05)">
              <div style="display:flex;flex-wrap:wrap;gap:6px;
                          align-items:center;margin-bottom:6px">
                <span style="font-size:11px;font-weight:700;color:var(--danger)"
                      id="powercut_case_box">断电恢复状态：--</span>
                <span class="mono" style="font-size:10px;color:var(--sub)"
                      id="powercut_case_meta">--</span>
              </div>
              <div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:6px;margin-bottom:6px">
                <div style="border:1px solid rgba(200,49,45,0.22);background:rgba(200,49,45,0.04);border-radius:8px;padding:4px 6px">
                  <div style="font-size:9px;color:var(--sub)">断电瞬时丢失</div>
                  <div class="mono" id="pc_loss_cut" style="font-size:11px;color:var(--danger);font-weight:700">--</div>
                </div>
                <div style="border:1px solid rgba(178,106,0,0.22);background:rgba(178,106,0,0.04);border-radius:8px;padding:4px 6px">
                  <div style="font-size:9px;color:var(--sub)">修复额外裁剪</div>
                  <div class="mono" id="pc_loss_repair" style="font-size:11px;color:var(--warn);font-weight:700">--</div>
                </div>
                <div style="border:1px solid rgba(17,24,39,0.14);background:rgba(0,0,0,0.02);border-radius:8px;padding:4px 6px">
                  <div style="font-size:9px;color:var(--sub)">总字节丢失</div>
                  <div class="mono" id="pc_loss_total" style="font-size:11px;color:var(--ink);font-weight:700">--</div>
                </div>
                <div style="border:1px solid rgba(0,113,227,0.20);background:rgba(0,113,227,0.04);border-radius:8px;padding:4px 6px">
                  <div style="font-size:9px;color:var(--sub)">完整帧变化</div>
                  <div class="mono" id="pc_loss_records" style="font-size:11px;color:var(--accent);font-weight:700">--</div>
                </div>
              </div>
              <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px">
                <div>
                  <div style="font-size:9px;color:var(--sub);margin-bottom:2px">截断前尾部</div>
                  <pre class="mono" id="hex_before"
                       style="font-size:9px;margin:0;max-height:52px;border-radius:7px;
                              padding:4px;background:rgba(0,0,0,0.03);
                              border:1px solid var(--line)">--</pre>
                </div>
                <div>
                  <div style="font-size:9px;color:var(--danger);margin-bottom:2px">已截断</div>
                  <pre class="mono" id="hex_dropped"
                       style="font-size:9px;margin:0;max-height:52px;border-radius:7px;
                              padding:4px;background:rgba(200,49,45,0.06);
                              border:1px solid rgba(200,49,45,0.2)">--</pre>
                </div>
                <div>
                  <div style="font-size:9px;color:var(--ok);margin-bottom:2px">修复后</div>
                  <pre class="mono" id="hex_after"
                       style="font-size:9px;margin:0;max-height:52px;border-radius:7px;
                              padding:4px;background:rgba(22,138,69,0.06);
                              border:1px solid rgba(22,138,69,0.2)">--</pre>
                </div>
              </div>
              <div style="font-size:10px;color:var(--sub);margin-top:6px;border-top:1px dashed rgba(17,24,39,0.14);padding-top:6px"
                   id="pc_truth_note">恢复策略：--</div>
              <div style="font-size:10px;color:var(--sub);margin-top:4px"
                   id="powercut_result">断电影响：--</div>
            </div>
          </div>
        </div><!-- /card_log -->

        <!-- 【Schema 结构】卡片 -->
        <div class="card ob-card-schema" id="card_schema">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
            <div class="ob-card-label ob-label-schema" style="margin-bottom:0">格式结构</div>
            <div style="font-size:10px;color:var(--sub);display:flex;gap:10px">
              <span class="mono" id="dv2_schema_hash">--</span>
              <span id="decode_state" class="mono" style="max-width:220px;overflow:hidden;
                                                           text-overflow:ellipsis;white-space:nowrap">--</span>
            </div>
          </div>
          <!-- schema_src 保留但隐藏，JS 仍赋值 -->
          <div id="schema_src" style="display:none"></div>
          <div id="dv2_schema_gen" style="display:none"></div>

          <!-- 帧整体结构 -->
          <div class="ob-schema-section">
            <div class="ob-schema-sec-title">帧结构 · Frame Layout</div>
            <div id="dv2_frame_layout_bar"
                 style="display:flex;border-radius:7px;overflow:hidden;
                        height:28px;border:1px solid var(--line);margin-bottom:4px"></div>
            <!-- Header 位域 -->
            <div style="font-size:9px;color:var(--sub);margin:5px 0 2px">Header 位域（32 bits）</div>
            <div style="display:flex;border-radius:6px;overflow:hidden;
                        height:22px;border:1px solid rgba(0,113,227,0.3)">
              <div style="width:90.6%;background:rgba(0,113,227,0.18);display:flex;align-items:center;
                          justify-content:center;font-size:9px;color:#004a95;font-weight:600;
                          border-right:1px solid rgba(0,113,227,0.3)">payload_len [28:0] 29b</div>
              <div style="width:3.1%;background:rgba(139,92,246,0.25);display:flex;align-items:center;
                          justify-content:center;font-size:8px;color:#5b21b6;
                          border-right:1px solid rgba(139,92,246,0.3)" title="varstr [29]">v</div>
              <div style="width:6.3%;background:rgba(245,158,11,0.25);display:flex;align-items:center;
                          justify-content:center;font-size:8px;color:#92400e;font-weight:600"
                   title="checksum_type [31:30]">cs[2]</div>
            </div>
            <!-- Payload 内部 -->
            <div style="font-size:9px;color:var(--sub);margin:5px 0 2px">Payload 内部结构</div>
            <div id="dv2_payload_layout_bar"
                 style="display:flex;border-radius:6px;overflow:hidden;
                        height:22px;border:1px solid rgba(22,138,69,0.3)"></div>
            <div id="dv2_payload_layout_legend"
                 style="display:flex;gap:8px;margin-top:4px;flex-wrap:wrap;
                        font-size:9px;color:var(--sub)"></div>
            <div id="dv2_frame_layout_legend"
                 style="display:flex;gap:10px;margin-top:5px;flex-wrap:wrap;
                        font-size:9px;color:var(--sub)"></div>
          </div>

          <!-- shared.bin 分区 -->
          <div class="ob-schema-section">
            <div class="ob-schema-sec-title">shared.bin 内存分区</div>
            <div id="dv2_shared_layout_bar"
                 style="display:flex;border-radius:7px;overflow:hidden;
                        height:24px;border:1px solid var(--line)"></div>
            <div id="dv2_shared_layout_legend"
                 style="display:flex;gap:8px;margin-top:4px;flex-wrap:wrap;
                        font-size:9px;color:var(--sub)"></div>
          </div>

          <!-- 事件类型编码尺寸 -->
          <div class="ob-schema-section">
            <div class="ob-schema-sec-title">事件编码尺寸（Binary per frame）</div>
            <div id="dv2_tag_bars" style="display:flex;flex-direction:column;gap:5px"></div>
          </div>

          <!-- 字段详情（点击展开） -->
          <div id="dv2_tag_fields"
               style="margin-top:8px;display:none;border-top:1px solid var(--line);padding-top:8px">
            <div style="font-size:11px;font-weight:600;color:var(--sub);margin-bottom:5px"
                 id="dv2_fields_title"></div>
            <div id="dv2_fields_content"
                 style="font-size:10px;font-family:'SF Mono',Menlo,Monaco,Consolas,monospace;
                        line-height:1.65"></div>
          </div>
        </div><!-- /card_schema -->

      </div><!-- /ob-col-right -->

    </div><!-- /ob-grid -->

    <!-- 快捷键帮助条 -->
    <div class="ob-shortcut-bar"
         onclick="this.querySelector('span').style.display =
                  this.querySelector('span').style.display==='none'?'inline':'none'">
      ⌨️ 快捷键
      <span>
        Space=暂停/继续 &nbsp; s/→=写入一条 &nbsp; b/←=视图回退
        &nbsp; 1/2/3=故障/诊断/恢复 &nbsp; 4=断电 &nbsp; r=修复 &nbsp; Esc=收起详情
      </span>
    </div>

  </div><!-- /wrap -->

  <script>
    let paused = false;
    let timer = null;
    let stepCursor = -1;
    let lastTagSig = '';
    let mapHoverIndex = -1;
    let mapLockIndex = -1;
    let mapDefaultIndex = -1;
    const dv2_thrHistory = [];
    const dv2_PHASES = ['Boot', 'Network', 'Runtime', 'Fault', 'Recovery'];
    let dv2_backendOnline = true;

    function fmtBytes(n) {
      const x = Number(n || 0);
      if (x < 1024) return x + ' B';
      if (x < 1024 * 1024) return (x / 1024).toFixed(1) + ' KB';
      return (x / 1024 / 1024).toFixed(2) + ' MB';
    }

    function esc(text) {
      return String(text ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function nowShort() {
      const d = new Date();
      return d.toLocaleTimeString('zh-CN', { hour12: false });
    }

    function anomalyLabel(kind) {
      const m = {
        fault: '故障',
        diag: '诊断',
        recover: '恢复',
        powercut: '断电',
        repair: '修复',
        repair_fail: '修复失败',
        control: '控制',
      };
      return m[kind] || '';
    }

    function anomalyBadge(kind) {
      const label = anomalyLabel(kind);
      if (!label) return '';
      return `<span class="badge badge-${kind}">${label}</span>`;
    }

    function anomalyRowClass(kind) {
      if (!kind) return '';
      return `row-${kind}`;
    }

    function anomalyMapClass(kind) {
      if (!kind) return '';
      return `anomaly-${kind}`;
    }

    function powercutStageLabel(stage) {
      const m = {
        idle: '空闲',
        detected: '待恢复(已检测到截断)',
        undetected: '未检测到截断',
        await_resume: '待继续时自动恢复',
        repaired: '手动已恢复',
        clean: '手动检查后无需裁剪',
        auto_resume_repaired: '继续时已自动恢复',
        auto_resume_clean: '继续时检查后无需裁剪',
        repair_failed: '恢复失败',
      };
      return m[stage] || stage || '--';
    }

    function setActionFeedback(text, ok = true) {
      // action_feedback 已移除：统一使用 toast 反馈，保留函数供旧调用点兼容
      dv2_toast(`${nowShort()} ${String(text || '--')}`, ok ? 'ok' : 'fail');
    }

    function flashButton(id, ok = true) {
      const btn = document.getElementById(id);
      if (!btn) return;
      btn.classList.remove('done', 'fail');
      btn.classList.add(ok ? 'done' : 'fail');
      window.setTimeout(() => btn.classList.remove('done', 'fail'), 800);
    }

    async function withPending(buttonId, fn) {
      const btn = document.getElementById(buttonId);
      if (btn) {
        btn.disabled = true;
        btn.classList.add('pending');
      }
      try {
        return await fn();
      } finally {
        if (btn) {
          btn.disabled = false;
          btn.classList.remove('pending');
        }
      }
    }

    function dv2_toast(dv2_msg, dv2_type) {
      const dv2_wrap = document.getElementById('dv2_toast_wrap');
      if (!dv2_wrap) return;
      const dv2_el = document.createElement('div');
      dv2_el.className = 'dv2-toast' + (dv2_type === 'fail' ? ' fail' : '');
      dv2_el.textContent = String(dv2_msg || '--');
      dv2_wrap.appendChild(dv2_el);
      setTimeout(() => { dv2_el.style.transition = 'opacity 0.25s'; dv2_el.style.opacity = '0'; }, 2200);
      setTimeout(() => { try { dv2_el.remove(); } catch(_) {} }, 2500);
    }

    function dv2_markBackend(dv2_ok, dv2_msg = '') {
      const dv2_time = document.getElementById('server_time');
      if (dv2_ok) {
        if (!dv2_backendOnline) {
          dv2_toast('后端连接已恢复', 'ok');
        }
        dv2_backendOnline = true;
        if (dv2_time) dv2_time.style.color = '';
        return;
      }
      if (dv2_backendOnline) {
        dv2_toast('后端连接异常: ' + String(dv2_msg || 'snapshot failed'), 'fail');
      }
      dv2_backendOnline = false;
      if (dv2_time) {
        dv2_time.textContent = '服务器连接异常';
        dv2_time.style.color = 'var(--danger)';
      }
    }

    function dv2_btnAction(dv2_btn, dv2_fetchPromise, dv2_onDone) {
      if (!dv2_btn) return;
      dv2_btn.classList.add('dv2-loading');
      dv2_btn.disabled = true;
      dv2_fetchPromise
        .then(async (dv2_resp) => {
          let dv2_data = null;
          try {
            dv2_data = await dv2_resp.json();
          } catch (dv2_parseErr) {
            dv2_data = {
              ok: false,
              error: `invalid json response (http ${dv2_resp.status})`,
            };
          }
          return { resp: dv2_resp, data: dv2_data };
        })
        .then(({ resp, data }) => {
          dv2_btn.classList.remove('dv2-loading');
          dv2_btn.disabled = false;
          const dv2_ok = !!(resp && resp.ok && data && data.ok);
          dv2_btn.classList.add(dv2_ok ? 'dv2-flash-ok' : 'dv2-flash-fail');
          setTimeout(() => dv2_btn.classList.remove('dv2-flash-ok', 'dv2-flash-fail'), 400);
          const dv2_err = (data && data.error) || (resp && !resp.ok ? `http ${resp.status}` : 'unknown');
          dv2_toast(dv2_ok ? (data.message || '操作成功') : ('失败: ' + dv2_err), dv2_ok ? 'ok' : 'fail');
          if (dv2_onDone) dv2_onDone(dv2_ok, data);
        })
        .catch(dv2_err => {
          dv2_btn.classList.remove('dv2-loading');
          dv2_btn.disabled = false;
          dv2_btn.classList.add('dv2-flash-fail');
          setTimeout(() => dv2_btn.classList.remove('dv2-flash-fail'), 400);
          dv2_toast('网络错误: ' + String(dv2_err), 'fail');
        });
    }

    function pct(v, lo, hi) {
      const x = Number(v || 0);
      if (hi <= lo) return 0;
      return Math.max(0, Math.min(100, ((x - lo) / (hi - lo)) * 100));
    }

    function updateTagFilter(tags) {
      const sig = tags.join('|');
      if (sig === lastTagSig) return;
      lastTagSig = sig;
      const select = document.getElementById('tag_filter');
      const prev = select.value;
      select.innerHTML = '<option value="">全部</option>';
      tags.forEach((t) => {
        const op = document.createElement('option');
        op.value = t;
        op.textContent = t;
        select.appendChild(op);
      });
      select.value = prev;
    }

    function renderIncident(incident) {
      const root = document.getElementById('incident_box');
      const status = (incident && incident.status) || 'normal';
      root.className = 'incident';
      if (status === 'active') root.classList.add('danger');
      else if (status === 'recovering') root.classList.add('warn');

      const titleMap = {
        normal: '正常运行（未告警）',
        active: '异常已触发（处理中）',
        recovering: '异常恢复中',
        resolved: '异常已恢复'
      };
      const actions = (incident && incident.actions) || [];
      const items = actions.slice(-8).map((x) => `<li>${x}</li>`).join('');
      root.innerHTML = `<div class="title">${titleMap[status] || status}</div><ul>${items || '<li>等待异常事件...</li>'}</ul>`;
      document.getElementById('incident_state').textContent = titleMap[status] || status;
    }

    function applyPairHighlight() {
      const rows = document.querySelectorAll('.map-row[data-idx]');
      rows.forEach((el) => el.classList.remove('active', 'locked'));

      const idx = mapHoverIndex >= 0 ? mapHoverIndex : (mapLockIndex >= 0 ? mapLockIndex : mapDefaultIndex);
      if (idx < 0) return;
      const dv2_userDriven = mapHoverIndex >= 0 || mapLockIndex >= 0;

      const targets = document.querySelectorAll(`.map-row[data-idx="${idx}"]`);
      targets.forEach((el) => {
        el.classList.add('active');
        if (mapLockIndex === idx) el.classList.add('locked');
      });

      if (!dv2_userDriven) return;
      targets.forEach((el) => {
        const side = el.getAttribute('data-side');
        const oppositeId = side === 'bin' ? 'decoded_map_list' : 'binary_map_list';
        const oppContainer = document.getElementById(oppositeId);
        if (!oppContainer) return;

        const oppEl = oppContainer.querySelector(`.map-row[data-idx="${idx}"]`);
        if (!oppEl) return;

        const containerRect = oppContainer.getBoundingClientRect();
        const elRect = oppEl.getBoundingClientRect();
        const isVisible = elRect.top >= containerRect.top && elRect.bottom <= containerRect.bottom;
        if (!isVisible) {
          oppEl.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
        }
      });
    }

    function renderFramePairs(data) {
      const pairs = data.runtime.frame_pairs || [];
      const binaryRoot = document.getElementById('binary_map_list');
      const decodedRoot = document.getElementById('decoded_map_list');
      const pcase = data.runtime.powercut_case || {};
      const pStage = String(pcase.stage || 'idle');
      const recBefore = Number(pcase.records_before || 0);
      const recAfterCut = Number(pcase.records_after_powercut || 0);
      const recAfterRepair = Number(pcase.records_after_repair || 0);
      const recFinal = recAfterRepair > 0 ? recAfterRepair : recAfterCut;
      const recLoss = recBefore > 0 ? Math.max(0, recBefore - recFinal) : 0;
      let lossHint = '';
      if (pStage !== 'idle' && recBefore > 0) {
        if (recLoss > 0) {
          lossHint = `⚠ 断电后完整帧减少 ${recLoss} 条（${recBefore} -> ${recFinal}）`;
        } else {
          lossHint = `ℹ 断电后完整帧未减少（${recBefore} -> ${recFinal}），通常仅丢失正在写入的半帧`;
        }
      }

      if (!pairs.length) {
        binaryRoot.innerHTML = '<div class="map-row">暂无二进制帧</div>';
        decodedRoot.innerHTML = '<div class="map-row">暂无可读日志</div>';
        mapHoverIndex = -1;
        mapLockIndex = -1;
        mapDefaultIndex = -1;
        return;
      }

      const idxSet = new Set(pairs.map((x) => Number(x.index || -1)));
      if (!idxSet.has(mapLockIndex)) mapLockIndex = -1;
      const dv2_mode = (document.getElementById('view_mode') || {}).value || 'auto';
      if (dv2_mode === 'auto') {
        mapDefaultIndex = Number(pairs[pairs.length - 1].index || -1);
      } else if (!idxSet.has(mapDefaultIndex)) {
        mapDefaultIndex = Number(pairs[0].index || -1);
      }

      const binaryRows = [
        '<div class="map-row head">Binary Frame Segment (hover/click 联动)</div>',
        ...(lossHint ? [`<div class="map-row head anomaly-powercut">${esc(lossHint)}</div>`] : []),
        ...pairs.map((p) => {
          const idx = Number(p.index || 0);
          const kind = String(p.anomaly || '');
          const label = `#${idx} bytes[${p.start}:${p.end}) payload=${p.payload_len} csum=${p.checksum_type} vstr=${p.varstr}`;
          const cls = anomalyMapClass(kind);
          const badge = anomalyBadge(kind);
          return `<div class="map-row ${cls}" data-idx="${idx}" data-side="bin">${badge ? badge + ' ' : ''}${esc(label)}\n${esc(p.binary_hex || '')}</div>`;
        }),
      ];

      const decodedRows = [
        '<div class="map-row head">Decoded Log (single format)</div>',
        ...(lossHint ? [`<div class="map-row head anomaly-powercut">${esc(lossHint)}</div>`] : []),
        ...pairs.map((p) => {
          const idx = Number(p.index || 0);
          const kind = String(p.anomaly || '');
          const label = `#${idx} ${p.uptime || '--'} ${p.tag || '--'}`;
          const cls = anomalyMapClass(kind);
          const badge = anomalyBadge(kind);
          return `<div class="map-row ${cls}" data-idx="${idx}" data-side="dec">${badge ? badge + ' ' : ''}${esc(label)}\n${esc(p.decoded_line || '(no decoded row)')}</div>`;
        }),
      ];

      binaryRoot.innerHTML = binaryRows.join('');
      decodedRoot.innerHTML = decodedRows.join('');

      const pairRows = document.querySelectorAll('.map-row[data-idx]');
      pairRows.forEach((el) => {
        const idx = Number(el.getAttribute('data-idx') || '-1');
        el.addEventListener('mouseenter', () => {
          mapHoverIndex = idx;
          applyPairHighlight();
        });
        el.addEventListener('mouseleave', () => {
          mapHoverIndex = -1;
          applyPairHighlight();
        });
        el.addEventListener('click', () => {
          if (mapLockIndex === idx) mapLockIndex = -1;
          else mapLockIndex = idx;
          applyPairHighlight();
        });
      });
      applyPairHighlight();
    }

    function renderTimelineMarks(data) {
      const marks = data.runtime.timeline_marks || [];
      const root = document.getElementById('timeline_marks');
      if (!marks.length) {
        root.innerHTML = '<span class="mono">等待标注...</span>';
        return;
      }
      root.innerHTML = marks
        .slice(-14)
        .map((m) => {
          const kind = String(m.kind || '');
          const at = m.at || '--';
          const up = m.uptime || '--';
          const text = m.text || '--';
          return `<span class="mark">${anomalyBadge(kind)} <span class="mono">${esc(at)}|${esc(up)}</span> ${esc(text)}</span>`;
        })
        .join('');
    }

    function pickTimelineRows(data) {
      const mode = document.getElementById('view_mode').value;
      const tagFilter = document.getElementById('tag_filter').value || '';
      const full = (data.runtime.full_events || []).filter((e) => !tagFilter || e.tag === tagFilter);

      if (mode === 'step') {
        if (full.length === 0) {
          stepCursor = -1;
          return { rows: [], activeIndex: -1, label: 'step=0/0' };
        }
        if (stepCursor < 0 || stepCursor >= full.length) stepCursor = full.length - 1;
        const start = Math.max(0, stepCursor - 24);
        const rows = full.slice(start, stepCursor + 1);
        return {
          rows,
          activeIndex: rows.length - 1,
          label: `step=${stepCursor + 1}/${full.length}`,
        };
      }

      stepCursor = full.length - 1;
      const recent = full.slice(Math.max(0, full.length - 25));
      return {
        rows: recent,
        activeIndex: recent.length - 1,
        label: full.length > 0 ? `step=${full.length}/${full.length}` : 'step=0/0',
      };
    }

    function renderTimeline(data) {
      const picked = pickTimelineRows(data);
      document.getElementById('step_info').textContent = picked.label;
      if (picked.rows.length === 0) {
        document.getElementById('timeline').innerHTML = '<tr><td colspan="7">暂无事件</td></tr>';
        return;
      }
      const htmlRows = picked.rows
        .map((e, i) => {
          const rowCls = [];
          if (i === picked.activeIndex) rowCls.push('active-row');
          if (e.anomaly) rowCls.push(anomalyRowClass(e.anomaly));
          const dv2_anomaly = String(e.anomaly || '');
          if (dv2_anomaly === 'fault' || dv2_anomaly === 'powercut') rowCls.push('dv2-tl-fault-left');
          if (dv2_anomaly === 'recover' || dv2_anomaly === 'repair') rowCls.push('dv2-tl-recover-left');
          if (i === picked.rows.length - 1) rowCls.push('dv2-tl-new');
          const cls = rowCls.length ? ` class="${rowCls.join(' ')}"` : '';
          const badge = anomalyBadge(String(e.anomaly || ''));
          const dv2_isLatest = (i === picked.rows.length - 1) && document.getElementById('view_mode').value === 'auto';
          const dv2_liveDot = dv2_isLatest
            ? `<td style="padding:4px 2px"><span style="display:inline-block;width:6px;height:6px;border-radius:50%;
                background:var(--ok);animation:dv2-live-pulse 1.2s ease-in-out infinite"></span></td>`
            : '<td style="padding:4px 2px"></td>';
          return `<tr${cls}>${dv2_liveDot}<td>${e.index}</td><td>${e.uptime}</td><td>${e.tag}</td><td>${badge}</td><td>${e.summary}</td></tr>`;
        })
        .join('');
      document.getElementById('timeline').innerHTML = htmlRows;
      if (document.getElementById('view_mode').value === 'auto') {
        const box = document.getElementById('timeline_box');
        box.scrollTop = box.scrollHeight;
      }
    }

    function render(data) {
      const dv2_serverTime = document.getElementById('server_time');
      dv2_serverTime.textContent = `服务器时间: ${data.server_time || '--'}`;
      dv2_serverTime.style.color = '';
      document.getElementById('phase').textContent = data.runtime.phase || '--';
      document.getElementById('records').textContent = String(data.runtime.records_total || 0);

      const st = data.runtime.state || {};
      document.getElementById('boot').textContent = st.boot_stage || '--';
      document.getElementById('net').textContent = st.net_state || '--';
      document.getElementById('uptime').textContent = st.uptime_text || '--';
      document.getElementById('rssi').textContent = String(st.rssi_dbm ?? '--');
      document.getElementById('soc').textContent = `${Number(st.soc || 0).toFixed(1)} %`;
      document.getElementById('latency').textContent = String(st.latency_us ?? '--');
      document.getElementById('soc_bar').style.width = pct(st.soc || 0, 0, 100) + '%';
      const dv2_socBarWrap = document.getElementById('soc_bar').parentElement;
      const dv2_socVal = Number(st.soc || 0);
      dv2_socBarWrap.dataset.level = dv2_socVal < 20 ? 'danger' : dv2_socVal < 40 ? 'warn' : 'ok';
      document.getElementById('latency_bar').style.width = pct(st.latency_us || 0, 250, 1200) + '%';

      const opt = data.runtime.optimization || {};

      const c = data.runtime.counters || {};
      document.getElementById('counter').textContent =
        `boot=${c.boot_stage || 0} sensor=${c.sensor_sample || 0} control=${c.control_loop || 0} net=${c.net_state || 0} power=${c.power_state || 0} alert=${c.alert_event || 0} note=${c.note_event || 0}`;

      renderIncident(data.runtime.incident || {});
      const ctrl = data.runtime.control || {};
      const deviceStatus = ctrl.pause ? 'paused' : 'running';
      const freeze = ctrl.powercut_freeze ? ' powercut-hold' : '';
      document.getElementById('device_state').textContent =
        `device=${deviceStatus}${freeze} step_token=${ctrl.step_token ?? 0} interval=${ctrl.interval_ms ?? '--'}ms`;
      const intervalInput = document.getElementById('device_interval');
      const intervalNum = Number(ctrl.interval_ms);
      if (Number.isFinite(intervalNum) && document.activeElement !== intervalInput) {
        intervalInput.value = String(intervalNum);
      }

      // 根据设备状态动态更新按钮外观
      const dv2_pauseBtn = document.getElementById('device_pause');
      const dv2_resumeBtn = document.getElementById('device_resume');
      const dv2_isPaused = !!(ctrl.pause || ctrl.powercut_freeze);

      if (dv2_pauseBtn) {
        dv2_pauseBtn.disabled = dv2_isPaused;
        dv2_pauseBtn.style.opacity = dv2_isPaused ? '0.45' : '1';
      }
      if (dv2_resumeBtn) {
        dv2_resumeBtn.disabled = !dv2_isPaused;
        dv2_resumeBtn.style.opacity = !dv2_isPaused ? '0.45' : '1';
        dv2_resumeBtn.textContent = ctrl.powercut_freeze ? '▶ 继续(含自动恢复)' : '▶ 继续';
      }

      const dv2_toggleBtn = document.getElementById('toggle_refresh');
      if (dv2_toggleBtn) {
        dv2_toggleBtn.textContent = paused ? '▶ 恢复轮询' : '⏸ 暂停轮询';
      }

      document.getElementById('shared_head_meta').textContent =
        `size=${fmtBytes(data.files.shared_size || 0)} mtime=${data.files.shared_mtime || '--'}`;
      document.getElementById('log_tail_meta').textContent =
        `size=${fmtBytes(data.files.log_size || 0)} mtime=${data.files.log_mtime || '--'}`;
      document.getElementById('shared_hex').textContent = data.files.shared_hex_head || '--';
      window._dv2_lastFiles = data.files || {};

      document.getElementById('readable_meta').textContent = String((data.runtime.frame_pairs || []).length);
      renderFramePairs(data);

      const p = data.runtime.powercut_case || {};
      const pStage = p.stage || 'idle';
      const pActive = pStage !== 'idle';

      const pcRow = document.getElementById('powercut_status_row');
      if (pcRow) pcRow.style.display = pActive ? 'block' : 'none';

      const pcBox = document.getElementById('powercut_case_box');
      if (pcBox) pcBox.textContent = `断电恢复状态：${powercutStageLabel(pStage)} | drop=${p.drop_bytes || 0}B | repair_drop=${p.repair_drop_bytes || 0}B`;

      const pcMeta = document.getElementById('powercut_case_meta');
      if (pcMeta) {
        const recBefore = Number(p.records_before || 0);
        const recAfterCut = Number(p.records_after_powercut || 0);
        const recAfterRepair = Number(p.records_after_repair || 0);
        pcMeta.textContent = `updated=${p.updated_at || '--'} bytes:${p.before_bytes || 0}->${p.after_bytes || 0} rec:${recBefore}->${recAfterCut}${recAfterRepair ? `->${recAfterRepair}` : ''}`;
      }

      const setHex = (id, val) => { const e = document.getElementById(id); if (e) e.textContent = val || '--'; };
      setHex('hex_before', p.tail_hex_before || '--');
      setHex('hex_dropped', p.tail_hex_dropped || '--');
      setHex('hex_after', p.tail_hex_after || '--');

      const beforeBytes = Number(p.before_bytes || 0);
      const afterCutBytes = Number(p.after_bytes || 0);
      const afterRepairBytes = Number(p.repair_after_bytes || 0);
      const cutLoss = Math.max(Number(p.drop_bytes || 0), Math.max(0, beforeBytes - afterCutBytes));
      const repairLoss = Math.max(0, Number(p.repair_drop_bytes || 0));
      const finalBytes = afterRepairBytes > 0 ? afterRepairBytes : afterCutBytes;
      const totalLoss = Math.max(0, beforeBytes - finalBytes, cutLoss + repairLoss);

      const recBefore = Number(p.records_before || 0);
      const recAfterCut = Number(p.records_after_powercut || 0);
      const recAfterRepair = Number(p.records_after_repair || 0);
      const finalRec = recAfterRepair > 0 ? recAfterRepair : recAfterCut;
      const recLoss = recBefore > 0 ? Math.max(0, recBefore - finalRec) : 0;

      const pcCut = document.getElementById('pc_loss_cut');
      const pcRepair = document.getElementById('pc_loss_repair');
      const pcTotal = document.getElementById('pc_loss_total');
      const pcRecs = document.getElementById('pc_loss_records');
      if (pcCut) pcCut.textContent = `${cutLoss} B`;
      if (pcRepair) pcRepair.textContent = `${repairLoss} B`;
      if (pcTotal) pcTotal.textContent = `${totalLoss} B`;
      if (pcRecs) pcRecs.textContent = recBefore > 0 ? `${recBefore} -> ${finalRec} (丢${recLoss})` : '--';

      const pcTruthNote = document.getElementById('pc_truth_note');
      if (pcTruthNote) {
        const frameErrCut = String(p.frame_err_after_powercut || '').trim();
        const errTxt = frameErrCut ? `；截断后错误=${frameErrCut}` : '';
        if (totalLoss > 0 || recLoss > 0) {
          pcTruthNote.textContent =
            `恢复策略：只保留完整帧并裁剪损坏尾部。真实结果：永久丢失 ${totalLoss}B，完整帧减少 ${recLoss} 条${errTxt}`;
        } else {
          pcTruthNote.textContent =
            `恢复策略：只保留完整帧并裁剪损坏尾部。本次未减少已提交完整帧（通常打断的是正在写入的半帧）${errTxt}`;
        }
      }

      const pcResult = document.getElementById('powercut_result');
      if (pcResult) pcResult.textContent = `断电影响：${data.runtime.powercut_result || '--'}`;

      const fpeEl = document.getElementById('frame_parse_error');
      const fpeText = data.runtime.frame_parse_error || '';
      if (fpeEl) {
        fpeEl.textContent = fpeText ? `⚠ ${fpeText}` : '';
        fpeEl.style.color = fpeText ? 'var(--danger)' : 'var(--sub)';
      }

      document.getElementById('schema_src').textContent = data.schema.source_path || '--';
      const dv2DecodeMode = data.runtime.decode_state || 'unknown';
      document.getElementById('decode_state').textContent =
        `mode=${dv2DecodeMode} decode_ok=${data.runtime.decode_ok} rc=${data.runtime.decode_rc} at=${data.runtime.last_decode_at || '--'}`;
      dv2_renderSchema(data.schema || {});

      const tags = Array.from(new Set((data.runtime.full_events || []).map((e) => e.tag))).sort();
      updateTagFilter(tags);
      renderTimelineMarks(data);
      renderTimeline(data);
      dv2_renderPhaseBar(data.runtime.phase || '');
      dv2_renderPerf(data.runtime.optimization || {}, Number(data.runtime.records_total || 0));
    }

    function dv2_renderSchema(dv2_sch) {
      const dv2_s = (dv2_sch && dv2_sch.structured) || {};
      const dv2_el = (dv2_id) => document.getElementById(dv2_id);
      const dv2_hdr = dv2_s.header || {};
      const dv2_scalePct = (dv2_weights, dv2_minPct) => {
        const dv2_ws = (dv2_weights || []).map((x) => Math.max(0, Number(x) || 0));
        if (!dv2_ws.length) return [];
        const dv2_sum = dv2_ws.reduce((a, b) => a + b, 0);
        if (dv2_sum <= 0) return dv2_ws.map(() => (100 / dv2_ws.length));
        const dv2_raw = dv2_ws.map((w) => (w / dv2_sum) * 100);
        const dv2_small = dv2_raw.map((p) => p < dv2_minPct);
        const dv2_smallCount = dv2_small.filter(Boolean).length;
        if (dv2_smallCount === 0) return dv2_raw;
        const dv2_reserved = dv2_smallCount * dv2_minPct;
        if (dv2_reserved >= 100) return dv2_ws.map(() => (100 / dv2_ws.length));
        const dv2_restRaw = dv2_raw.reduce((a, p, i) => a + (dv2_small[i] ? 0 : p), 0);
        if (dv2_restRaw <= 0) return dv2_ws.map(() => (100 / dv2_ws.length));
        return dv2_raw.map((p, i) => {
          if (dv2_small[i]) return dv2_minPct;
          return (p / dv2_restRaw) * (100 - dv2_reserved);
        });
      };

      if (dv2_el('dv2_schema_hash')) dv2_el('dv2_schema_hash').textContent = dv2_hdr.schema_hash || '--';
      if (dv2_el('dv2_schema_gen')) {
        const dv2_gen = (dv2_hdr.generation ?? '--');
        dv2_el('dv2_schema_gen').textContent = String(dv2_gen);
      }

      const dv2_layoutBar = dv2_el('dv2_frame_layout_bar');
      const dv2_layoutLeg = dv2_el('dv2_frame_layout_legend');
      const dv2_payloadBar = dv2_el('dv2_payload_layout_bar');
      const dv2_payloadLeg = dv2_el('dv2_payload_layout_legend');
      const dv2_sharedBar = dv2_el('dv2_shared_layout_bar');
      const dv2_sharedLeg = dv2_el('dv2_shared_layout_legend');
      const dv2_tagBarsEl = dv2_el('dv2_tag_bars');
      const dv2_tagFields = dv2_el('dv2_tag_fields');
      const dv2_fieldsTitle = dv2_el('dv2_fields_title');
      const dv2_fieldsContent = dv2_el('dv2_fields_content');

      if (!dv2_s.ok) {
        if (dv2_layoutBar) dv2_layoutBar.innerHTML = '';
        if (dv2_layoutLeg) dv2_layoutLeg.innerHTML = '';
        if (dv2_payloadBar) dv2_payloadBar.innerHTML = '';
        if (dv2_payloadLeg) dv2_payloadLeg.innerHTML = '';
        if (dv2_sharedBar) dv2_sharedBar.innerHTML = '';
        if (dv2_sharedLeg) dv2_sharedLeg.innerHTML = '';
        if (dv2_tagBarsEl) dv2_tagBarsEl.innerHTML = '';
        if (dv2_tagFields) dv2_tagFields.style.display = 'none';
        if (dv2_fieldsTitle) dv2_fieldsTitle.textContent = '';
        if (dv2_fieldsContent) dv2_fieldsContent.innerHTML = '';
        window.dv2_tags_cache = [];
        return;
      }

      const dv2_tags = dv2_s.tags || [];
      const dv2_frameLayout = dv2_s.frame_layout || {};
      const dv2_frameSegs = dv2_frameLayout.segments || [];
      const dv2_FRAME_VAR_WEIGHT = 3;
      const dv2_frameWeights = dv2_frameSegs.map((x) => {
        const dv2_bytes = Number(x.bytes);
        return dv2_bytes > 0 ? dv2_bytes : dv2_FRAME_VAR_WEIGHT;
      });
      const dv2_framePct = dv2_scalePct(dv2_frameWeights, 16);
      const dv2_frameShort = { 'Frame Header': 'Hdr', Payload: 'Payload', Checksum: 'CRC' };

      if (dv2_layoutBar && dv2_frameSegs.length) {
        dv2_layoutBar.innerHTML = dv2_frameSegs
          .map((dv2_seg, dv2_i) => {
            const dv2_bytes = Number(dv2_seg.bytes);
            const dv2_wPct = Number(dv2_framePct[dv2_i] || 0);
            const dv2_labelLong = `${dv2_seg.name}${dv2_bytes > 0 ? ` ${dv2_bytes}B` : ' (var)'}`;
            const dv2_labelShort = dv2_frameShort[dv2_seg.name] || dv2_seg.name;
            const dv2_label = dv2_wPct >= 20 ? dv2_labelLong : dv2_labelShort;
            return `<div style="width:${dv2_wPct.toFixed(1)}%;background:${dv2_seg.color};display:flex;
                         align-items:center;justify-content:center;padding:0 4px;overflow:hidden;
                         font-size:${dv2_wPct >= 20 ? '11px' : '10px'};color:white;font-weight:600;white-space:nowrap;text-overflow:ellipsis;
                         text-shadow:0 1px 2px rgba(0,0,0,0.28)" title="${esc(dv2_seg.desc)}">
                      ${esc(dv2_label)}
                    </div>`;
          })
          .join('');
      }
      if (dv2_layoutLeg && dv2_frameSegs.length) {
        dv2_layoutLeg.innerHTML = dv2_frameSegs
          .map(
            (dv2_seg) =>
              `<span style="display:inline-flex;align-items:center;gap:5px;font-size:11px;color:var(--sub)">
                 <span style="width:10px;height:10px;border-radius:3px;background:${dv2_seg.color};
                              display:inline-block;flex-shrink:0"></span>
                 <strong style="color:var(--ink)">${esc(dv2_seg.name)}</strong> — ${esc(dv2_seg.desc)}
               </span>`
          )
          .join('');
      }

      const dv2_payloadPrefixSegs = dv2_frameLayout.payload_prefix || [];
      const dv2_payloadVarWeight = Math.max(
        4,
        Math.round(
          dv2_tags.reduce((acc, t) => acc + Number(t.payload_fields_fixed || 0), 0) / Math.max(1, dv2_tags.length)
        )
      );
      const dv2_payloadWeights = dv2_payloadPrefixSegs.map((x) => {
        const dv2_bytes = Number(x.bytes || 0);
        return dv2_bytes > 0 ? dv2_bytes : dv2_payloadVarWeight;
      });
      const dv2_payloadPct = dv2_scalePct(dv2_payloadWeights, 13);
      const dv2_payloadShort = { timestamp: 'ts', tag_id: 'tag', ele_count: 'n', fields: 'fields' };
      if (dv2_payloadBar && dv2_payloadPrefixSegs.length) {
        dv2_payloadBar.innerHTML = dv2_payloadPrefixSegs
          .map((dv2_seg, dv2_i) => {
            const dv2_bytes = Number(dv2_seg.bytes || 0);
            const dv2_wPct = Number(dv2_payloadPct[dv2_i] || 0);
            const dv2_label = dv2_bytes > 0 ? `${dv2_seg.name} ${dv2_bytes}B` : `${dv2_seg.name} (var)`;
            const dv2_shortLabel = dv2_payloadShort[dv2_seg.name] || dv2_seg.name;
            const dv2_showLabel = dv2_wPct >= 18 ? dv2_label : dv2_shortLabel;
            return `<div style="width:${dv2_wPct.toFixed(1)}%;background:${dv2_seg.color};display:flex;align-items:center;
                         justify-content:center;font-size:${dv2_wPct >= 18 ? '10px' : '9px'};font-weight:600;color:${dv2_bytes > 0 ? '#0f3f2f' : '#155a30'};
                         overflow:hidden;white-space:nowrap;text-overflow:ellipsis;padding:0 3px" title="${esc(dv2_seg.desc || '')}">
                      ${esc(dv2_showLabel)}
                    </div>`;
          })
          .join('');
      }
      if (dv2_payloadLeg && dv2_payloadPrefixSegs.length) {
        dv2_payloadLeg.innerHTML = dv2_payloadPrefixSegs
          .map(
            (dv2_seg) =>
              `<span>
                 <span style="display:inline-block;width:9px;height:9px;border-radius:2px;
                              background:${dv2_seg.color};margin-right:4px;vertical-align:middle"></span>
                 ${esc(dv2_seg.name)} — ${esc(dv2_seg.desc || '')}
               </span>`
          )
          .join('');
      }

      const dv2_fileSize = Number(dv2_hdr.file_size || 1);
      const dv2_sharedSegs = [
        { name: 'Header', range: dv2_hdr.header_range || '0:56', color: '#6366f1' },
        { name: 'Bitmap', range: dv2_hdr.bitmap_range || '', color: '#8b5cf6' },
        { name: 'EventTag', range: dv2_hdr.eventtag_range || '', color: '#0ea5e9' },
      ].filter((x) => x.range);
      const dv2_sharedWeights = dv2_sharedSegs.map((dv2_seg) => {
        const dv2_pair = String(dv2_seg.range || '').split(':').map((x) => Number(x));
        const dv2_a = Number.isFinite(dv2_pair[0]) ? dv2_pair[0] : 0;
        const dv2_b = Number.isFinite(dv2_pair[1]) ? dv2_pair[1] : dv2_fileSize;
        return Math.max(1, dv2_b - dv2_a);
      });
      const dv2_sharedPct = dv2_scalePct(dv2_sharedWeights, 12);
      const dv2_sharedShort = { Header: 'Hdr', Bitmap: 'Bmp', EventTag: 'Tags' };

      if (dv2_sharedBar && dv2_sharedSegs.length) {
        dv2_sharedBar.innerHTML = dv2_sharedSegs
          .map((dv2_seg, dv2_i) => {
            const dv2_pair = String(dv2_seg.range || '').split(':').map((x) => Number(x));
            const dv2_a = Number.isFinite(dv2_pair[0]) ? dv2_pair[0] : 0;
            const dv2_b = Number.isFinite(dv2_pair[1]) ? dv2_pair[1] : dv2_fileSize;
            const dv2_wPct = Number(dv2_sharedPct[dv2_i] || 0);
            const dv2_label = dv2_wPct >= 16 ? dv2_seg.name : (dv2_sharedShort[dv2_seg.name] || dv2_seg.name);
            return `<div style="width:${dv2_wPct}%;background:${dv2_seg.color};display:flex;align-items:center;
                         justify-content:center;font-size:${dv2_wPct >= 16 ? '10px' : '9px'};color:white;font-weight:600;
                         overflow:hidden;padding:0 3px;white-space:nowrap;text-overflow:ellipsis" title="bytes[${esc(dv2_seg.range)})">
                      ${esc(dv2_label)}
                    </div>`;
          })
          .join('');
      }
      if (dv2_sharedLeg && dv2_sharedSegs.length) {
        dv2_sharedLeg.innerHTML = dv2_sharedSegs
          .map(
            (dv2_seg) =>
              `<span>
                 <span style="display:inline-block;width:9px;height:9px;border-radius:2px;
                              background:${dv2_seg.color};margin-right:4px;vertical-align:middle"></span>
                 ${esc(dv2_seg.name)} bytes[${esc(dv2_seg.range)})
               </span>`
          )
          .join('');
      }

      if (dv2_tagBarsEl && dv2_tags.length) {
        const dv2_maxSize = Math.max(
          ...dv2_tags.map((dv2_t) => Number(dv2_t.encoded_size_varmax || dv2_t.encoded_size || 1))
        );
        const dv2_fieldColors = ['#0071e3', '#8b5cf6', '#0ea5e9', '#16a34a', '#f59e0b', '#ec4899', '#06b6d4', '#84cc16'];
        const dv2_overhead = Number(dv2_frameLayout.frame_fixed_overhead || 19);
        dv2_tagBarsEl.innerHTML = dv2_tags
          .map((dv2_t) => {
            const dv2_encFixed = Number(dv2_t.encoded_size || 0);
            const dv2_encVarMax = Number(dv2_t.encoded_size_varmax || dv2_encFixed || 1);
            const dv2_barSize = Math.max(dv2_encFixed, dv2_encVarMax, 1);
            const dv2_pct = ((dv2_barSize / dv2_maxSize) * 100).toFixed(1);
            const dv2_fields = dv2_t.fields || [];

            const dv2_stackParts = [];
            const dv2_overheadPct = Math.max(0.5, (dv2_overhead / dv2_barSize) * 100).toFixed(1);
            dv2_stackParts.push(
              `<div style="width:${dv2_overheadPct}%;height:100%;background:rgba(107,114,128,0.35);flex-shrink:0"
                    title="固定开销 ${dv2_overhead}B (frame_header+payload_prefix+checksum)"></div>`
            );
            dv2_fields.forEach((dv2_f, dv2_fi) => {
              const dv2_fixed = Number(dv2_f.wire_bytes_fixed || dv2_f.len || 0);
              const dv2_varmax = Number(dv2_f.wire_bytes_varmax || dv2_fixed);
              const dv2_extra = Math.max(0, dv2_varmax - dv2_fixed);
              const dv2_fc = dv2_fieldColors[dv2_fi % dv2_fieldColors.length];
              const dv2_fw = Math.max(0, (dv2_fixed / dv2_barSize) * 100).toFixed(1);
              if (Number(dv2_fw) > 0) {
                dv2_stackParts.push(
                  `<div style="width:${dv2_fw}%;height:100%;background:${dv2_fc};opacity:0.78;flex-shrink:0"
                        title="${esc(dv2_f.name)} fixed=${dv2_fixed}B"></div>`
                );
              }
              if (dv2_extra > 0) {
                const dv2_exw = Math.max(0, (dv2_extra / dv2_barSize) * 100).toFixed(1);
                if (Number(dv2_exw) > 0) {
                  dv2_stackParts.push(
                    `<div style="width:${dv2_exw}%;height:100%;background:${dv2_fc};opacity:0.36;flex-shrink:0"
                          title="${esc(dv2_f.name)} varstr extra(max)=${dv2_extra}B"></div>`
                  );
                }
              }
            });

            const dv2_sizeLabel = dv2_encVarMax > dv2_encFixed
              ? `~${dv2_encFixed}-${dv2_encVarMax}B`
              : `~${dv2_encFixed}B`;

            return `<div style="display:flex;align-items:center;gap:8px;cursor:pointer;
                                padding:2px 4px;border-radius:7px;transition:background .12s"
                         onmouseenter="this.style.background='rgba(0,113,227,0.05)'"
                         onmouseleave="this.style.background=''"
                         onclick="dv2_showTagFields(${Number(dv2_t.id)})">
                      <div style="width:108px;font-family:'SF Mono',Menlo,monospace;font-size:11px;
                                  color:var(--sub);text-align:right;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(dv2_t.name)}</div>
                      <div style="flex:1;height:16px;border-radius:999px;background:#e5e9f0;overflow:hidden;position:relative">
                        <div style="width:${dv2_pct}%;height:100%;border-radius:999px;overflow:hidden;
                                    display:flex;transition:width 0.45s cubic-bezier(0.4,0,0.2,1)">
                          ${dv2_stackParts.join('')}
                        </div>
                      </div>
                      <div style="width:104px;font-size:11px;font-weight:600;color:var(--ink);flex-shrink:0;text-align:right">${dv2_sizeLabel}</div>
                      <div style="font-size:10px;color:var(--sub);flex-shrink:0">▸</div>
                    </div>`;
          })
          .join('');
        window.dv2_tags_cache = dv2_tags;
      } else {
        if (dv2_tagBarsEl) dv2_tagBarsEl.innerHTML = '';
        window.dv2_tags_cache = [];
      }

    }

    function dv2_showTagFields(dv2_tagId) {
      const dv2_tags = window.dv2_tags_cache || [];
      const dv2_t = dv2_tags.find((x) => Number(x.id) === Number(dv2_tagId));
      const dv2_box = document.getElementById('dv2_tag_fields');
      const dv2_titleEl = document.getElementById('dv2_fields_title');
      const dv2_content = document.getElementById('dv2_fields_content');
      if (!dv2_t || !dv2_box) return;
      if (dv2_box.style.display !== 'none' && dv2_titleEl && dv2_titleEl.dataset.openId === String(dv2_tagId)) {
        dv2_box.style.display = 'none';
        return;
      }

      const dv2_fixed = Number(dv2_t.encoded_size || 0);
      const dv2_varmax = Number(dv2_t.encoded_size_varmax || dv2_fixed);
      if (dv2_titleEl) {
        dv2_titleEl.textContent =
          `${dv2_t.name}  (id=${dv2_t.id}, ${dv2_t.field_count} 字段, ` +
          (dv2_varmax > dv2_fixed ? `估算 ~${dv2_fixed}-${dv2_varmax}B/frame` : `估算 ~${dv2_fixed}B/frame`) +
          ')';
        dv2_titleEl.dataset.openId = String(dv2_tagId);
      }

      const dv2_fields = dv2_t.fields || [];
      if (dv2_content) {
        const dv2_prefix = Number(dv2_t.payload_prefix_bytes || 11);
        let dv2_offset = dv2_prefix;
        dv2_content.innerHTML = dv2_fields.length
          ? `<div style="margin-bottom:6px;font-size:11px;color:var(--sub)">
               <strong>payload 前缀</strong>：timestamp(8B) + tag_id(2B) + ele_count(1B) = ${dv2_prefix}B
             </div>
             <div style="display:grid;grid-template-columns:130px 56px 58px 66px 84px 1fr;gap:4px 10px;font-size:11px">
               <div style="font-weight:600;color:var(--sub)">字段名</div>
               <div style="font-weight:600;color:var(--sub)">类型</div>
               <div style="font-weight:600;color:var(--sub)">schema</div>
               <div style="font-weight:600;color:var(--sub)">fixed</div>
               <div style="font-weight:600;color:var(--sub)">payload偏移</div>
               <div style="font-weight:600;color:var(--sub)">varstr 规则</div>
               ${dv2_fields
                 .map((dv2_f, dv2_i) => {
                   const dv2_colors = ['#0071e3', '#8b5cf6', '#0ea5e9', '#16a34a', '#f59e0b', '#ec4899', '#06b6d4', '#84cc16'];
                   const dv2_dotColor = dv2_colors[dv2_i % dv2_colors.length];
                   const dv2_schemaLen = Number(dv2_f.len) || 0;
                   const dv2_fixedB = Number(dv2_f.wire_bytes_fixed || dv2_schemaLen);
                   const dv2_varB = Number(dv2_f.wire_bytes_varmax || dv2_fixedB);
                   const dv2_fieldOffset = dv2_offset;
                   const dv2_rule = dv2_varB > dv2_fixedB ? `varstr max=${dv2_varB}B` : 'fixed';
                   const dv2_row = `<div style="display:contents">
                     <div style="color:var(--ink);display:flex;align-items:center;gap:5px">
                       <span style="width:8px;height:8px;border-radius:2px;background:${dv2_dotColor};
                                    display:inline-block;flex-shrink:0"></span>${esc(dv2_f.name)}
                     </div>
                     <div style="color:var(--accent)">${esc(dv2_f.type)}</div>
                     <div style="color:var(--sub)">${dv2_schemaLen}B</div>
                     <div style="color:var(--sub)">${dv2_fixedB}B</div>
                     <div style="color:var(--sub);font-family:'SF Mono',Menlo,monospace">+${dv2_fieldOffset}B</div>
                     <div style="color:var(--sub)">${esc(dv2_rule)}</div>
                   </div>`;
                   dv2_offset += dv2_fixedB;
                   return dv2_row;
                 })
                 .join('')}
             </div>`
          : '<div style="color:var(--sub)">（无字段信息）</div>';
      }
      dv2_box.style.display = 'block';
    }

    function dv2_renderPhaseBar(dv2_phaseStr) {
      const dv2_bar = document.getElementById('dv2_phase_bar');
      if (!dv2_bar) return;
      const dv2_match = String(dv2_phaseStr || '').match(/Phase\\s*(\\d)/i);
      const dv2_current = dv2_match ? Number.parseInt(dv2_match[1], 10) - 1 : 0;

      if (dv2_bar.dataset.built !== '1') {
        dv2_bar.dataset.built = '1';
        dv2_bar.innerHTML = dv2_PHASES.map((dv2_name, dv2_i) => {
          const dv2_node = `<div class="dv2-phase-node">
            <div class="dv2-phase-dot" id="dv2_pd_${dv2_i}"></div>
            <div class="dv2-phase-label" id="dv2_pl_${dv2_i}">${dv2_name}</div>
          </div>`;
          const dv2_line = dv2_i < dv2_PHASES.length - 1
            ? `<div class="dv2-phase-line" id="dv2_pln_${dv2_i}"></div>` : '';
          return dv2_node + dv2_line;
        }).join('');
      }

      dv2_PHASES.forEach((_, dv2_i) => {
        const dv2_dot = document.getElementById(`dv2_pd_${dv2_i}`);
        const dv2_lbl = document.getElementById(`dv2_pl_${dv2_i}`);
        const dv2_line = document.getElementById(`dv2_pln_${dv2_i}`);
        if (dv2_dot) {
          dv2_dot.className =
            'dv2-phase-dot' + (dv2_i < dv2_current ? ' done' : dv2_i === dv2_current ? ' active' : '');
        }
        if (dv2_lbl) {
          dv2_lbl.className =
            'dv2-phase-label' + (dv2_i < dv2_current ? ' done' : dv2_i === dv2_current ? ' active' : '');
        }
        if (dv2_line) dv2_line.className = 'dv2-phase-line' + (dv2_i < dv2_current ? ' done' : '');
      });
    }

    function dv2_renderPerf(dv2_opt, dv2_totalRecords) {
      const dv2_binBytes =
        Number(dv2_opt.binary_bytes || 0) || Number((window._dv2_lastFiles || {}).log_size || 0);
      const dv2_txtBytes = Number(dv2_opt.text_est_bytes || 0);
      const dv2_ratio = Number(dv2_opt.space_ratio_text_div_binary || 0);
      const dv2_decMs = Number(dv2_opt.last_decode_ms || 0);
      const dv2_rps = Number(dv2_opt.records_per_sec || 0);

      const dv2_el = (dv2_id) => document.getElementById(dv2_id);
      if (dv2_el('dv2_perf_bin')) dv2_el('dv2_perf_bin').textContent = fmtBytes(dv2_binBytes);
      if (dv2_el('dv2_perf_txt')) dv2_el('dv2_perf_txt').textContent = fmtBytes(dv2_txtBytes);
      if (dv2_el('dv2_perf_ratio')) dv2_el('dv2_perf_ratio').textContent = dv2_ratio.toFixed(2) + '×';
      if (dv2_el('dv2_perf_dec')) dv2_el('dv2_perf_dec').textContent = dv2_decMs.toFixed(2) + ' ms';

      const dv2_perBin = dv2_totalRecords > 0 ? dv2_binBytes / dv2_totalRecords : 0;
      const dv2_perTxt = dv2_totalRecords > 0 ? dv2_txtBytes / dv2_totalRecords : 0;
      const dv2_maxBar = Math.max(dv2_perTxt, dv2_perBin, 1);
      const dv2_cmpEl = dv2_el('dv2_perf_compare');
      if (dv2_cmpEl) {
        dv2_cmpEl.innerHTML = [
          { label: 'Binary', value: dv2_perBin, color: 'var(--accent-gradient)' },
          {
            label: 'Text',
            value: dv2_perTxt,
            color: 'linear-gradient(135deg,#9ca3af 0%,#6b7280 100%)',
          },
        ]
          .map((dv2_row) => {
            const dv2_pct = Math.min(100, (dv2_row.value / dv2_maxBar) * 100).toFixed(1);
            return `<div style="display:flex;align-items:center;gap:6px">
                      <div style="width:44px;font-size:10px;color:var(--sub);
                                  text-align:right;flex-shrink:0">${dv2_row.label}</div>
                      <div style="flex:1;height:8px;border-radius:999px;background:#e5e9f0;overflow:hidden">
                        <div style="height:100%;width:${dv2_pct}%;background:${dv2_row.color};border-radius:999px;
                                    transition:width 0.4s cubic-bezier(0.4,0,0.2,1)"></div>
                      </div>
                      <div style="width:46px;font-size:10px;font-weight:600;
                                  color:var(--ink);flex-shrink:0">${dv2_row.value.toFixed(1)}B</div>
                    </div>`;
          })
          .join('');
      }

      const dv2_rpsLabel = document.getElementById('dv2_perf_rps_label');
      if (dv2_rpsLabel) dv2_rpsLabel.textContent = dv2_rps.toFixed(1) + ' r/s';

      dv2_thrHistory.push(dv2_rps);
      if (dv2_thrHistory.length > 60) dv2_thrHistory.shift();
      const dv2_canvas = dv2_el('dv2_perf_canvas');
      if (!dv2_canvas || dv2_thrHistory.length < 2) return;
      dv2_canvas.width = dv2_canvas.offsetWidth || 600;
      const dv2_ctx = dv2_canvas.getContext('2d');
      if (!dv2_ctx) return;
      dv2_ctx.clearRect(0, 0, dv2_canvas.width, dv2_canvas.height);
      const dv2_maxY = Math.max(...dv2_thrHistory, 1);
      const dv2_w = dv2_canvas.width;
      const dv2_h = dv2_canvas.height;
      const dv2_pts = dv2_thrHistory.map((dv2_v, dv2_i) => ({
        x: (dv2_i / 59) * dv2_w,
        y: dv2_h - (dv2_v / dv2_maxY) * (dv2_h - 8) - 4,
      }));
      dv2_ctx.beginPath();
      dv2_ctx.moveTo(dv2_pts[0].x, dv2_pts[0].y);
      for (let dv2_i = 1; dv2_i < dv2_pts.length; dv2_i += 1) {
        dv2_ctx.lineTo(dv2_pts[dv2_i].x, dv2_pts[dv2_i].y);
      }
      dv2_ctx.strokeStyle = '#0071e3';
      dv2_ctx.lineWidth = 1.5;
      dv2_ctx.lineJoin = 'round';
      dv2_ctx.stroke();
    }

    async function callInject(action, buttonId) {
      await withPending(buttonId, async () => {
        try {
          const r = await fetch('/api/inject', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action }),
          });
          const data = await r.json();
          if (!r.ok || !data.ok) {
            document.getElementById('inject_result').textContent = `注入结果：失败 (${data.error || 'unknown'})`;
            setActionFeedback(`注入失败: ${action}`, false);
            flashButton(buttonId, false);
            return;
          }
          document.getElementById('inject_result').textContent = `注入结果：成功 (${data.message || action})`;
          setActionFeedback(`注入已执行: ${action}`, true);
          flashButton(buttonId, true);
          await tick();
        } catch (e) {
          document.getElementById('inject_result').textContent = `注入结果：失败 (${String(e)})`;
          setActionFeedback(`注入失败: ${action}`, false);
          flashButton(buttonId, false);
        }
      });
    }

    async function tick() {
      if (paused) return;
      try {
        const r = await fetch('/api/snapshot', { cache: 'no-store' });
        if (!r.ok) {
          dv2_markBackend(false, `http ${r.status}`);
          return;
        }
        const data = await r.json();
        dv2_markBackend(true);
        render(data);
      } catch (dv2_err) {
        dv2_markBackend(false, String(dv2_err));
      }
    }

    async function callControl(action, intervalMs = null, buttonId = '') {
      await withPending(buttonId, async () => {
        try {
          const payload = { action };
          if (intervalMs !== null) payload.interval_ms = Number(intervalMs);
          const r = await fetch('/api/control', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
          });
          const data = await r.json();
          if (!r.ok || !data.ok) {
            document.getElementById('inject_result').textContent =
              `控制失败：${data.error || 'unknown'}`;
            setActionFeedback(`控制失败: ${action}`, false);
            if (buttonId) flashButton(buttonId, false);
            return;
          }
          document.getElementById('inject_result').textContent =
            `控制成功：${data.message || action}`;
          setActionFeedback(`控制已执行: ${data.message || action}`, true);
          if (buttonId) flashButton(buttonId, true);
          await tick();
        } catch (e) {
          document.getElementById('inject_result').textContent =
            `控制失败：${String(e)}`;
          setActionFeedback(`控制失败: ${action}`, false);
          if (buttonId) flashButton(buttonId, false);
        }
      });
    }

    async function callPowercut(buttonId) {
      await withPending(buttonId, async () => {
        try {
          const input = document.getElementById('powercut_drop_bytes');
          let dropBytes = Number(input ? input.value : 7);
          if (!Number.isFinite(dropBytes) || dropBytes <= 0) dropBytes = 7;
          const r = await fetch('/api/powercut', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ drop_bytes: Math.floor(dropBytes) }),
          });
          const data = await r.json();
          if (!r.ok || !data.ok) {
            document.getElementById('powercut_result').textContent = `断电影响：失败 (${data.error || 'unknown'})`;
            setActionFeedback('断电模拟失败', false);
            flashButton(buttonId, false);
            return;
          }
          document.getElementById('powercut_result').textContent = `断电影响：${data.message || '--'}`;
          setActionFeedback('断电模拟已执行；点击“继续设备”会自动恢复后再继续', true);
          flashButton(buttonId, true);
          await tick();
        } catch (e) {
          document.getElementById('powercut_result').textContent = `断电影响：失败 (${String(e)})`;
          setActionFeedback('断电模拟失败', false);
          flashButton(buttonId, false);
        }
      });
    }

    async function callPowercutRepair(buttonId) {
      await withPending(buttonId, async () => {
        try {
          const r = await fetch('/api/powercut_repair', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({}),
          });
          const data = await r.json();
          if (!r.ok || !data.ok) {
            document.getElementById('powercut_result').textContent = `断电恢复：失败 (${data.error || 'unknown'})`;
            setActionFeedback('断电恢复失败', false);
            flashButton(buttonId, false);
            return;
          }
          document.getElementById('powercut_result').textContent = `断电恢复：${data.message || '--'}`;
          setActionFeedback('断电恢复已执行，请观察时间线和日志联动高亮', true);
          flashButton(buttonId, true);
          await tick();
        } catch (e) {
          document.getElementById('powercut_result').textContent = `断电恢复：失败 (${String(e)})`;
          setActionFeedback('断电恢复失败', false);
          flashButton(buttonId, false);
        }
      });
    }

    function restartTimer() {
      if (timer) clearInterval(timer);
      const ms = Number(document.getElementById('refresh_ms').value || 800);
      timer = setInterval(tick, ms);
    }

    document.getElementById('toggle_refresh').addEventListener('click', () => {
      paused = !paused;
      document.getElementById('toggle_refresh').textContent = paused ? '▶ 恢复轮询' : '⏸ 暂停轮询';
      if (!paused) tick();
    });

    document.getElementById('refresh_ms').addEventListener('change', restartTimer);
    document.getElementById('tag_filter').addEventListener('change', () => tick());

    document.getElementById('view_mode').addEventListener('change', () => {
      if (document.getElementById('view_mode').value === 'auto') {
        stepCursor = -1;
        paused = false;
        document.getElementById('toggle_refresh').textContent = '⏸ 暂停轮询';
        setActionFeedback('已切回自动跟随模式', true);
      } else {
        setActionFeedback('已切到逐步查看模式（仅影响前端视图）', true);
      }
      tick();
    });

    document.getElementById('step_prev').addEventListener('click', () => {
      document.getElementById('view_mode').value = 'step';
      if (stepCursor > 0) stepCursor -= 1;
      setActionFeedback('时间线逐步查看：上一步', true);
      tick();
    });

    document.getElementById('step_next').addEventListener('click', () => {
      document.getElementById('view_mode').value = 'step';
      stepCursor += 1;
      setActionFeedback('时间线逐步查看：下一步', true);
      tick();
    });

    document.getElementById('inject_fault').addEventListener('click', () => {
      const dv2_btn = document.getElementById('inject_fault');
      const dv2_fetch = fetch('/api/inject', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'fault' }),
      });
      dv2_btnAction(dv2_btn, dv2_fetch, (dv2_ok, dv2_data) => {
        document.getElementById('inject_result').textContent =
          dv2_ok ? `注入结果：成功 (${dv2_data.message || 'fault'})` : `注入结果：失败 (${dv2_data.error || 'unknown'})`;
        tick();
      });
    });
    document.getElementById('inject_diag').addEventListener('click', () => {
      const dv2_btn = document.getElementById('inject_diag');
      const dv2_fetch = fetch('/api/inject', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'diag' }),
      });
      dv2_btnAction(dv2_btn, dv2_fetch, (dv2_ok, dv2_data) => {
        document.getElementById('inject_result').textContent =
          dv2_ok ? `注入结果：成功 (${dv2_data.message || 'diag'})` : `注入结果：失败 (${dv2_data.error || 'unknown'})`;
        tick();
      });
    });
    document.getElementById('inject_recover').addEventListener('click', () => {
      const dv2_btn = document.getElementById('inject_recover');
      const dv2_fetch = fetch('/api/inject', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'recover' }),
      });
      dv2_btnAction(dv2_btn, dv2_fetch, (dv2_ok, dv2_data) => {
        document.getElementById('inject_result').textContent =
          dv2_ok ? `注入结果：成功 (${dv2_data.message || 'recover'})` : `注入结果：失败 (${dv2_data.error || 'unknown'})`;
        tick();
      });
    });
    document.getElementById('device_pause').addEventListener('click', () => {
      const dv2_btn = document.getElementById('device_pause');
      const dv2_fetch = fetch('/api/control', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'pause' }),
      });
      dv2_btnAction(dv2_btn, dv2_fetch, (dv2_ok, dv2_data) => {
        document.getElementById('inject_result').textContent =
          dv2_ok ? `控制成功：${dv2_data.message || 'pause'}` : `控制失败：${dv2_data.error || 'unknown'}`;
        tick();
      });
    });
    document.getElementById('device_resume').addEventListener('click', () => {
      const dv2_btn = document.getElementById('device_resume');
      const dv2_fetch = fetch('/api/control', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'resume' }),
      });
      dv2_btnAction(dv2_btn, dv2_fetch, (dv2_ok, dv2_data) => {
        document.getElementById('inject_result').textContent =
          dv2_ok ? `控制成功：${dv2_data.message || 'resume'}` : `控制失败：${dv2_data.error || 'unknown'}`;
        tick();
      });
    });
    document.getElementById('device_step').addEventListener('click', () => {
      const dv2_btn = document.getElementById('device_step');
      const dv2_fetch = fetch('/api/control', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'step' }),
      });
      dv2_btnAction(dv2_btn, dv2_fetch, (dv2_ok, dv2_data) => {
        document.getElementById('inject_result').textContent =
          dv2_ok ? `控制成功：${dv2_data.message || 'step'}` : `控制失败：${dv2_data.error || 'unknown'}`;
        tick();
      });
    });
    document.getElementById('device_interval_apply').addEventListener('click', () => {
      const dv2_btn = document.getElementById('device_interval_apply');
      const dv2_msRaw = document.getElementById('device_interval').value;
      const dv2_ms = Number(dv2_msRaw);
      const dv2_fetch = fetch('/api/control', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: 'set_interval', interval_ms: dv2_ms }),
      });
      dv2_btnAction(dv2_btn, dv2_fetch, (dv2_ok, dv2_data) => {
        document.getElementById('inject_result').textContent =
          dv2_ok ? `控制成功：${dv2_data.message || 'set_interval'}` : `控制失败：${dv2_data.error || 'unknown'}`;
        tick();
      });
    });
    document.getElementById('device_interval').addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter') {
        document.getElementById('device_interval_apply').click();
      }
    });
    document.getElementById('simulate_powercut').addEventListener('click', () => {
      const dv2_btn = document.getElementById('simulate_powercut');
      const dv2_input = document.getElementById('powercut_drop_bytes');
      let dv2_dropBytes = Number(dv2_input ? dv2_input.value : 7);
      if (!Number.isFinite(dv2_dropBytes) || dv2_dropBytes <= 0) dv2_dropBytes = 7;
      const dv2_fetch = fetch('/api/powercut', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ drop_bytes: Math.floor(dv2_dropBytes) }),
      });
      dv2_btnAction(dv2_btn, dv2_fetch, (dv2_ok, dv2_data) => {
        document.getElementById('powercut_result').textContent =
          dv2_ok ? `断电影响：${dv2_data.message || '--'}` : `断电影响：失败 (${dv2_data.error || 'unknown'})`;
        tick();
      });
    });
    document.getElementById('repair_powercut').addEventListener('click', () => {
      const dv2_btn = document.getElementById('repair_powercut');
      const dv2_fetch = fetch('/api/powercut_repair', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });
      dv2_btnAction(dv2_btn, dv2_fetch, (dv2_ok, dv2_data) => {
        document.getElementById('powercut_result').textContent =
          dv2_ok ? `断电恢复：${dv2_data.message || '--'}` : `断电恢复：失败 (${dv2_data.error || 'unknown'})`;
        tick();
      });
    });

    document.addEventListener('keydown', function(dv2_ev) {
      const dv2_active = document.activeElement;
      if (dv2_ev.repeat) return;
      if (
        dv2_active &&
        (['INPUT', 'SELECT', 'TEXTAREA', 'BUTTON'].includes(dv2_active.tagName) || dv2_active.isContentEditable)
      ) return;

      const dv2_key = String(dv2_ev.key || '');
      const dv2_keyLower = dv2_key.toLowerCase();
      const dv2_isSpace = dv2_key === ' ' || dv2_keyLower === 'spacebar' || dv2_ev.code === 'Space';

      // 设备直接控制快捷键
      if (dv2_isSpace) {
        dv2_ev.preventDefault();
        const dv2_isPausedNow = document.getElementById('device_resume') &&
          !document.getElementById('device_resume').disabled;
        const dv2_action = dv2_isPausedNow ? 'resume' : 'pause';
        const dv2_targetBtnId = dv2_isPausedNow ? 'device_resume' : 'device_pause';
        const dv2_btn = document.getElementById(dv2_targetBtnId);
        dv2_btnAction(
          dv2_btn,
          fetch('/api/control', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action: dv2_action }),
          }),
          () => tick()
        );
        return;
      }

      const dv2_map = {
        ArrowRight: () => document.getElementById('device_step').click(),
        s: () => document.getElementById('device_step').click(),
        ArrowLeft: () => {
          document.getElementById('view_mode').value = 'step';
          if (stepCursor > 0) stepCursor -= 1;
          tick();
        },
        b: () => {
          document.getElementById('view_mode').value = 'step';
          if (stepCursor > 0) stepCursor -= 1;
          tick();
        },
        '1': () => document.getElementById('inject_fault').click(),
        '2': () => document.getElementById('inject_diag').click(),
        '3': () => document.getElementById('inject_recover').click(),
        '4': () => document.getElementById('simulate_powercut').click(),
        r: () => document.getElementById('repair_powercut').click(),
        Escape: () => {
          const dv2_tf = document.getElementById('dv2_tag_fields');
          if (dv2_tf) dv2_tf.style.display = 'none';
        },
      };

      const dv2_fn = dv2_map[dv2_key] || dv2_map[dv2_keyLower];
      if (dv2_fn) {
        dv2_ev.preventDefault();
        dv2_fn();
      }
    });

    restartTimer();
    tick();
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run live dashboard server for optbinlog stream showcase")
    p.add_argument("--read-bin", required=True)
    p.add_argument("--shared", required=True)
    p.add_argument("--log", required=True)
    p.add_argument("--schema-source", required=True, help="schema source text file, e.g. embedded_tags.txt")
    p.add_argument("--control-file", default="", help="optional control file for pause/resume/step")
    p.add_argument("--inject-bin", default="", help="optional injector binary for manual fault injection")
    p.add_argument("--fault-at-cycle", type=int, default=-1)
    p.add_argument("--recover-at-cycle", type=int, default=-1)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--poll-interval", type=float, default=0.35)
    p.add_argument("--max-recent", type=int, default=240)
    p.add_argument("--state-out", default="", help="optional json snapshot output path")
    return p.parse_args()


def fmt_uptime(ms: int) -> str:
    sec = ms // 1000
    rem = ms % 1000
    m = sec // 60
    s = sec % 60
    return f"{m:02d}:{s:02d}.{rem:03d}"


def fields_map(event: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for field in event.get("fields", []):
        out[field.get("name", "unknown")] = field.get("value")
    return out


def phase_from_state(state: Dict[str, Any]) -> str:
    if state["boot_stage"] != "APP_READY":
        return "Phase 1/5 Boot"
    if state["net_state"] != "ONLINE":
        return "Phase 2/5 Network"
    if state["alert_active"]:
        return "Phase 4/5 Fault"
    if state["seen_alert"] and not state["alert_active"]:
        return "Phase 5/5 Recovery"
    return "Phase 3/5 Runtime"


def event_summary(tag: str, f: Dict[str, Any]) -> str:
    if tag == "boot_stage":
        stage = BOOT_STAGE_MAP.get(int(f.get("stage", 0)), "UNKNOWN")
        return f"BOOT {stage} code={int(f.get('code', 0))}"
    if tag == "sensor_sample":
        return f"SENSOR id={int(f.get('sensor_id', 0))} value={int(f.get('value_x100', 0))/100.0:.2f}"
    if tag == "control_loop":
        return f"CTRL latency={int(f.get('latency_us', 0))}us pwm={int(f.get('pwm', 0))}"
    if tag == "net_state":
        ns = NET_STATE_MAP.get(int(f.get("state", 0)), "UNKNOWN")
        return f"NET {ns} rssi={int(f.get('rssi_dbm', 0))} retry={int(f.get('retry', 0))}"
    if tag == "power_state":
        return f"PWR soc={int(f.get('soc', 0))}% current={int(f.get('current_ma', 0))}mA"
    if tag == "alert_event":
        lv = ALERT_LEVEL_MAP.get(int(f.get("level", 0)), "UNKNOWN")
        return f"ALERT {lv} fault={int(f.get('fault', 0))} value_x10={int(f.get('value_x10', 0))}"
    if tag == "note_event":
        return f"NOTE {str(f.get('msg', ''))}"
    return tag


def classify_event_anomaly(tag: str, f: Dict[str, Any]) -> str:
    if tag == "alert_event":
        level = int(f.get("level", 0))
        if level >= 2:
            return "fault"
        if level == 1:
            return "recover"
    if tag == "note_event":
        msg = str(f.get("msg", ""))
        if "TEMP_WARN" in msg:
            return "fault"
        if "DIAG_RETRY_FLOW" in msg:
            return "diag"
        if "TEMP_RECOVERED" in msg:
            return "recover"
    return ""


def estimate_text_bytes(events: List[Dict[str, Any]]) -> int:
    total = 0
    for event in events:
        tag = event.get("tag", "unknown")
        f = fields_map(event)
        parts = [f"{k}={v}" for k, v in f.items()]
        line = f"{event.get('index', 0)} {tag} {' '.join(parts)}\n"
        total += len(line.encode("utf-8"))
    return total


def parse_binlog_frames(path: str) -> Tuple[List[Dict[str, Any]], str]:
    frames: List[Dict[str, Any]] = []
    p = Path(path)
    if not p.exists():
        return frames, "log missing"
    try:
        data = p.read_bytes()
    except OSError as e:
        return frames, f"read error: {e}"

    off = 0
    frame_no = 0
    total = len(data)
    while off + 8 <= total:
        frame_no += 1
        frame_header = struct.unpack_from("<I", data, off)[0]
        checksum_type = (frame_header >> 30) & 0x3
        varstr = 1 if (frame_header & 0x20000000) else 0
        payload_len = frame_header & 0x1FFFFFFF
        frame_size = 4 + payload_len + 4
        if payload_len < 11 or payload_len > 1024 * 1024:
            return frames, f"invalid payload_len={payload_len} at frame={frame_no}"
        if checksum_type not in (0, 1, 2):
            return frames, f"invalid checksum_type={checksum_type} at frame={frame_no}"
        if off + frame_size > total:
            return frames, f"truncated at frame={frame_no}"
        frames.append(
            {
                "frame_no": frame_no,
                "start": off,
                "end": off + frame_size,
                "frame_size": frame_size,
                "payload_len": payload_len,
                "checksum_type": checksum_type,
                "varstr": varstr,
                "binary_hex": " ".join(f"{b:02x}" for b in data[off : off + frame_size]),
            }
        )
        off += frame_size
    if off != total:
        return frames, f"trailing_bytes={total - off}"
    return frames, ""


def build_frame_mapping_text(frames: List[Dict[str, Any]], events: List[Dict[str, Any]], tail: int = 20) -> str:
    if not frames:
        return "(no frame mapping)"
    lines: List[str] = []
    event_by_idx = {int(e.get("index", 0)): e for e in events}
    for f in frames[-tail:]:
        idx = int(f["frame_no"])
        ev = event_by_idx.get(idx)
        if ev:
            line = (
                f"frame#{idx:>4} bytes[{f['start']:>6}:{f['end']:<6}) "
                f"payload={f['payload_len']:<4} -> {ev.get('tag','?')} | {event_summary(ev.get('tag','?'), fields_map(ev))}"
            )
        else:
            line = (
                f"frame#{idx:>4} bytes[{f['start']:>6}:{f['end']:<6}) "
                f"payload={f['payload_len']:<4} -> (no decoded row)"
            )
        lines.append(line)
    return "\n".join(lines)


def build_frame_pairs(
    frames: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
    frame_err: str = "",
    event_is_view: bool = False,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    event_by_idx = {int(e.get("index", 0)): e for e in events}
    for f in frames:
        idx = int(f.get("frame_no", 0))
        ev = event_by_idx.get(idx)
        if ev is None:
            rows.append(
                {
                    "index": idx,
                    "start": int(f.get("start", 0)),
                    "end": int(f.get("end", 0)),
                    "payload_len": int(f.get("payload_len", 0)),
                    "checksum_type": int(f.get("checksum_type", 0)),
                    "varstr": int(f.get("varstr", 0)),
                    "binary_hex": str(f.get("binary_hex", "")),
                    "uptime": "--",
                    "tag": "(unknown)",
                    "decoded_line": "(no decoded row)",
                    "anomaly": "",
                }
            )
            continue

        if event_is_view:
            tag = str(ev.get("tag", "unknown"))
            uptime = str(ev.get("uptime", "--"))
            decoded_line = str(ev.get("summary", "(no decoded row)"))
            anomaly = str(ev.get("anomaly", ""))
        else:
            fm = fields_map(ev)
            tag = str(ev.get("tag", "unknown"))
            uptime = fmt_uptime(int(fm.get("uptime_ms", 0)))
            decoded_line = event_summary(tag, fm)
            anomaly = classify_event_anomaly(tag, fm)
        rows.append(
            {
                "index": idx,
                "start": int(f.get("start", 0)),
                "end": int(f.get("end", 0)),
                "payload_len": int(f.get("payload_len", 0)),
                "checksum_type": int(f.get("checksum_type", 0)),
                "varstr": int(f.get("varstr", 0)),
                "binary_hex": str(f.get("binary_hex", "")),
                "uptime": uptime,
                "tag": tag,
                "decoded_line": decoded_line,
                "anomaly": anomaly,
            }
        )
    if rows and frame_err:
        tail_damage = (
            "truncated" in frame_err
            or "trailing_bytes" in frame_err
            or "invalid payload_len" in frame_err
            or "invalid checksum_type" in frame_err
        )
        if tail_damage:
            rows[-1]["anomaly"] = rows[-1].get("anomaly") or "powercut"
    return rows


def parse_shared_layout(path: str) -> str:
    p = Path(path)
    if not p.exists():
        return "(shared file missing)"
    try:
        data = p.read_bytes()
    except OSError as e:
        return f"(shared read error: {e})"
    if len(data) < 56:
        return f"(shared too short: {len(data)} bytes)"

    try:
        magic = data[0:8].decode("ascii", errors="replace")
        header_version = struct.unpack_from("<I", data, 8)[0]
        state = struct.unpack_from("<I", data, 12)[0]
        num_arrays = struct.unpack_from("<I", data, 16)[0]
        tag_count = struct.unpack_from("<I", data, 20)[0]
        bitmap_offset = struct.unpack_from("<i", data, 24)[0]
        eventtag_offset = struct.unpack_from("<i", data, 28)[0]
        schema_hash = struct.unpack_from("<I", data, 32)[0]
        generation = struct.unpack_from("<Q", data, 36)[0]
        total_size = struct.unpack_from("<I", data, 44)[0]
        wait_loops = struct.unpack_from("<I", data, 48)[0]
        wait_ms = struct.unpack_from("<I", data, 52)[0]
    except struct.error as e:
        return f"(shared header parse error: {e})"

    bitmap_size = num_arrays * 13
    tag_size = tag_count * 54
    lines: List[str] = []
    lines.append("Shared Binary Layout")
    lines.append(f"- magic={magic} version={header_version} state={state}")
    lines.append(f"- schema_hash=0x{schema_hash:08x} generation={generation}")
    lines.append(f"- total_size={total_size} file_size={len(data)} wait_loops={wait_loops} wait_ms={wait_ms}")
    lines.append(f"- header bytes[0:56)")
    lines.append(f"- bitmap bytes[{bitmap_offset}:{bitmap_offset + bitmap_size}) count={num_arrays}")
    lines.append(f"- eventtag bytes[{eventtag_offset}:{eventtag_offset + tag_size}) count={tag_count}")

    max_tags = min(tag_count, 6)
    lines.append("")
    lines.append("Tag Layout (head)")
    for i in range(max_tags):
        pos = eventtag_offset + i * 54
        if pos + 54 > len(data):
            break
        hdr = struct.unpack_from("<H", data, pos)[0]
        tag_index = hdr & 0x0FFF
        tag_ele_num = (hdr >> 12) & 0x0F
        tag_ele_offset = struct.unpack_from("<i", data, pos + 2)[0]
        tag_name_raw = data[pos + 6 : pos + 54]
        tag_name = tag_name_raw.split(b"\x00", 1)[0].decode("utf-8", errors="replace")
        lines.append(
            f"- tag[{i}] id={tag_index} ele_num={tag_ele_num} name={tag_name} "
            f"tag_ele_offset={tag_ele_offset}"
        )
        for e in range(min(tag_ele_num, 4)):
            ep = tag_ele_offset + e * 33
            if ep + 33 > len(data):
                break
            first = data[ep]
            ele_type = first & 0x03
            ele_len = (first >> 2) & 0x3F
            ele_name_raw = data[ep + 1 : ep + 33]
            ele_name = ele_name_raw.split(b"\x00", 1)[0].decode("utf-8", errors="replace")
            lines.append(f"    - ele[{e}] type={ele_type} len={ele_len} name={ele_name}")
    lines.append("")
    lines.append("Mapping: shared(tag/field layout) -> run.bin(frame payload) -> decoded(jsonl/table)")
    return "\n".join(lines)


def parse_shared_schema_structured(path: str) -> Dict[str, Any]:
    """解析 shared.bin，返回结构化 schema 信息供前端可视化渲染。"""
    result: Dict[str, Any] = {"ok": False, "error": "", "header": {}, "tags": [], "frame_layout": {}}
    p = Path(path)
    if not p.exists():
        result["error"] = "shared file missing"
        return result
    try:
        data = p.read_bytes()
    except OSError as e:
        result["error"] = f"read error: {e}"
        return result
    if len(data) < 56:
        result["error"] = f"shared too short: {len(data)} bytes"
        return result
    try:
        magic = data[0:8].decode("ascii", errors="replace").rstrip("\x00")
        header_version = struct.unpack_from("<I", data, 8)[0]
        state_val = struct.unpack_from("<I", data, 12)[0]
        num_arrays = struct.unpack_from("<I", data, 16)[0]
        tag_count = struct.unpack_from("<I", data, 20)[0]
        bitmap_offset = struct.unpack_from("<i", data, 24)[0]
        eventtag_offset = struct.unpack_from("<i", data, 28)[0]
        schema_hash = struct.unpack_from("<I", data, 32)[0]
        generation = struct.unpack_from("<Q", data, 36)[0]
        total_size = struct.unpack_from("<I", data, 44)[0]
    except struct.error as e:
        result["error"] = f"header parse error: {e}"
        return result

    bitmap_size = num_arrays * 13
    tag_size = tag_count * 54
    result["header"] = {
        "magic": magic,
        "version": header_version,
        "state": state_val,
        "schema_hash": f"0x{schema_hash:08x}",
        "generation": generation,
        "total_size": total_size,
        "file_size": len(data),
        "num_arrays": num_arrays,
        "tag_count": tag_count,
        "header_range": "0:56",
        "bitmap_range": f"{bitmap_offset}:{bitmap_offset + bitmap_size}",
        "eventtag_range": f"{eventtag_offset}:{eventtag_offset + tag_size}",
    }
    result["frame_layout"] = {
        "segments": [
            {
                "name": "Frame Header",
                "bytes": 4,
                "color": "#0071e3",
                "desc": "payload_len[28:0] + varstr[29] + checksum_type[31:30]",
            },
            {
                "name": "Payload",
                "bytes": -1,
                "color": "#16a34a",
                "desc": "timestamp(8) + tag_id(2) + ele_count(1) + fields",
            },
            {
                "name": "Checksum",
                "bytes": 4,
                "color": "#f59e0b",
                "desc": "4B trailer: CRC32 / CRC32C / none(0)",
            },
        ],
        "payload_prefix": [
            {"name": "timestamp", "bytes": 8, "color": "#15803d", "desc": "int64 little-endian"},
            {"name": "tag_id", "bytes": 2, "color": "#0f766e", "desc": "uint16 little-endian"},
            {"name": "ele_count", "bytes": 1, "color": "#166534", "desc": "uint8"},
            {"name": "fields", "bytes": -1, "color": "#22c55e", "desc": "values encoded in schema order"},
        ],
        "frame_header_bytes": 4,
        "payload_prefix_bytes": 11,
        "checksum_bytes": 4,
        "frame_fixed_overhead": 19,
    }

    tags: List[Dict[str, Any]] = []
    type_map = {1: "uint", 2: "double", 3: "string", 0: "unknown"}
    for i in range(min(tag_count, 12)):
        pos = eventtag_offset + i * 54
        if pos + 54 > len(data):
            break
        try:
            hdr = struct.unpack_from("<H", data, pos)[0]
            tag_index = hdr & 0x0FFF
            tag_ele_num = (hdr >> 12) & 0x0F
            tag_ele_offset = struct.unpack_from("<i", data, pos + 2)[0]
            tag_name = data[pos + 6 : pos + 54].split(b"\x00", 1)[0].decode("utf-8", errors="replace")
        except struct.error:
            break
        fields: List[Dict[str, Any]] = []
        for e in range(min(tag_ele_num, 8)):
            ep = tag_ele_offset + e * 33
            if ep + 33 > len(data):
                break
            first = data[ep]
            ele_type = first & 0x03
            ele_len = (first >> 2) & 0x3F
            ele_name = data[ep + 1 : ep + 33].split(b"\x00", 1)[0].decode("utf-8", errors="replace")
            if ele_type == 2:
                wire_fixed = 8
                wire_varmax = 8
                wire_rule = "double fixed 8B"
            elif ele_type == 3:
                wire_fixed = int(ele_len)
                wire_varmax = int(2 + ele_len)
                wire_rule = "string: fixed=lenB; varstr=max(2+len)B"
            else:
                wire_fixed = int(ele_len)
                wire_varmax = int(ele_len)
                wire_rule = "fixed len bytes"
            fields.append(
                {
                    "name": ele_name,
                    "type": type_map.get(ele_type, str(ele_type)),
                    "type_code": int(ele_type),
                    "len": int(ele_len),
                    "wire_bytes_fixed": wire_fixed,
                    "wire_bytes_varmax": wire_varmax,
                    "wire_rule": wire_rule,
                }
            )
        payload_fields_fixed = sum(int(f.get("wire_bytes_fixed", 0)) for f in fields)
        payload_fields_varmax = sum(int(f.get("wire_bytes_varmax", 0)) for f in fields)
        payload_fixed = 11 + payload_fields_fixed
        payload_varmax = 11 + payload_fields_varmax
        enc_size = 4 + payload_fixed + 4
        enc_size_varmax = 4 + payload_varmax + 4
        tags.append(
            {
                "id": int(tag_index),
                "name": tag_name,
                "field_count": int(tag_ele_num),
                "fields": fields,
                "encoded_size": int(enc_size),
                "encoded_size_varmax": int(enc_size_varmax),
                "payload_prefix_bytes": 11,
                "payload_fields_fixed": int(payload_fields_fixed),
                "payload_fields_varmax": int(payload_fields_varmax),
                "payload_bytes_fixed": int(payload_fixed),
                "payload_bytes_varmax": int(payload_varmax),
                "has_string_field": any(int(f.get("type_code", 0)) == 3 for f in fields),
            }
        )
    result["tags"] = tags
    result["ok"] = True
    return result


def default_state() -> Dict[str, Any]:
    return {
        "uptime_ms": 0,
        "uptime_text": "00:00.000",
        "boot_stage": "ROM",
        "net_state": "DOWN",
        "soc": 100.0,
        "voltage_mv": 4100,
        "current_ma": 0,
        "rssi_dbm": 0,
        "latency_us": 0,
        "pwm": 0,
        "sensor_1": 0.0,
        "sensor_2": 0.0,
        "sensor_3": 0.0,
        "alert_active": False,
        "seen_alert": False,
    }


def event_to_view(event: Dict[str, Any]) -> Dict[str, Any]:
    f = fields_map(event)
    uptime = int(f.get("uptime_ms", 0))
    tag = event.get("tag", "unknown")
    anomaly = classify_event_anomaly(tag, f)
    return {
        "index": int(event.get("index", 0)),
        "uptime": fmt_uptime(uptime),
        "tag": tag,
        "summary": event_summary(tag, f),
        "anomaly": anomaly,
    }


class LiveModel:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.start_ts = time.time()

        self.last_index = 0
        self.counters: Counter = Counter()
        self.recent: Deque[Dict[str, Any]] = deque(maxlen=max(40, args.max_recent))
        self.full_events: List[Dict[str, Any]] = []

        self.state = default_state()
        self.incident = {
            "status": "normal",
            "actions": ["等待异常注入触发..."],
        }

        self.decode_cache = ""
        self.schema_text = ""
        self.decode_ms = 0.0
        self.readable_tail_lines = 0
        self.decoded_jsonl_tail = ""
        self.decoded_brief_tail = ""
        self.text_est_bytes = 0
        self.frame_mapping_tail = ""
        self.frame_pairs: List[Dict[str, Any]] = []
        self.frame_parse_error = ""
        self.shared_layout_text = ""
        self.manual_inject_count = 0
        self.powercut_result = ""
        self.timeline_marks: List[Dict[str, Any]] = []
        self.powercut_case: Dict[str, Any] = {
            "active": False,
            "stage": "idle",
            "updated_at": "-",
            "drop_bytes": 0,
            "before_bytes": 0,
            "after_bytes": 0,
            "repair_before_bytes": 0,
            "repair_after_bytes": 0,
            "repair_drop_bytes": 0,
            "records_before": 0,
            "records_after_powercut": 0,
            "records_after_repair": 0,
            "frame_err_after_powercut": "",
            "reader_tail": "",
            "tail_hex_before": "",
            "tail_hex_after": "",
            "tail_hex_dropped": "",
        }
        self.control_file = Path(args.control_file) if args.control_file else None
        self.control_pause = False
        self.control_step_token = 0
        self.control_interval_ms = 180
        self.powercut_freeze = False
        self.last_decode_sig: Tuple[int, int, int, int] = (0, 0, 0, 0)

        if self.control_file:
            self._init_control_file(self.control_interval_ms)
            self._load_control_state()

        inject_enabled = bool(args.inject_bin and Path(args.inject_bin).exists())

        self.snapshot: Dict[str, Any] = {
            "server_time": "",
            "config": {
                "fault_at_cycle": args.fault_at_cycle,
                "recover_at_cycle": args.recover_at_cycle,
                "inject_enabled": inject_enabled,
                "control_enabled": bool(self.control_file),
            },
            "files": {
                "shared_path": str(Path(args.shared).resolve()),
                "log_path": str(Path(args.log).resolve()),
                "shared_size": 0,
                "log_size": 0,
                "shared_mtime": "-",
                "log_mtime": "-",
                "shared_mtime_ns": 0,
                "log_mtime_ns": 0,
                "shared_hex_head": "",
                "log_hex_tail": "",
            },
            "schema": {
                "source_path": str(Path(args.schema_source).resolve()),
                "source_text": "",
                "layout_text": "",
                "structured": {},
            },
            "runtime": {
                "phase": "Phase 1/5 Boot",
                "records_total": 0,
                "state": copy.deepcopy(self.state),
                "counters": {},
                "recent_events": [],
                "full_events": [],
                "incident": copy.deepcopy(self.incident),
                "decode_ok": False,
                "decode_rc": 1,
                "decode_error": "not started",
                "decode_state": "waiting",
                "last_decode_at": "-",
                "manual_inject_count": 0,
                "readable_tail_lines": 0,
                "decoded_jsonl_tail": "",
                "decoded_brief_tail": "",
                "frame_mapping_tail": "",
                "frame_pairs": [],
                "frame_parse_error": "",
                "powercut_result": "",
                "powercut_case": {},
                "timeline_marks": [],
                "optimization": {
                    "binary_bytes": 0,
                    "text_est_bytes": 0,
                    "space_ratio_text_div_binary": 0.0,
                    "records_per_sec": 0.0,
                    "last_decode_ms": 0.0,
                },
                "control": {
                    "pause": False,
                    "step_token": 0,
                    "interval_ms": self.control_interval_ms,
                    "powercut_freeze": False,
                },
            },
        }

    def _push_incident_action(self, text: str) -> None:
        actions = self.incident.setdefault("actions", [])
        if not actions or actions[-1] != text:
            actions.append(text)
        if len(actions) > 20:
            self.incident["actions"] = actions[-20:]

    def _push_timeline_mark(
        self,
        kind: str,
        text: str,
        event_index: int = 0,
        uptime: str = "--",
    ) -> None:
        mark = {
            "at": datetime.now().strftime("%H:%M:%S"),
            "kind": kind,
            "text": text,
            "event_index": int(event_index),
            "uptime": uptime,
        }
        marks = self.timeline_marks
        if marks:
            prev = marks[-1]
            if (
                prev.get("kind") == mark["kind"]
                and prev.get("text") == mark["text"]
                and int(prev.get("event_index", 0)) == mark["event_index"]
            ):
                return
        marks.append(mark)
        if len(marks) > 80:
            self.timeline_marks = marks[-80:]

    def _init_control_file(self, interval_ms: int) -> None:
        if not self.control_file:
            return
        self.control_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.control_file.exists():
            self.control_file.write_text(
                f"pause=0\nstep_token=0\ninterval_ms={max(0, int(interval_ms))}\n",
                encoding="utf-8",
            )

    def _load_control_state(self) -> None:
        if not self.control_file or not self.control_file.exists():
            return
        pause = self.control_pause
        step_token = self.control_step_token
        interval_ms = self.control_interval_ms
        try:
            text = self.control_file.read_text(encoding="utf-8")
        except OSError:
            return
        for line in text.splitlines():
            if line.startswith("pause="):
                try:
                    pause = bool(int(line.split("=", 1)[1].strip() or "0"))
                except ValueError:
                    pass
            elif line.startswith("step_token="):
                try:
                    step_token = int(line.split("=", 1)[1].strip() or "0")
                except ValueError:
                    pass
            elif line.startswith("interval_ms="):
                try:
                    interval_ms = int(line.split("=", 1)[1].strip() or "0")
                except ValueError:
                    pass
        self.control_pause = pause
        self.control_step_token = max(0, step_token)
        self.control_interval_ms = max(0, interval_ms)

    def _save_control_state(self) -> None:
        if not self.control_file:
            return
        self.control_file.parent.mkdir(parents=True, exist_ok=True)
        self.control_file.write_text(
            (
                f"pause={1 if self.control_pause else 0}\n"
                f"step_token={max(0, self.control_step_token)}\n"
                f"interval_ms={max(0, self.control_interval_ms)}\n"
            ),
            encoding="utf-8",
        )

    def _update_from_event(self, event: Dict[str, Any], emit_marks: bool = True) -> None:
        tag = event.get("tag", "unknown")
        f = fields_map(event)
        idx = int(event.get("index", 0))
        uptime = int(f.get("uptime_ms", self.state["uptime_ms"]))
        self.state["uptime_ms"] = uptime
        self.state["uptime_text"] = fmt_uptime(uptime)
        self.counters[tag] += 1

        if tag == "boot_stage":
            self.state["boot_stage"] = BOOT_STAGE_MAP.get(int(f.get("stage", 0)), "UNKNOWN")
        elif tag == "sensor_sample":
            sid = int(f.get("sensor_id", 0))
            val = int(f.get("value_x100", 0)) / 100.0
            if sid == 1:
                self.state["sensor_1"] = val
            elif sid == 2:
                self.state["sensor_2"] = val
            elif sid == 3:
                self.state["sensor_3"] = val
        elif tag == "control_loop":
            self.state["latency_us"] = int(f.get("latency_us", 0))
            self.state["pwm"] = int(f.get("pwm", 0))
        elif tag == "net_state":
            self.state["net_state"] = NET_STATE_MAP.get(int(f.get("state", 0)), "UNKNOWN")
            self.state["rssi_dbm"] = int(f.get("rssi_dbm", 0))
        elif tag == "power_state":
            self.state["soc"] = float(int(f.get("soc", 0)))
            self.state["voltage_mv"] = int(f.get("voltage_mv", 0))
            self.state["current_ma"] = int(f.get("current_ma", 0))
        elif tag == "alert_event":
            level = int(f.get("level", 0))
            fault = int(f.get("fault", 0))
            if level >= 2:
                self.state["seen_alert"] = True
                self.state["alert_active"] = True
                self.incident["status"] = "active"
                self._push_incident_action(
                    f"{fmt_uptime(uptime)} 告警触发 fault={fault} level={ALERT_LEVEL_MAP.get(level, level)}"
                )
                if emit_marks:
                    self._push_timeline_mark(
                        "fault",
                        f"#{idx} ALERT fault={fault} level={ALERT_LEVEL_MAP.get(level, level)}",
                        event_index=idx,
                        uptime=fmt_uptime(uptime),
                    )
            else:
                self.state["alert_active"] = False
                if self.state["seen_alert"]:
                    self.incident["status"] = "resolved"
                self._push_incident_action(
                    f"{fmt_uptime(uptime)} 告警级别回落 level={ALERT_LEVEL_MAP.get(level, level)}"
                )
                if emit_marks:
                    self._push_timeline_mark(
                        "recover",
                        f"#{idx} ALERT level={ALERT_LEVEL_MAP.get(level, level)}",
                        event_index=idx,
                        uptime=fmt_uptime(uptime),
                    )
        elif tag == "note_event":
            msg = str(f.get("msg", ""))
            if "TEMP_WARN" in msg:
                self.state["seen_alert"] = True
                self.state["alert_active"] = True
                self.incident["status"] = "active"
                self._push_incident_action(f"{fmt_uptime(uptime)} NOTE: {msg} -> 进入异常处理")
                if emit_marks:
                    self._push_timeline_mark(
                        "fault",
                        f"#{idx} NOTE {msg}",
                        event_index=idx,
                        uptime=fmt_uptime(uptime),
                    )
            elif "DIAG_RETRY_FLOW" in msg:
                self.incident["status"] = "recovering"
                self._push_incident_action(f"{fmt_uptime(uptime)} NOTE: {msg} -> 诊断/重试")
                if emit_marks:
                    self._push_timeline_mark(
                        "diag",
                        f"#{idx} NOTE {msg}",
                        event_index=idx,
                        uptime=fmt_uptime(uptime),
                    )
            elif "TEMP_RECOVERED" in msg:
                self.state["alert_active"] = False
                self.incident["status"] = "resolved"
                self._push_incident_action(f"{fmt_uptime(uptime)} NOTE: {msg} -> 异常恢复")
                if emit_marks:
                    self._push_timeline_mark(
                        "recover",
                        f"#{idx} NOTE {msg}",
                        event_index=idx,
                        uptime=fmt_uptime(uptime),
                    )

    def _refresh_file_meta(self) -> None:
        files = self.snapshot["files"]
        shared = Path(self.args.shared)
        log = Path(self.args.log)

        if shared.exists():
            st = shared.stat()
            files["shared_size"] = st.st_size
            files["shared_mtime"] = datetime.fromtimestamp(st.st_mtime).strftime("%H:%M:%S")
            files["shared_mtime_ns"] = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000)))
            try:
                with shared.open("rb") as f:
                    head = f.read(80)
                files["shared_hex_head"] = " ".join(f"{b:02x}" for b in head) if head else "(empty shared)"
            except OSError as e:
                files["shared_hex_head"] = f"read error: {e}"
        else:
            files["shared_size"] = 0
            files["shared_mtime"] = "-"
            files["shared_mtime_ns"] = 0
            files["shared_hex_head"] = "(shared file not created yet)"

        if log.exists():
            st = log.stat()
            files["log_size"] = st.st_size
            files["log_mtime"] = datetime.fromtimestamp(st.st_mtime).strftime("%H:%M:%S")
            files["log_mtime_ns"] = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000)))
            try:
                with log.open("rb") as f:
                    if st.st_size > 80:
                        f.seek(st.st_size - 80)
                    tail = f.read(80)
                files["log_hex_tail"] = " ".join(f"{b:02x}" for b in tail) if tail else "(empty log)"
            except OSError as e:
                files["log_hex_tail"] = f"read error: {e}"
        else:
            files["log_size"] = 0
            files["log_mtime"] = "-"
            files["log_mtime_ns"] = 0
            files["log_hex_tail"] = "(log file not created yet)"

    def _refresh_schema_text(self) -> None:
        p = Path(self.args.schema_source)
        if not p.exists():
            self.schema_text = "(schema source missing)"
            self.shared_layout_text = "(shared layout unavailable)"
        else:
            try:
                self.schema_text = p.read_text(encoding="utf-8")
            except OSError as e:
                self.schema_text = f"(schema read error: {e})"
                self.shared_layout_text = "(shared layout unavailable)"
            else:
                self.shared_layout_text = parse_shared_layout(self.args.shared)
        self.snapshot["schema"]["structured"] = parse_shared_schema_structured(self.args.shared)

    def _build_readable_tail(self) -> None:
        lines = [x for x in self.decode_cache.splitlines() if x.strip()]
        tail_jsonl = lines[-20:]
        self.readable_tail_lines = len(tail_jsonl)
        self.decoded_jsonl_tail = "\n".join(tail_jsonl)

        tail_brief: List[str] = []
        for item in self.full_events[-20:]:
            tail_brief.append(f"#{item['index']:>4} {item['uptime']} {item['tag']:<14} {item['summary']}")
        self.decoded_brief_tail = "\n".join(tail_brief)

    def _decode_current(self) -> None:
        runtime = self.snapshot["runtime"]
        cmd = [
            self.args.read_bin,
            "--shared",
            self.args.shared,
            "--log",
            self.args.log,
            "--format",
            "jsonl",
            "--limit",
            "0",
        ]
        t0 = time.time()
        cp = subprocess.run(cmd, text=True, capture_output=True, check=False)
        self.decode_ms = (time.time() - t0) * 1000.0

        runtime["decode_rc"] = cp.returncode
        runtime["decode_ok"] = cp.returncode == 0
        runtime["decode_state"] = "decoded"
        runtime["decode_error"] = (cp.stderr or "").strip()[-300:] if cp.returncode != 0 else ""
        runtime["last_decode_at"] = datetime.now().strftime("%H:%M:%S")
        files = self.snapshot.get("files", {})
        self.last_decode_sig = (
            int(files.get("shared_size", 0)),
            int(files.get("shared_mtime_ns", 0)),
            int(files.get("log_size", 0)),
            int(files.get("log_mtime_ns", 0)),
        )

        if cp.returncode != 0:
            frames, frame_err = parse_binlog_frames(self.args.log)
            self.frame_parse_error = frame_err
            prev_total = len(self.full_events)
            parsable_total = len(frames)
            # 解码失败时按“当前可解析完整帧数”回退展示，避免沿用旧结果掩盖真实丢失。
            if self.full_events:
                self.full_events = list(self.full_events[:parsable_total])
            else:
                self.full_events = []
            self.recent.clear()
            for item in self.full_events[-self.recent.maxlen :]:
                self.recent.append(item)
            self.last_index = parsable_total
            runtime["records_total"] = self.last_index

            lost_count = max(0, prev_total - parsable_total)
            if lost_count > 0:
                self._push_incident_action(
                    f"日志尾部损坏：完整帧 {prev_total}->{parsable_total}，丢失 {lost_count} 条"
                )
                self._push_timeline_mark(
                    "powercut",
                    f"尾部损坏: 完整帧 {prev_total}->{parsable_total} (丢{lost_count})",
                    event_index=parsable_total,
                    uptime=self.state.get("uptime_text", "--"),
                )

            self.frame_pairs = build_frame_pairs(frames, self.full_events, frame_err=frame_err, event_is_view=True)
            self.frame_mapping_tail = build_frame_mapping_text(frames, [], tail=20)
            return

        self.decode_cache = cp.stdout or ""
        parsed: List[Dict[str, Any]] = []
        for line in self.decode_cache.splitlines():
            s = line.strip()
            if not s or not s.startswith("{"):
                continue
            try:
                parsed.append(json.loads(s))
            except json.JSONDecodeError:
                continue
        frames, frame_err = parse_binlog_frames(self.args.log)
        self.frame_parse_error = frame_err

        # 只保留“有完整帧映射”的解码事件，确保时间线/双列展示与真实可解析二进制一致。
        parsable_total = len(frames)
        if parsable_total >= 0 and len(parsed) > parsable_total:
            parsed = parsed[:parsable_total]

        if not parsed:
            self.frame_pairs = build_frame_pairs(frames, [], frame_err=frame_err)
            self.frame_mapping_tail = build_frame_mapping_text(frames, [], tail=20)
            self.full_events = []
            self.recent.clear()
            self.last_index = 0
            runtime["records_total"] = 0
            return

        max_idx = max(int(x.get("index", 0)) for x in parsed)
        rebuild_full = False
        emit_marks = True
        if self.last_index > 0 and max_idx < self.last_index:
            # 当索引大幅回退时，视作新轮次；轻微回退通常来自尾部修复，不应重复刷标注。
            rebuild_full = True
            if max_idx < max(8, self.last_index // 2):
                self.timeline_marks = []
                self.incident = {"status": "normal", "actions": ["检测到新日志轮次，状态已重置"]}
                emit_marks = True
            else:
                self._push_incident_action("检测到日志尾部回退，已按当前日志重建状态")
                emit_marks = False
            self.last_index = 0
            self.counters.clear()
            self.recent.clear()
            self.full_events = []
            self.state = default_state()

        if rebuild_full:
            for event in parsed:
                self._update_from_event(event, emit_marks=emit_marks)
        else:
            new_events = [x for x in parsed if int(x.get("index", 0)) > self.last_index]
            for event in new_events:
                self._update_from_event(event)

        self.full_events = [event_to_view(e) for e in parsed][-4000:]
        self.recent.clear()
        for item in self.full_events[-self.recent.maxlen :]:
            self.recent.append(item)

        # records_total 反映“当前日志中的真实最新索引”，而不是历史最大值。
        self.last_index = max_idx
        runtime["records_total"] = self.last_index
        self.text_est_bytes = estimate_text_bytes(parsed)
        self._build_readable_tail()
        self.frame_pairs = build_frame_pairs(frames, parsed, frame_err=frame_err)
        self.frame_mapping_tail = build_frame_mapping_text(frames, parsed, tail=20)

    def _refresh_runtime_snapshot(self) -> None:
        runtime = self.snapshot["runtime"]
        runtime["phase"] = phase_from_state(self.state)
        runtime["state"] = copy.deepcopy(self.state)
        runtime["counters"] = dict(self.counters)
        runtime["recent_events"] = list(self.recent)
        runtime["full_events"] = list(self.full_events)
        runtime["incident"] = copy.deepcopy(self.incident)
        runtime["manual_inject_count"] = self.manual_inject_count
        runtime["readable_tail_lines"] = self.readable_tail_lines
        runtime["decoded_jsonl_tail"] = self.decoded_jsonl_tail
        runtime["decoded_brief_tail"] = self.decoded_brief_tail
        runtime["frame_mapping_tail"] = self.frame_mapping_tail
        runtime["frame_pairs"] = list(self.frame_pairs)
        runtime["frame_parse_error"] = self.frame_parse_error
        runtime["powercut_result"] = self.powercut_result
        runtime["powercut_case"] = copy.deepcopy(self.powercut_case)
        runtime["timeline_marks"] = copy.deepcopy(self.timeline_marks[-24:])

        binary_bytes = int(self.snapshot["files"].get("log_size", 0))
        text_bytes = int(self.text_est_bytes)
        ratio = (float(text_bytes) / float(binary_bytes)) if binary_bytes > 0 else 0.0
        elapsed = max(0.001, time.time() - self.start_ts)
        records_per_sec = float(self.last_index) / elapsed
        runtime["optimization"] = {
            "binary_bytes": binary_bytes,
            "text_est_bytes": text_bytes,
            "space_ratio_text_div_binary": ratio,
            "records_per_sec": records_per_sec,
            "last_decode_ms": self.decode_ms,
        }
        runtime["control"] = {
            "pause": bool(self.control_pause),
            "step_token": int(self.control_step_token),
            "interval_ms": int(self.control_interval_ms),
            "powercut_freeze": bool(self.powercut_freeze),
        }

    def refresh(self) -> None:
        with self.lock:
            self._load_control_state()
            self._refresh_file_meta()
            self._refresh_schema_text()

            runtime = self.snapshot["runtime"]
            log_size = int(self.snapshot["files"].get("log_size", 0))
            if log_size > 0 and Path(self.args.shared).exists():
                curr_sig = (
                    int(self.snapshot["files"].get("shared_size", 0)),
                    int(self.snapshot["files"].get("shared_mtime_ns", 0)),
                    int(self.snapshot["files"].get("log_size", 0)),
                    int(self.snapshot["files"].get("log_mtime_ns", 0)),
                )
                if curr_sig != self.last_decode_sig:
                    self._decode_current()
                    self.last_decode_sig = curr_sig
                else:
                    runtime["decode_state"] = "idle"
                    runtime["decode_error"] = ""
            else:
                runtime["decode_ok"] = False
                runtime["decode_rc"] = 1
                runtime["decode_state"] = "waiting"
                runtime["decode_error"] = "waiting for first stream frame"
                if runtime.get("last_decode_at") in {"", "-"}:
                    runtime["last_decode_at"] = datetime.now().strftime("%H:%M:%S")

            self.snapshot["server_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.snapshot["schema"]["source_text"] = self.schema_text
            self.snapshot["schema"]["layout_text"] = self.shared_layout_text
            self._refresh_runtime_snapshot()

            if self.args.state_out:
                try:
                    Path(self.args.state_out).write_text(
                        json.dumps(self.snapshot, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                except OSError:
                    pass

    def inject(self, action: str) -> Tuple[bool, str]:
        inject_bin = Path(self.args.inject_bin) if self.args.inject_bin else None
        if not inject_bin or not inject_bin.exists():
            return False, "injector not configured"

        if action not in {"fault", "diag", "recover"}:
            return False, f"unsupported action: {action}"

        uptime_ms = int(self.state.get("uptime_ms", 0)) + 25
        cmd = [
            str(inject_bin),
            "--shared",
            self.args.shared,
            "--log",
            self.args.log,
            "--action",
            action,
            "--uptime-ms",
            str(uptime_ms),
        ]
        cp = subprocess.run(cmd, text=True, capture_output=True, check=False)
        if cp.returncode != 0:
            err = (cp.stderr or cp.stdout or "inject failed").strip()[-280:]
            return False, err

        self.manual_inject_count += 1
        self._push_incident_action(f"{fmt_uptime(uptime_ms)} 手动注入 action={action}")
        kind_map = {"fault": "fault", "diag": "diag", "recover": "recover"}
        self._push_timeline_mark(
            kind_map.get(action, "control"),
            f"手动注入 action={action}",
            event_index=max(0, self.last_index),
            uptime=fmt_uptime(uptime_ms),
        )

        # 注入后立即刷新一次，让看板能马上看到新增事件。
        self._refresh_file_meta()
        self._decode_current()
        self._refresh_runtime_snapshot()
        return True, (cp.stdout or f"inject_ok action={action}").strip()

    def control_action(self, action: str, interval_ms: int = -1) -> Tuple[bool, str]:
        if not self.control_file:
            return False, "control file not configured"

        if action == "pause":
            self.control_pause = True
            self._save_control_state()
            self._push_timeline_mark("control", "设备已暂停")
            self._refresh_runtime_snapshot()
            return True, "device paused"
        if action == "resume":
            if self.powercut_freeze or bool(self.powercut_case.get("active", False)):
                self._push_timeline_mark("control", "继续前自动恢复触发")
                ok, repair_msg = self.repair_powercut(source="auto_resume", pause_after=False)
                if not ok:
                    self.control_pause = True
                    self.powercut_freeze = True
                    self._save_control_state()
                    self._refresh_runtime_snapshot()
                    return False, f"继续前自动恢复失败：{repair_msg}"
                self.control_pause = False
                self.powercut_freeze = False
                self._save_control_state()
                self._push_timeline_mark("control", "自动恢复完成，设备继续运行")
                self._refresh_runtime_snapshot()
                return True, f"自动恢复后继续运行：{repair_msg}"
            self.control_pause = False
            self.powercut_freeze = False
            self._save_control_state()
            self._push_timeline_mark("control", "设备已继续")
            self._refresh_runtime_snapshot()
            return True, "device resumed"
        if action == "step":
            self.control_pause = True
            self.control_step_token += 1
            self._save_control_state()
            self._push_timeline_mark("control", f"设备单步执行 token={self.control_step_token}")
            self._refresh_runtime_snapshot()
            return True, f"device stepped token={self.control_step_token}"
        if action == "set_interval":
            if interval_ms < 0:
                return False, "interval_ms must be >= 0"
            self.control_interval_ms = int(interval_ms)
            self._save_control_state()
            self._push_timeline_mark("control", f"设备间隔调整为 {self.control_interval_ms}ms")
            self._refresh_runtime_snapshot()
            return True, f"interval set to {self.control_interval_ms}ms"
        return False, f"unsupported control action: {action}"

    def simulate_powercut(self, drop_bytes: int = 7) -> Tuple[bool, str]:
        log_path = Path(self.args.log)
        if not log_path.exists():
            return False, "log not exists"
        if drop_bytes <= 0:
            drop_bytes = 1
        size = log_path.stat().st_size
        if size <= drop_bytes:
            return False, f"log too small ({size} bytes)"
        frames_before_cut, _ = parse_binlog_frames(str(log_path))
        records_before = len(frames_before_cut)
        backup = log_path.with_suffix(log_path.suffix + ".powercut.backup.bin")
        try:
            data = log_path.read_bytes()
            # 真实截断当前运行日志，用于在看板中可视化“写入中断导致的坏尾”。
            before_tail = data[-min(48, len(data)) :]
            dropped = data[-drop_bytes:]
            after_data = data[:-drop_bytes]
            backup.write_bytes(data)
            log_path.write_bytes(after_data)
            after_tail = after_data[-min(48, len(after_data)) :] if after_data else b""
        except OSError as e:
            return False, f"powercut file error: {e}"

        cmd = [
            self.args.read_bin,
            "--shared",
            self.args.shared,
            "--log",
            str(log_path),
            "--format",
            "jsonl",
            "--limit",
            "0",
        ]
        cp = subprocess.run(cmd, text=True, capture_output=True, check=False)
        frames_after_cut, frame_err_after_cut = parse_binlog_frames(str(log_path))
        records_after_cut = len(frames_after_cut)
        lost_records_after_cut = max(0, records_before - records_after_cut)
        err = (cp.stderr or cp.stdout or "").strip().splitlines()
        tail = err[-1] if err else ("ok" if cp.returncode == 0 else "decode failed")
        detected = (cp.returncode != 0) or bool(frame_err_after_cut)
        if detected:
            msg = f"断电截断完成：drop={drop_bytes}B，reader 已检测到尾部异常"
        else:
            msg = f"断电截断完成：drop={drop_bytes}B，reader 暂未报告异常"
        if lost_records_after_cut > 0:
            msg += f"，完整帧 {records_before}->{records_after_cut}（丢{lost_records_after_cut}条）"

        self.powercut_case = {
            "active": True,
            "stage": "await_resume" if detected else "undetected",
            "updated_at": datetime.now().strftime("%H:%M:%S"),
            "drop_bytes": int(drop_bytes),
            "before_bytes": int(size),
            "after_bytes": int(size - drop_bytes),
            "repair_before_bytes": 0,
            "repair_after_bytes": 0,
            "repair_drop_bytes": 0,
            "records_before": int(records_before),
            "records_after_powercut": int(records_after_cut),
            "records_after_repair": 0,
            "frame_err_after_powercut": frame_err_after_cut,
            "reader_tail": tail,
            "tail_hex_before": " ".join(f"{b:02x}" for b in before_tail),
            "tail_hex_after": " ".join(f"{b:02x}" for b in after_tail),
            "tail_hex_dropped": " ".join(f"{b:02x}" for b in dropped),
        }
        self.powercut_result = msg
        self.control_pause = True
        self.powercut_freeze = True
        self._push_timeline_mark(
            "powercut",
            f"断电截断 drop={drop_bytes}B, detected={1 if detected else 0}, rec {records_before}->{records_after_cut}",
            event_index=max(0, records_after_cut),
            uptime=self.state.get("uptime_text", "--"),
        )
        if detected:
            self._push_incident_action("断电模拟: 已检测到截断；点击“继续设备”会先自动恢复再继续")
        else:
            self._push_incident_action("断电模拟: reader 暂未识别异常；点击“继续设备”仍会执行自动恢复检查")
        self._save_control_state()
        self._refresh_file_meta()
        self._decode_current()
        self._refresh_runtime_snapshot()
        return True, f"{msg}；设备已进入断电保持，点击“继续设备”将自动恢复后继续"

    def repair_powercut(self, source: str = "manual", pause_after: bool = True) -> Tuple[bool, str]:
        log_path = Path(self.args.log)
        if not log_path.exists():
            return False, "log not exists"

        before = int(log_path.stat().st_size)
        # 直接复用 reader 的修复路径，保证看板演示与真实恢复实现一致。
        cmd = [
            self.args.read_bin,
            "--shared",
            self.args.shared,
            "--log",
            self.args.log,
            "--repair-tail",
            "--format",
            "table",
            "--limit",
            "1",
        ]
        cp = subprocess.run(cmd, text=True, capture_output=True, check=False)
        after = int(log_path.stat().st_size) if log_path.exists() else 0

        msg_lines = [x for x in ((cp.stderr or "") + "\n" + (cp.stdout or "")).splitlines() if x.strip()]
        tail = msg_lines[-1].strip() if msg_lines else "repair finished"
        if cp.returncode != 0:
            msg = f"恢复失败：rc={cp.returncode}，detail={tail}"
            self.powercut_result = msg
            self.powercut_case["active"] = True
            self.powercut_case["stage"] = "repair_failed"
            self.powercut_case["updated_at"] = datetime.now().strftime("%H:%M:%S")
            self.powercut_case["reader_tail"] = tail
            self.powercut_case["records_after_repair"] = int(self.last_index)
            fail_label = "继续时自动" if source == "auto_resume" else "手动"
            self._push_timeline_mark(
                "repair_fail",
                f"{fail_label}恢复失败 rc={cp.returncode}",
                event_index=max(0, self.last_index),
                uptime=self.state.get("uptime_text", "--"),
            )
            self._push_incident_action("断电恢复失败: 请查看 reader 错误信息")
            self._refresh_file_meta()
            self._decode_current()
            self._refresh_runtime_snapshot()
            return False, msg

        drop = max(0, before - after)
        self.powercut_case["active"] = False
        if source == "auto_resume":
            self.powercut_case["stage"] = "auto_resume_repaired" if drop > 0 else "auto_resume_clean"
        else:
            self.powercut_case["stage"] = "repaired" if drop > 0 else "clean"
        self.powercut_case["updated_at"] = datetime.now().strftime("%H:%M:%S")
        self.powercut_case["repair_before_bytes"] = before
        self.powercut_case["repair_after_bytes"] = after
        self.powercut_case["repair_drop_bytes"] = drop
        self.powercut_case["reader_tail"] = tail

        msg = f"恢复完成：{before}->{after}，裁剪={drop}B"
        self.powercut_result = msg
        self.control_pause = bool(pause_after)
        self.powercut_freeze = False
        self._save_control_state()
        self._refresh_file_meta()
        self._decode_current()
        self.powercut_case["records_after_repair"] = int(self.last_index)
        records_before = int(self.powercut_case.get("records_before", 0))
        records_after_repair = int(self.powercut_case.get("records_after_repair", 0))
        records_lost_total = max(0, records_before - records_after_repair) if records_before > 0 else 0
        repair_label = "继续时自动" if source == "auto_resume" else "手动"
        self._push_timeline_mark(
            "repair",
            f"{repair_label}恢复完成 drop={drop}B, rec {records_before}->{records_after_repair}",
            event_index=max(0, self.last_index),
            uptime=self.state.get("uptime_text", "--"),
        )
        self._push_incident_action(
            f"{repair_label}恢复完成: 修复后 {before} -> {after} 字节，丢弃 {drop} 字节截断尾部"
        )
        msg = (
            f"恢复完成：{before}->{after}，裁剪={drop}B"
            + (f"，完整帧 {records_before}->{records_after_repair}（丢{records_lost_total}条）" if records_before > 0 else "")
        )
        self.powercut_result = msg
        self._refresh_runtime_snapshot()
        return True, msg

    def get_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return copy.deepcopy(self.snapshot)


class Handler(BaseHTTPRequestHandler):
    model: LiveModel = None  # type: ignore

    def _write(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_json(self, code: int, obj: Dict[str, Any]) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self._write(code, body, "application/json; charset=utf-8")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._write(200, HTML_PAGE.encode("utf-8"), "text/html; charset=utf-8")
            return
        if parsed.path == "/api/snapshot":
            self._write_json(200, self.model.get_snapshot())
            return
        if parsed.path == "/api/decoded.jsonl":
            with self.model.lock:
                body = self.model.decode_cache.encode("utf-8")
            self._write(200, body, "text/plain; charset=utf-8")
            return
        self._write(404, b"not found", "text/plain; charset=utf-8")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path not in {"/api/inject", "/api/control", "/api/powercut", "/api/powercut_repair"}:
            self._write_json(404, {"ok": False, "error": "not found"})
            return

        length_raw = self.headers.get("Content-Length", "0").strip()
        try:
            length = int(length_raw)
        except ValueError:
            length = 0
        payload = self.rfile.read(max(0, length)) if length > 0 else b"{}"
        try:
            obj = json.loads(payload.decode("utf-8"))
        except Exception:
            self._write_json(400, {"ok": False, "error": "invalid json"})
            return

        with self.model.lock:
            if parsed.path == "/api/inject":
                action = str(obj.get("action", "")).strip().lower()
                ok, message = self.model.inject(action)
            elif parsed.path == "/api/control":
                action = str(obj.get("action", "")).strip().lower()
                interval_ms = -1
                if "interval_ms" in obj:
                    try:
                        interval_ms = int(obj.get("interval_ms", -1))
                    except (TypeError, ValueError):
                        interval_ms = -1
                ok, message = self.model.control_action(action, interval_ms=interval_ms)
            elif parsed.path == "/api/powercut":
                drop_bytes = 7
                if "drop_bytes" in obj:
                    try:
                        drop_bytes = int(obj.get("drop_bytes", 7))
                    except (TypeError, ValueError):
                        drop_bytes = 7
                ok, message = self.model.simulate_powercut(drop_bytes=drop_bytes)
            else:
                ok, message = self.model.repair_powercut()
        if ok:
            self._write_json(200, {"ok": True, "message": message})
        else:
            self._write_json(400, {"ok": False, "error": message})

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def monitor_loop(model: LiveModel) -> None:
    while not model.stop_event.is_set():
        model.refresh()
        model.stop_event.wait(max(0.05, model.args.poll_interval))


def main() -> int:
    args = parse_args()
    model = LiveModel(args)
    Handler.model = model

    monitor = threading.Thread(target=monitor_loop, args=(model,), daemon=True)
    monitor.start()

    class ReuseServer(ThreadingHTTPServer):
        allow_reuse_address = True

    server: ThreadingHTTPServer = ReuseServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}/"
    print(f"live_dashboard_started,{url}", flush=True)
    try:
        server.serve_forever(poll_interval=0.3)
    except KeyboardInterrupt:
        pass
    finally:
        model.stop_event.set()
        try:
            server.shutdown()
        except OSError:
            pass
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
