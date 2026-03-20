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
    :root {
      --bg: #f2f4f7;
      --bg-2: #eef2f7;
      --card: rgba(255, 255, 255, 0.78);
      --card-solid: #ffffff;
      --ink: #111827;
      --sub: #6b7280;
      --line: rgba(17, 24, 39, 0.08);
      --line-strong: rgba(17, 24, 39, 0.16);
      --accent: #0071e3;
      --accent-soft: rgba(0, 113, 227, 0.12);
      --warn: #b26a00;
      --danger: #c8312d;
      --ok: #168a45;
      --shadow: 0 22px 48px rgba(15, 23, 42, 0.09), 0 2px 8px rgba(15, 23, 42, 0.04);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "PingFang SC", "Helvetica Neue", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(1000px 560px at 105% -12%, rgba(0, 113, 227, 0.12) 0%, transparent 60%),
        radial-gradient(840px 520px at -15% 118%, rgba(99, 102, 241, 0.10) 0%, transparent 62%),
        var(--bg);
    }
    .wrap { max-width: 1480px; margin: 0 auto; padding: 20px 18px 34px; }
    .title {
      display: flex; align-items: end; justify-content: space-between; gap: 12px;
      margin-bottom: 16px;
    }
    h1 {
      margin: 0;
      font-size: 30px;
      font-weight: 660;
      letter-spacing: -.02em;
      font-family: "SF Pro Rounded", "SF Pro Display", -apple-system, "PingFang SC", sans-serif;
    }
    .subtitle { color: var(--sub); font-size: 13px; }

    .status {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }
    .pill {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 10px 12px;
      box-shadow: 0 4px 14px rgba(15, 23, 42, 0.04);
      backdrop-filter: blur(14px);
    }
    .pill .k { font-size: 12px; color: var(--sub); }
    .pill .v { font-size: 17px; font-weight: 680; margin-top: 4px; letter-spacing: -.01em; }

    .grid {
      display: grid;
      grid-template-columns: 1.35fr 1fr;
      gap: 12px;
    }

    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(16px);
    }
    .card h2 {
      display: flex;
      align-items: center;
      gap: 8px;
      font-family: "SF Pro Rounded", "SF Pro Display", -apple-system, "PingFang SC", sans-serif;
    }
    .card h2::before {
      content: "";
      width: 9px;
      height: 9px;
      border-radius: 999px;
      background: #6b7280;
      box-shadow: 0 0 0 3px rgba(107, 114, 128, 0.13);
      flex: 0 0 auto;
    }
    #card_state { border-color: rgba(0, 113, 227, 0.24); }
    #card_incident { border-color: rgba(178, 106, 0, 0.24); }
    #card_timeline { border-color: rgba(14, 116, 144, 0.22); }
    #card_log { border-color: rgba(22, 138, 69, 0.24); }
    #card_schema { border-color: rgba(71, 85, 105, 0.24); }
    #card_state h2 { color: #0058b3; }
    #card_incident h2 { color: #8f5400; }
    #card_timeline h2 { color: #0f5f74; }
    #card_log h2 { color: #166534; }
    #card_schema h2 { color: #334155; }
    #card_state h2::before { background: #0a84ff; box-shadow: 0 0 0 3px rgba(10, 132, 255, 0.14); }
    #card_incident h2::before { background: #f59e0b; box-shadow: 0 0 0 3px rgba(245, 158, 11, 0.14); }
    #card_timeline h2::before { background: #0ea5b7; box-shadow: 0 0 0 3px rgba(14, 165, 183, 0.14); }
    #card_log h2::before { background: #16a34a; box-shadow: 0 0 0 3px rgba(22, 163, 74, 0.14); }
    #card_schema h2::before { background: #64748b; box-shadow: 0 0 0 3px rgba(100, 116, 139, 0.14); }
    #card_state { min-height: 240px; }
    #card_incident { min-height: 260px; }
    #card_timeline { min-height: 500px; }
    #card_log { min-height: 520px; }
    #card_schema { min-height: 520px; }
    .card h2 { margin: 0 0 12px; font-size: 16px; font-weight: 640; letter-spacing: -.01em; }
    .kvs { display: grid; grid-template-columns: 124px 1fr; gap: 7px; font-size: 12px; }
    .kvs .k { color: var(--sub); }

    .bar {
      width: 100%; height: 10px; border-radius: 999px;
      background: #e5e9f0; border: 1px solid #d8dee8; overflow: hidden;
    }
    .bar > span {
      display: block; height: 100%;
      background: linear-gradient(90deg, #0071e3, #2d8cff);
    }

    .metrics { display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 10px; }
    .mono {
      font-family: "SF Mono", "Menlo", "Monaco", "Consolas", monospace;
      font-size: 12px;
    }
    .subtitle, .foot {
      font-family: "SF Pro Rounded", "SF Pro Text", -apple-system, "PingFang SC", sans-serif;
      line-height: 1.55;
      letter-spacing: .005em;
    }

    .controls {
      display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-bottom: 9px;
    }
    button, select, input[type="number"] {
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.92);
      border-radius: 12px;
      padding: 7px 10px;
      font-size: 12px;
      color: var(--ink);
      font-family: inherit;
      cursor: pointer;
      transition: transform .14s ease, box-shadow .16s ease, background-color .2s ease, border-color .2s ease;
    }
    input[type="number"] { cursor: text; }
    button:hover, select:hover, input[type="number"]:hover {
      transform: translateY(-1px);
      box-shadow: 0 8px 20px rgba(17, 24, 39, 0.12);
      border-color: var(--line-strong);
      background: #ffffff;
    }
    button:active {
      transform: translateY(0);
      box-shadow: inset 0 1px 2px rgba(17, 24, 39, 0.16);
    }
    button.done {
      background: rgba(22, 138, 69, 0.14);
      border-color: rgba(22, 138, 69, 0.36);
    }
    button.pending {
      opacity: .75;
      cursor: wait;
    }
    button.fail {
      background: rgba(200, 49, 45, 0.12);
      border-color: rgba(200, 49, 45, 0.36);
    }
    button:disabled {
      opacity: .7;
      cursor: wait;
    }
    button.primary {
      background: var(--accent-soft);
      border-color: rgba(0, 113, 227, 0.36);
      color: #004a95;
      font-weight: 700;
    }
    button.warn {
      background: rgba(178, 106, 0, 0.12);
      border-color: rgba(178, 106, 0, 0.26);
      color: #8e5100;
    }
    button.danger {
      background: rgba(200, 49, 45, 0.12);
      border-color: rgba(200, 49, 45, 0.30);
      color: #8e211e;
    }
    input[type="checkbox"] { accent-color: var(--accent); transform: translateY(1px); }
    label { color: var(--sub); font-size: 12px; }
    .toggle {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.72);
      border-radius: 999px;
      padding: 5px 10px;
    }

    .timeline {
      max-height: 390px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 13px;
      background: rgba(255, 255, 255, 0.90);
    }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { border-bottom: 1px solid rgba(17, 24, 39, 0.07); padding: 8px 9px; text-align: left; }
    th { position: sticky; top: 0; z-index: 1; background: #f8fafc; }
    tr.active-row td { background: rgba(0, 113, 227, 0.10); }
    tr.row-fault td { background: rgba(200, 49, 45, 0.10); }
    tr.row-diag td { background: rgba(178, 106, 0, 0.09); }
    tr.row-recover td { background: rgba(22, 138, 69, 0.10); }
    tr.row-powercut td { background: rgba(173, 50, 70, 0.10); }
    tr.row-repair td { background: rgba(0, 113, 227, 0.12); }
    tr.row-repair_fail td { background: rgba(200, 49, 45, 0.10); }

    .incident {
      border-left: 4px solid var(--ok);
      border-radius: 12px;
      padding: 10px 11px;
      background: rgba(22, 138, 69, 0.08);
      margin-bottom: 8px;
    }
    .incident.warn { border-left-color: var(--warn); background: rgba(178, 106, 0, 0.10); }
    .incident.danger { border-left-color: var(--danger); background: rgba(200, 49, 45, 0.10); }
    .incident .title { font-size: 13px; font-weight: 700; margin-bottom: 6px; }
    .incident ul { margin: 0; padding-left: 18px; font-size: 12px; }

    pre {
      margin: 0;
      background: rgba(255, 255, 255, 0.95);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px;
      max-height: 220px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
    }
    #shared_hex, #powercut_case_text, #shared_layout_text {
      font-family: "SF Mono", "Menlo", "Monaco", "Consolas", monospace;
      font-size: 11px;
      line-height: 1.5;
      letter-spacing: .015em;
    }
    #schema_text {
      font-family: "SF Pro Rounded", "SF Pro Text", -apple-system, "PingFang SC", sans-serif;
      font-size: 12px;
      line-height: 1.6;
      letter-spacing: .01em;
      color: #1f2937;
    }

    .split { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
    .map-list {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.94);
      max-height: 330px;
      overflow: auto;
      padding: 6px;
    }
    .map-row {
      padding: 6px 8px;
      border-radius: 7px;
      margin-bottom: 4px;
      border: 1px solid transparent;
      cursor: pointer;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: "SF Mono", "Menlo", "Monaco", "Consolas", monospace;
      font-size: 11px;
      line-height: 1.35;
      transition: background-color .15s ease, border-color .15s ease;
    }
    .map-row[data-side="dec"] {
      font-family: "SF Pro Rounded", "SF Pro Text", -apple-system, "PingFang SC", sans-serif;
      font-size: 12px;
      line-height: 1.5;
      letter-spacing: .005em;
    }
    .map-row[data-side="bin"] {
      font-family: "SF Mono", "Menlo", "Monaco", "Consolas", monospace;
      font-size: 11px;
      line-height: 1.45;
      letter-spacing: .015em;
    }
    .map-row:hover {
      background: #f3f6fb;
      border-color: #cfd8e6;
    }
    .map-row.active {
      background: rgba(0, 113, 227, 0.10);
      border-color: rgba(0, 113, 227, 0.34);
    }
    .map-row.locked {
      background: rgba(0, 113, 227, 0.16);
      border-color: rgba(0, 113, 227, 0.52);
    }
    .map-row.head {
      font-weight: 700;
      color: #3e3934;
      cursor: default;
    }
    .map-row.anomaly-fault { border-color: rgba(200, 49, 45, 0.34); background: rgba(200, 49, 45, 0.10); }
    .map-row.anomaly-diag { border-color: rgba(178, 106, 0, 0.34); background: rgba(178, 106, 0, 0.08); }
    .map-row.anomaly-recover { border-color: rgba(22, 138, 69, 0.34); background: rgba(22, 138, 69, 0.10); }
    .map-row.anomaly-powercut { border-color: rgba(173, 50, 70, 0.34); background: rgba(173, 50, 70, 0.10); }
    .map-row.anomaly-repair { border-color: rgba(0, 113, 227, 0.36); background: rgba(0, 113, 227, 0.10); }
    .map-row.anomaly-repair_fail { border-color: rgba(200, 49, 45, 0.34); background: rgba(200, 49, 45, 0.10); }

    .mark-strip {
      border: 1px dashed #cbd5e1;
      background: rgba(248, 250, 252, 0.92);
      border-radius: 12px;
      padding: 6px;
      margin-bottom: 8px;
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      min-height: 38px;
      align-items: center;
    }
    .mark {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      border: 1px solid #d6dee9;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 11px;
      background: #ffffff;
    }

    .badge {
      display: inline-block;
      border-radius: 999px;
      padding: 1px 7px;
      font-size: 11px;
      line-height: 1.4;
      border: 1px solid #d7deea;
      background: #f4f7fb;
      color: #344054;
      white-space: nowrap;
    }
    .badge-fault { background: rgba(200, 49, 45, 0.10); border-color: rgba(200, 49, 45, 0.34); color: #9d2120; }
    .badge-diag { background: rgba(178, 106, 0, 0.10); border-color: rgba(178, 106, 0, 0.32); color: #8f5400; }
    .badge-recover { background: rgba(22, 138, 69, 0.10); border-color: rgba(22, 138, 69, 0.32); color: #176e3d; }
    .badge-powercut { background: rgba(173, 50, 70, 0.10); border-color: rgba(173, 50, 70, 0.34); color: #8c2a3b; }
    .badge-repair { background: rgba(0, 113, 227, 0.10); border-color: rgba(0, 113, 227, 0.34); color: #0058b3; }
    .badge-repair_fail { background: rgba(200, 49, 45, 0.10); border-color: rgba(200, 49, 45, 0.34); color: #9d2120; }
    .badge-control { background: #eef2f8; border-color: #c8d2e2; color: #374151; }

    .powercut-box {
      margin-top: 8px;
      padding: 8px;
      border: 1px solid #d0dbe9;
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.95);
    }
    .foot { margin-top: 8px; color: var(--sub); font-size: 12px; }
    .foot.ok { color: var(--ok); }
    .foot.err { color: var(--danger); }

    @media (max-width: 1100px) {
      .status { grid-template-columns: repeat(3, minmax(0,1fr)); }
      .grid { grid-template-columns: 1fr; }
      .split { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="title">
      <div>
        <h1>Optbinlog 运行中可视化看板</h1>
        <div class="subtitle">Embedded Runtime · 实时流程 · 异常闭环 · 二进制与可读联动</div>
      </div>
      <div id="server_time" class="subtitle">--</div>
    </div>

    <div class="status">
      <div class="pill"><div class="k">当前阶段</div><div class="v" id="phase">--</div></div>
      <div class="pill"><div class="k">记录条数</div><div class="v" id="records">0</div></div>
      <div class="pill"><div class="k">异常状态</div><div class="v" id="incident_state">--</div></div>
      <div class="pill"><div class="k">日志大小(binary)</div><div class="v" id="log_size">0</div></div>
      <div class="pill"><div class="k">估算 text 大小</div><div class="v" id="text_size">0</div></div>
      <div class="pill"><div class="k">空间比 text/binary</div><div class="v" id="space_ratio">--</div></div>
    </div>

    <div class="grid">
      <div class="card" id="card_state">
        <h2>设备状态 · 优化指标</h2>
        <div class="metrics">
          <div>
            <div class="kvs"><div class="k">Boot</div><div id="boot">--</div></div>
            <div class="kvs"><div class="k">Network</div><div id="net">--</div></div>
            <div class="kvs"><div class="k">Uptime</div><div id="uptime">--</div></div>
            <div class="kvs"><div class="k">RSSI</div><div id="rssi">--</div></div>
            <div class="kvs"><div class="k">吞吐(records/s)</div><div id="throughput">--</div></div>
            <div class="kvs"><div class="k">解码耗时(ms)</div><div id="decode_ms">--</div></div>
          </div>
          <div>
            <div class="kvs"><div class="k">Battery</div><div id="soc">--</div></div>
            <div class="bar"><span id="soc_bar" style="width:0%"></span></div>
            <div class="kvs"><div class="k">Latency(us)</div><div id="latency">--</div></div>
            <div class="bar"><span id="latency_bar" style="width:0%"></span></div>
          </div>
        </div>
        <div class="foot" id="counter">--</div>
      </div>

      <div class="card" id="card_incident">
        <h2>异常注入 · 处理路径</h2>
        <div class="controls">
          <button class="danger" id="inject_fault">注入异常</button>
          <button class="warn" id="inject_diag">注入诊断重试</button>
          <button class="primary" id="inject_recover">注入恢复</button>
          <label>断电截断字节<input id="powercut_drop_bytes" type="number" min="1" step="1" value="7" style="width:72px" /></label>
          <button class="danger" id="simulate_powercut">模拟断电截断</button>
          <button class="primary" id="repair_powercut">手动恢复(备用)</button>
        </div>
        <div id="action_feedback" class="foot">操作反馈：--</div>
        <div id="inject_result" class="foot">注入结果：--</div>
        <div id="powercut_result" class="foot">断电影响：--</div>
        <div id="powercut_case_box" class="foot mono">断电恢复状态：--</div>
        <div id="incident_box" class="incident">
          <div class="title">等待异常事件...</div>
          <ul><li>尚未检测到告警</li></ul>
        </div>
        <div class="foot mono" id="plan">--</div>
      </div>

      <div class="card" id="card_timeline">
        <h2>事件时间线 · 自动/逐步</h2>
        <div class="controls">
          <button id="toggle_refresh">暂停刷新</button>
          <label>刷新频率
            <select id="refresh_ms">
              <option value="250">250ms</option>
              <option value="400">400ms</option>
              <option value="800" selected>800ms</option>
              <option value="1200">1200ms</option>
            </select>
          </label>
          <label>查看模式
            <select id="view_mode">
              <option value="auto" selected>自动跟随</option>
              <option value="step">逐步查看</option>
            </select>
          </label>
          <button id="step_prev">上一步</button>
          <button id="step_next">下一步</button>
          <span class="mono" id="step_info">step=--</span>
        </div>
        <div class="controls">
          <button class="danger" id="device_pause">暂停设备</button>
          <button class="primary" id="device_resume">继续设备</button>
          <button class="warn" id="device_step">设备单步(1事件)</button>
          <label>设备间隔(ms)<input id="device_interval" type="number" min="0" step="10" value="180" style="width:88px" /></label>
          <button id="device_interval_apply">应用间隔</button>
          <span class="mono" id="device_state">device=--</span>
        </div>
        <div class="foot">
          用法：先点“暂停设备”，再用“设备单步(1事件)”推进；讲完后点“继续设备”恢复自动运行。设备间隔(ms)=每写入1条日志后的等待时间。
        </div>
        <div class="controls">
          <label>Tag 过滤
            <select id="tag_filter"><option value="">全部</option></select>
          </label>
          <a class="mono" href="/api/decoded.jsonl" target="_blank" rel="noopener">下载当前 JSONL</a>
        </div>
        <div id="timeline_marks" class="mark-strip"><span class="mono">等待标注...</span></div>
        <div class="timeline" id="timeline_box">
          <table>
            <thead>
              <tr><th>#</th><th>uptime</th><th>tag</th><th>标注</th><th>summary</th></tr>
            </thead>
            <tbody id="timeline"></tbody>
          </table>
        </div>
      </div>

      <div class="card" id="card_log">
        <h2>二进制日志 ↔ 可读日志</h2>
        <div class="split">
          <div>
            <div class="kvs">
              <div class="k">shared.bin(head)</div><div class="mono" id="shared_head_meta">--</div>
              <div class="k">run.bin(tail)</div><div class="mono" id="log_tail_meta">--</div>
            </div>
            <pre class="mono" id="shared_hex">--</pre>
            <div style="height:8px"></div>
            <div class="map-list" id="binary_map_list">--</div>
          </div>
          <div>
            <div class="kvs">
              <div class="k">解析日志(单一格式)</div><div class="mono" id="readable_meta">--</div>
            </div>
            <div class="map-list" id="decoded_map_list">--</div>
            <div class="foot mono" id="frame_parse_error">--</div>
            <div class="powercut-box">
              <div class="kvs">
                <div class="k">断电/恢复可视化</div><div class="mono" id="powercut_case_meta">--</div>
              </div>
              <pre class="mono" id="powercut_case_text">--</pre>
            </div>
          </div>
        </div>
      </div>

      <div class="card" id="card_schema">
        <h2>共享日志格式 · Schema</h2>
        <div class="kvs">
          <div class="k">源文件</div><div class="mono" id="schema_src">--</div>
          <div class="k">最近解码</div><div class="mono" id="decode_state">--</div>
        </div>
        <pre class="mono" id="shared_layout_text">--</pre>
        <div style="height:8px"></div>
        <pre class="mono" id="schema_text">--</pre>
      </div>
    </div>
  </div>

  <script>
    let paused = false;
    let timer = null;
    let stepCursor = -1;
    let lastTagSig = '';
    let mapHoverIndex = -1;
    let mapLockIndex = -1;
    let mapDefaultIndex = -1;

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
      const el = document.getElementById('action_feedback');
      el.textContent = `操作反馈：${nowShort()} ${text}`;
      el.classList.remove('ok', 'err');
      el.classList.add(ok ? 'ok' : 'err');
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
      rows.forEach((el) => {
        el.classList.remove('active', 'locked');
      });
      const idx = mapHoverIndex >= 0 ? mapHoverIndex : (mapLockIndex >= 0 ? mapLockIndex : mapDefaultIndex);
      if (idx < 0) return;
      const targets = document.querySelectorAll(`.map-row[data-idx="${idx}"]`);
      targets.forEach((el) => {
        el.classList.add('active');
        if (mapLockIndex === idx) el.classList.add('locked');
      });
    }

    function renderFramePairs(data) {
      const pairs = data.runtime.frame_pairs || [];
      const binaryRoot = document.getElementById('binary_map_list');
      const decodedRoot = document.getElementById('decoded_map_list');

      if (!pairs.length) {
        binaryRoot.innerHTML = '<div class="map-row">暂无二进制帧</div>';
        decodedRoot.innerHTML = '<div class="map-row">暂无可读日志</div>';
        mapHoverIndex = -1;
        mapLockIndex = -1;
        mapDefaultIndex = -1;
        return;
      }

      mapDefaultIndex = Number(pairs[pairs.length - 1].index || -1);
      const idxSet = new Set(pairs.map((x) => Number(x.index || -1)));
      if (!idxSet.has(mapLockIndex)) mapLockIndex = -1;

      const binaryRows = [
        '<div class="map-row head">Binary Frame Segment (hover/click 联动)</div>',
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
        document.getElementById('timeline').innerHTML = '<tr><td colspan="5">暂无事件</td></tr>';
        return;
      }
      const html = picked.rows
        .map((e, i) => {
          const rowCls = [];
          if (i === picked.activeIndex) rowCls.push('active-row');
          if (e.anomaly) rowCls.push(anomalyRowClass(e.anomaly));
          const cls = rowCls.length ? ` class="${rowCls.join(' ')}"` : '';
          const badge = anomalyBadge(String(e.anomaly || ''));
          return `<tr${cls}><td>${e.index}</td><td>${e.uptime}</td><td>${e.tag}</td><td>${badge}</td><td>${e.summary}</td></tr>`;
        })
        .join('');
      document.getElementById('timeline').innerHTML = html;
      if (document.getElementById('view_mode').value === 'auto') {
        const box = document.getElementById('timeline_box');
        box.scrollTop = box.scrollHeight;
      }
    }

    function render(data) {
      document.getElementById('server_time').textContent = `服务器时间: ${data.server_time || '--'}`;
      document.getElementById('phase').textContent = data.runtime.phase || '--';
      document.getElementById('records').textContent = String(data.runtime.records_total || 0);
      document.getElementById('log_size').textContent = fmtBytes(data.files.log_size || 0);
      document.getElementById('text_size').textContent = fmtBytes(data.runtime.optimization?.text_est_bytes || 0);
      document.getElementById('space_ratio').textContent = (data.runtime.optimization?.space_ratio_text_div_binary || 0).toFixed(2) + 'x';

      const st = data.runtime.state || {};
      document.getElementById('boot').textContent = st.boot_stage || '--';
      document.getElementById('net').textContent = st.net_state || '--';
      document.getElementById('uptime').textContent = st.uptime_text || '--';
      document.getElementById('rssi').textContent = String(st.rssi_dbm ?? '--');
      document.getElementById('soc').textContent = `${Number(st.soc || 0).toFixed(1)} %`;
      document.getElementById('latency').textContent = String(st.latency_us ?? '--');
      document.getElementById('soc_bar').style.width = pct(st.soc || 0, 0, 100) + '%';
      document.getElementById('latency_bar').style.width = pct(st.latency_us || 0, 250, 1200) + '%';

      const opt = data.runtime.optimization || {};
      document.getElementById('throughput').textContent = Number(opt.records_per_sec || 0).toFixed(2);
      document.getElementById('decode_ms').textContent = Number(opt.last_decode_ms || 0).toFixed(2);

      const c = data.runtime.counters || {};
      document.getElementById('counter').textContent =
        `boot=${c.boot_stage || 0} sensor=${c.sensor_sample || 0} control=${c.control_loop || 0} net=${c.net_state || 0} power=${c.power_state || 0} alert=${c.alert_event || 0} note=${c.note_event || 0}`;

      renderIncident(data.runtime.incident || {});
      document.getElementById('plan').textContent =
        `默认注入参数: fault_at_cycle=${data.config.fault_at_cycle}, recover_at_cycle=${data.config.recover_at_cycle} | 手动注入次数=${data.runtime.manual_inject_count || 0}`;
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

      document.getElementById('shared_head_meta').textContent =
        `size=${fmtBytes(data.files.shared_size || 0)} mtime=${data.files.shared_mtime || '--'}`;
      document.getElementById('log_tail_meta').textContent =
        `size=${fmtBytes(data.files.log_size || 0)} mtime=${data.files.log_mtime || '--'}`;
      document.getElementById('shared_hex').textContent = data.files.shared_hex_head || '--';

      document.getElementById('readable_meta').textContent =
        `frame_rows=${(data.runtime.frame_pairs || []).length}`;
      renderFramePairs(data);
      document.getElementById('frame_parse_error').textContent =
        `frame_parse_error=${data.runtime.frame_parse_error || '(none)'}`;
      document.getElementById('powercut_result').textContent =
        `断电影响：${data.runtime.powercut_result || '--'}`;
      const p = data.runtime.powercut_case || {};
      const pStage = p.stage || 'idle';
      document.getElementById('powercut_case_box').textContent =
        `断电恢复状态：${powercutStageLabel(pStage)} | drop=${p.drop_bytes || 0}B | repair_drop=${p.repair_drop_bytes || 0}B`;
      document.getElementById('powercut_case_meta').textContent =
        `updated=${p.updated_at || '--'} before=${p.before_bytes || 0} after=${p.after_bytes || 0}`;
      document.getElementById('powercut_case_text').textContent =
        `stage: ${powercutStageLabel(pStage)}\n\n` +
        `tail_before:\n${p.tail_hex_before || '--'}\n\n` +
        `tail_dropped:\n${p.tail_hex_dropped || '--'}\n\n` +
        `tail_after:\n${p.tail_hex_after || '--'}\n\n` +
        `reader:\n${p.reader_tail || '--'}`;

      document.getElementById('schema_src').textContent = data.schema.source_path || '--';
      document.getElementById('schema_text').textContent = data.schema.source_text || '--';
      document.getElementById('shared_layout_text').textContent = data.schema.layout_text || '--';
      document.getElementById('decode_state').textContent =
        `decode_ok=${data.runtime.decode_ok} rc=${data.runtime.decode_rc} at=${data.runtime.last_decode_at || '--'}`;

      const tags = Array.from(new Set((data.runtime.full_events || []).map((e) => e.tag))).sort();
      updateTagFilter(tags);
      renderTimelineMarks(data);
      renderTimeline(data);
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
        if (!r.ok) return;
        const data = await r.json();
        render(data);
      } catch (_) {
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
      document.getElementById('toggle_refresh').textContent = paused ? '恢复刷新' : '暂停刷新';
      if (!paused) tick();
    });

    document.getElementById('refresh_ms').addEventListener('change', restartTimer);
    document.getElementById('tag_filter').addEventListener('change', () => tick());

    document.getElementById('view_mode').addEventListener('change', () => {
      if (document.getElementById('view_mode').value === 'auto') {
        stepCursor = -1;
        paused = false;
        document.getElementById('toggle_refresh').textContent = '暂停刷新';
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

    document.getElementById('inject_fault').addEventListener('click', () => callInject('fault', 'inject_fault'));
    document.getElementById('inject_diag').addEventListener('click', () => callInject('diag', 'inject_diag'));
    document.getElementById('inject_recover').addEventListener('click', () => callInject('recover', 'inject_recover'));
    document.getElementById('device_pause').addEventListener('click', () => callControl('pause', null, 'device_pause'));
    document.getElementById('device_resume').addEventListener('click', () => callControl('resume', null, 'device_resume'));
    document.getElementById('device_step').addEventListener('click', () => callControl('step', null, 'device_step'));
    document.getElementById('device_interval_apply').addEventListener('click', () => {
      const ms = document.getElementById('device_interval').value;
      callControl('set_interval', ms, 'device_interval_apply');
    });
    document.getElementById('device_interval').addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter') {
        const ms = document.getElementById('device_interval').value;
        callControl('set_interval', ms, 'device_interval_apply');
      }
    });
    document.getElementById('simulate_powercut').addEventListener('click', () => callPowercut('simulate_powercut'));
    document.getElementById('repair_powercut').addEventListener('click', () => callPowercutRepair('repair_powercut'));

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
                "shared_hex_head": "",
                "log_hex_tail": "",
            },
            "schema": {
                "source_path": str(Path(args.schema_source).resolve()),
                "source_text": "",
                "layout_text": "",
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
            try:
                with shared.open("rb") as f:
                    head = f.read(80)
                files["shared_hex_head"] = " ".join(f"{b:02x}" for b in head) if head else "(empty shared)"
            except OSError as e:
                files["shared_hex_head"] = f"read error: {e}"
        else:
            files["shared_size"] = 0
            files["shared_mtime"] = "-"
            files["shared_hex_head"] = "(shared file not created yet)"

        if log.exists():
            st = log.stat()
            files["log_size"] = st.st_size
            files["log_mtime"] = datetime.fromtimestamp(st.st_mtime).strftime("%H:%M:%S")
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
            files["log_hex_tail"] = "(log file not created yet)"

    def _refresh_schema_text(self) -> None:
        p = Path(self.args.schema_source)
        if not p.exists():
            self.schema_text = "(schema source missing)"
            self.shared_layout_text = "(shared layout unavailable)"
            return
        try:
            self.schema_text = p.read_text(encoding="utf-8")
        except OSError as e:
            self.schema_text = f"(schema read error: {e})"
            self.shared_layout_text = "(shared layout unavailable)"
            return
        self.shared_layout_text = parse_shared_layout(self.args.shared)

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
        runtime["decode_error"] = (cp.stderr or "").strip()[-300:] if cp.returncode != 0 else ""
        runtime["last_decode_at"] = datetime.now().strftime("%H:%M:%S")

        if cp.returncode != 0:
            frames, frame_err = parse_binlog_frames(self.args.log)
            self.frame_parse_error = frame_err
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

        if not parsed:
            self.frame_pairs = build_frame_pairs(frames, [], frame_err=frame_err)
            self.frame_mapping_tail = build_frame_mapping_text(frames, [], tail=20)
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

        self.last_index = max(self.last_index, max_idx)
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

            log_size = int(self.snapshot["files"].get("log_size", 0))
            if log_size > 0 and Path(self.args.shared).exists():
                self._decode_current()
            else:
                runtime = self.snapshot["runtime"]
                runtime["decode_ok"] = False
                runtime["decode_rc"] = 1
                runtime["decode_error"] = "waiting for first stream frame"
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
        err = (cp.stderr or cp.stdout or "").strip().splitlines()
        tail = err[-1] if err else ("ok" if cp.returncode == 0 else "decode failed")
        detected = cp.returncode != 0
        if detected:
            msg = f"断电截断完成：drop={drop_bytes}B，reader 已检测到尾部异常"
        else:
            msg = f"断电截断完成：drop={drop_bytes}B，reader 暂未报告异常"

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
            f"断电截断 drop={drop_bytes}B, detected={1 if detected else 0}",
            event_index=max(0, self.last_index),
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
        repair_label = "继续时自动" if source == "auto_resume" else "手动"
        self._push_timeline_mark(
            "repair",
            f"{repair_label}恢复完成 drop={drop}B",
            event_index=max(0, self.last_index),
            uptime=self.state.get("uptime_text", "--"),
        )
        self._push_incident_action(
            f"{repair_label}恢复完成: 修复后 {before} -> {after} 字节，丢弃 {drop} 字节截断尾部"
        )
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
