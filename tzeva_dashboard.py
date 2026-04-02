from __future__ import annotations

import json
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from flask import Flask, Response, jsonify, request, stream_with_context

app = Flask(__name__)
TZ = ZoneInfo("Asia/Jerusalem")

DATA_URL = "https://www.tzevaadom.co.il/static/historical/all.json"
SNAPSHOT_URL = "https://iwm.diskin.net/api/snapshot"
LOCAL_ZONE_SOURCE = Path("alert-zones-local.json")

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
DATA_FILE = CACHE_DIR / "all.json"
META_FILE = CACHE_DIR / "meta.json"

REFRESH_SECONDS = 600
STREAM_POLL_SECONDS = 1
LIVE_REFRESH_SECONDS = 2
DEFAULT_THREAT_TYPES = {0}
ZONE_NAME_ALIASES = {
    "תל אביב מרכז העיר": "תל אביב - מרכז העיר",
    "תל אביב עבר הירקון": "תל אביב - עבר הירקון",
    "תל אביב-מרכז העיר": "תל אביב - מרכז העיר",
    "תל אביב-עבר הירקון": "תל אביב - עבר הירקון",
}

CITY_COORDS: dict[str, tuple[float, float]] = {
    "אשקלון": (31.6688, 34.5743),
    "אשדוד": (31.8014, 34.6435),
    "באר שבע": (31.2520, 34.7915),
    "חולון": (32.0158, 34.7874),
    "בת ים": (32.0236, 34.7503),
    "ראשון לציון": (31.9730, 34.7925),
    "ירושלים": (31.7683, 35.2137),
    "חיפה": (32.7940, 34.9896),
    "קריית שמונה": (33.2073, 35.5708),
    "מטולה": (33.2796, 35.5795),
    "נהריה": (33.0059, 35.0941),
    "שדרות": (31.5224, 34.5953),
    "נתיבות": (31.4231, 34.5891),
    "אופקים": (31.3141, 34.6203),
    "גוש דן": (32.0600, 34.8000),
    "תל אביב - מרכז העיר": (32.0853, 34.7818),
    "תל אביב - עבר הירקון": (32.1133, 34.8044),
    "עוטף עזה": (31.4300, 34.5000),
    "מרכז הנגב": (31.1000, 34.9000),
}

CITY_ALIASES = {
    "Ashkelon": "אשקלון",
    "Ashdod": "אשדוד",
    "Beer Sheva": "באר שבע",
    "Holon": "חולון",
    "Bat Yam": "בת ים",
    "Rishon LeZion": "ראשון לציון",
    "Jerusalem": "ירושלים",
    "Haifa": "חיפה",
}

WEEKDAY_NAMES_HE = {
    "0": "יום שני",
    "1": "יום שלישי",
    "2": "יום רביעי",
    "3": "יום חמישי",
    "4": "יום שישי",
    "5": "שבת",
    "6": "יום ראשון",
}

HTML = r'''<!doctype html>
<html lang="he" dir="rtl">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Iron Monitor Live</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    :root {
      --bg:#0a101b;
      --panel:#131b2b;
      --panel2:#182237;
      --line:#2b3b5c;
      --text:#eef3ff;
      --muted:#9db0d6;
      --radius:20px;
      --shadow:0 10px 28px rgba(0,0,0,.3);
    }
    * { box-sizing: border-box; }
    body {
      margin:0;
      font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      background:linear-gradient(180deg,#09111d,#0d1526);
      color:var(--text);
    }
    .wrap { max-width:1550px; margin:0 auto; padding:20px; }
    .grid { display:grid; grid-template-columns:1.1fr .9fr; gap:20px; }
    .stack { display:grid; gap:20px; }
    .card {
      background:rgba(19,27,43,.94);
      border:1px solid rgba(255,255,255,.08);
      border-radius:var(--radius);
      box-shadow:var(--shadow);
      padding:18px;
      backdrop-filter: blur(10px);
    }
    .title { font-size:30px; font-weight:800; margin-bottom:10px; }
    .sub { color:#9db0d6; line-height:1.5; font-size:14px; }
    .controls {
      display:grid;
      grid-template-columns:1.4fr 1fr 1fr 1fr auto;
      gap:12px;
      align-items:end;
    }
    label { display:block; font-size:13px; color:#9db0d6; margin-bottom:8px; }
    input, select, button {
      width:100%;
      background:#182237;
      color:#eef3ff;
      border:1px solid #2b3b5c;
      border-radius:14px;
      padding:12px 14px;
      font-size:14px;
      direction:rtl;
      text-align:right;
    }
    button { cursor:pointer; text-align:center; font-weight:700; }
    .btn-primary { background:linear-gradient(135deg,#4f7cff,#7aa2ff); border:none; }
    .btn-secondary { background:linear-gradient(135deg,#31405f,#233250); }
    .stats { display:grid; grid-template-columns:repeat(4, 1fr); gap:12px; }
    .stat {
      background:linear-gradient(180deg,rgba(255,255,255,.05),rgba(255,255,255,.02));
      border:1px solid rgba(255,255,255,.08);
      border-radius:18px;
      padding:16px;
    }
    .stat .k { color:#9db0d6; font-size:12px; margin-bottom:8px; }
    .stat .v { font-size:30px; font-weight:800; }
    .stat .s { margin-top:8px; font-size:12px; color:#9db0d6; }
    .section-head { display:flex; justify-content:space-between; align-items:center; margin-bottom:12px; gap:10px; }
    .small { font-size:12px; color:#9db0d6; }
    #map { height:520px; border-radius:18px; overflow:hidden; border:1px solid rgba(255,255,255,.08); }
    .list { display:flex; flex-direction:column; gap:10px; max-height:420px; overflow:auto; }
    .row {
      display:flex; justify-content:space-between; gap:10px; align-items:center;
      padding:14px; border-radius:16px; background:rgba(255,255,255,.04); border:1px solid rgba(255,255,255,.06);
    }
    .row .name { font-weight:700; }
    .row .meta { font-size:12px; color:#9db0d6; margin-top:4px; }
    .badge { padding:8px 10px; border-radius:999px; background:rgba(111,160,255,.16); color:#c8d8ff; font-size:12px; white-space:nowrap; }
    .legend { display:flex; gap:10px; flex-wrap:wrap; margin-top:10px; }
    .pill { padding:7px 10px; border-radius:999px; font-size:12px; border:1px solid rgba(255,255,255,.08); background:rgba(255,255,255,.04); }
    .dot { display:inline-block; width:10px; height:10px; border-radius:50%; margin-left:6px; }
    .dot.red { background:#ff5c5c; }
    .dot.yellow { background:#ffd166; }
    .dot.purple { background:#b56cff; }
    .dot.green { background:#3ddc97; }
    @media (max-width:1150px) {
      .grid { grid-template-columns:1fr; }
      .controls, .stats { grid-template-columns:1fr; }
      #map { height:420px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card" style="margin-bottom:20px;">
      <div class="title">🚨 Iron Monitor Live</div>
      <div class="sub">
        מפת לייב ארצית נפרדת מהחיפוש. החיפוש משפיע רק על הסטטיסטיקה. פוליגונים מופיעים רק באירועי לייב. אין ציור התחלתי של כל האזורים. הסטטיסטיקה מחושבת לפי בסיס הנתונים ההיסטורי בלבד, והלייב מוצג בנפרד.
      </div>
      <div class="small" id="datasetMeta" style="margin-top:10px;">טוען נתונים...</div>
      <div class="small" id="liveStatus" style="margin-top:6px;">מתחבר לשידור חי...</div>
    </div>

    <div class="card" style="margin-bottom:20px;">
      <div class="controls">
        <div>
          <label for="citySelect">עיר / אזור</label>
          <input list="citiesList" id="citySelect" placeholder="חולון / אשדוד / באר שבע ...">
          <datalist id="citiesList"></datalist>
        </div>
        <div>
          <label for="fromDate">מתאריך</label>
          <input id="fromDate" type="date">
        </div>
        <div>
          <label for="toDate">עד תאריך</label>
          <input id="toDate" type="date">
        </div>
        <div>
          <label for="preset">טווח מהיר</label>
          <select id="preset">
            <option value="7">7 ימים אחרונים</option>
            <option value="30" selected>30 ימים אחרונים</option>
            <option value="90">90 ימים אחרונים</option>
            <option value="365">שנה אחרונה</option>
            <option value="all">כל הבסיס</option>
          </select>
        </div>
        <div style="display:flex; gap:10px; align-items:end;">
          <button class="btn-primary" id="applyBtn">הצג</button>
          <button class="btn-secondary" id="refreshBtn">רענן</button>
        </div>
      </div>
    </div>

    <div class="grid">
      <div class="stack">
        <div class="card">
          <div class="section-head">
            <h3>מפה</h3>
            <div class="small" id="mapCaption">—</div>
          </div>
          <div id="map"></div>
          <div class="legend">
            <div class="pill"><span class="dot red"></span>ירי טילים (קטגוריה 1)</div>
            <div class="pill"><span class="dot yellow"></span>כלי טיס (קטגוריה 2)</div>
            <div class="pill"><span class="dot green"></span>שידור חי מחובר</div>
          </div>
        </div>

        <div class="card">
          <div class="section-head">
            <h3>גרף לפי ימים</h3>
            <div class="small" id="chartCaption">—</div>
          </div>
          <canvas id="dailyChart" height="120"></canvas>
        </div>
      </div>

      <div class="stack">
        <div class="card">
          <h3 style="margin-bottom:14px;">סיכום</h3>
          <div class="stats">
            <div class="stat"><div class="k">היום</div><div class="v" id="statToday">—</div><div class="s" id="statTodaySub">—</div></div>
            <div class="stat"><div class="k">7 ימים אחרונים</div><div class="v" id="statWeek">—</div><div class="s" id="statWeekSub">—</div></div>
            <div class="stat"><div class="k">30 ימים אחרונים</div><div class="v" id="statMonth">—</div><div class="s" id="statMonthSub">—</div></div>
            <div class="stat"><div class="k">סה״כ בטווח</div><div class="v" id="statTotal">—</div><div class="s" id="statTotalSub">—</div></div>
          </div>
        </div>

        <div class="card">
          <div class="section-head">
            <h3>שעות שיא</h3>
            <div class="small" id="hourlyCaption">—</div>
          </div>
          <canvas id="hourlyChart" height="130"></canvas>
        </div>

        <div class="card">
          <h3 style="margin-bottom:14px;">אירועים אחרונים</h3>
          <div class="list" id="recentEventsList"></div>
        </div>

        <div class="card">
          <h3 style="margin-bottom:14px;">אומדן סטטיסטי</h3>
          <div class="list" id="predictionList"></div>
        </div>
      </div>
    </div>
  </div>

<script>
let datasetMeta = null;
let allCities = [];
let dailyChart = null;
let hourlyChart = null;
let map = null;
let stream = null;
let liveCountryLayer = null;

let activeAlertsMap = {}; // Хранит активные полигоны
let processedEventKeys = new Set(); // Хранит хеши обработанных событий, чтобы не мигать старыми
let activeAnimations = [];

let zoneIndex = {};
let zoneCentroids = {};
let hasFittedMap = false;

const fallbackCityCoords = {
  "אשקלון": [31.6688, 34.5743],
  "אשדוד": [31.8014, 34.6435],
  "באר שבע": [31.2520, 34.7915],
  "חולון": [32.0158, 34.7874],
  "בת ים": [32.0236, 34.7503],
  "ראשון לציון": [31.9730, 34.7925],
  "ירושלים": [31.7683, 35.2137],
  "חיפה": [32.7940, 34.9896],
  "קריית שמונה": [33.2073, 35.5708],
  "מטולה": [33.2796, 35.5795],
  "נהריה": [33.0059, 35.0941],
  "שדרות": [31.5224, 34.5953],
  "נתיבות": [31.4231, 34.5891],
  "אופקים": [31.3141, 34.6203],
  "גוש דן": [32.0600, 34.8000],
  "תל אביב - מרכז העיר": [32.0853, 34.7818],
  "תל אביב - עבר הירקון": [32.1133, 34.8044],
  "עוטף עזה": [31.4300, 34.5000],
  "מרכז הנגב": [31.1000, 34.9000]
};

function todayLocalISO() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function fmtNum(v) { return new Intl.NumberFormat('he-IL').format(v ?? 0); }
function setText(id, value) { document.getElementById(id).textContent = value; }
function parseISODate(dateStr) {
  const [y, m, d] = dateStr.split('-').map(Number);
  return new Date(y, m - 1, d, 12, 0, 0);
}
function shiftDays(dateStr, days) {
  const d = parseISODate(dateStr);
  d.setDate(d.getDate() + days);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

async function getJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(txt || `HTTP ${res.status}`);
  }
  return await res.json();
}

function ensureMap() {
  if (map) return;
  map = L.map('map', { zoomControl: true }).setView([31.6, 35.0], 7);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    maxZoom: 20,
    subdomains: 'abcd',
    attribution: '&copy; OpenStreetMap &copy; CARTO'
  }).addTo(map);
  liveCountryLayer = L.layerGroup().addTo(map);
  setTimeout(() => map.invalidateSize(), 300);
}

function initCountryMapView() {
  ensureMap();
  if (!hasFittedMap) {
    map.setView([31.6, 35.0], 7);
    hasFittedMap = true;
  }
  updateMapCaption();
}

function updateMapCaption() {
    const count = Object.keys(activeAlertsMap).length;
    setText('mapCaption', `לייב ארצי · איומים פעילים כרגע: ${count}`);
}

function clearLiveLayers() {
  if (!liveCountryLayer) return;
  liveCountryLayer.clearLayers();
  activeAlertsMap = {};
  processedEventKeys.clear();
  clearActiveAnimations();
  updateMapCaption();
}

function clearActiveAnimations() {
  if (!map) return;
  for (const layer of activeAnimations) {
    try { map.removeLayer(layer); } catch (e) {}
  }
  activeAnimations = [];
}

function fadeAndRemoveLayer(layer, durationMs = 1800, targetOpacity = 0) {
  if (!layer) return;
  const started = performance.now();
  const initialOpacity = typeof layer.options?.opacity === 'number' ? layer.options.opacity : 1;
  const initialFillOpacity = typeof layer.options?.fillOpacity === 'number' ? layer.options.fillOpacity : initialOpacity;

  function step(now) {
    const progress = Math.min((now - started) / durationMs, 1);
    const opacity = initialOpacity + (targetOpacity - initialOpacity) * progress;
    const fillOpacity = initialFillOpacity + (targetOpacity - initialFillOpacity) * progress;

    try {
      if (typeof layer.setStyle === 'function') {
        layer.setStyle({ opacity, fillOpacity });
      } else if (layer.getElement && layer.getElement()) {
        layer.getElement().style.opacity = String(opacity);
      }
    } catch (e) {}

    if (progress < 1) {
      requestAnimationFrame(step);
      return;
    }

    try { liveCountryLayer.removeLayer(layer); } catch (e) {}
    activeAnimations = activeAnimations.filter(x => x !== layer);
  }

  requestAnimationFrame(step);
}

function polygonCenter(points) {
  if (!points || !points.length) return null;
  let lat = 0;
  let lon = 0;
  for (const p of points) {
    lat += p[0];
    lon += p[1];
  }
  return [lat / points.length, lon / points.length];
}

function animationDurationByCountdown(countdown) {
  const cd = Number(countdown || 0);
  if (cd >= 60) return 6500;
  if (cd >= 30) return 4200;
  if (cd >= 15) return 2600;
  return 1600;
}

function detectEstimatedOrigin(zoneName, zone) {
  const countdown = Number(zone?.countdown || 0);
  const center = zone?.polygon?.length ? polygonCenter(zone.polygon) : null;
  const lat = center ? center[0] : null;

  if (countdown >= 60) return 'iran';
  if (lat !== null) {
    if (lat >= 32.8) return 'lebanon';
    if (lat <= 31.6) return 'gaza';
  }
  if (countdown >= 30) return 'iran';
  if (countdown <= 15) return 'lebanon';
  return 'unknown';
}

function originStyle(origin) {
  if (origin === 'iran') {
    return {
      rocketStroke: '#c084fc',
      rocketFill: '#a855f7',
      trail: '#c084fc',
      blastStroke: '#c084fc',
      blastFill: '#7e22ce',
      label: 'איראן'
    };
  }
  if (origin === 'lebanon') {
    return {
      rocketStroke: '#ffd166',
      rocketFill: '#ffb703',
      trail: '#ffd166',
      blastStroke: '#f59e0b',
      blastFill: '#f59e0b',
      label: 'לבנון'
    };
  }
  if (origin === 'gaza') {
    return {
      rocketStroke: '#ff6b6b',
      rocketFill: '#ef4444',
      trail: '#ff7d7d',
      blastStroke: '#ef4444',
      blastFill: '#b91c1c',
      label: 'עזה'
    };
  }
  return {
    rocketStroke: '#e5e7eb',
    rocketFill: '#9ca3af',
    trail: '#d1d5db',
    blastStroke: '#d1d5db',
    blastFill: '#6b7280',
    label: 'לא ידוע'
  };
}

// АНИМАЦИЯ ПОЛЕТА (СОХРАНЕНА)
function animateFlightToPolygon(zoneName, zone, delayMs = 0) {
  if (!map || !zone?.polygon?.length) return;

  const target = polygonCenter(zone.polygon);
  if (!target) return;

  const origin = detectEstimatedOrigin(zoneName, zone);
  const style = originStyle(origin);

  let start = null;
  if (origin === 'iran') {
    start = [32.1, 39.5];
  } else if (origin === 'lebanon') {
    start = [33.45, 35.35];
  } else if (origin === 'gaza') {
    start = [31.5, 34.45];
  } else {
    start = [target[0], target[1] + 1.8];
  }

  const duration = animationDurationByCountdown(zone.countdown);

  const trail = L.polyline([start], {
    color: style.trail,
    weight: 3,
    opacity: 0.95,
    dashArray: '8,8'
  }).addTo(liveCountryLayer);

  const glowTrail = L.polyline([start], {
    color: style.trail,
    weight: 8,
    opacity: 0.18,
    lineCap: 'round'
  }).addTo(liveCountryLayer);

  const rocketIcon = L.divIcon({
    className: 'rocket-icon-wrapper',
    html: `
      <div style="position:relative; width:22px; height:22px; display:flex; align-items:center; justify-content:center;">
        <div style="position:absolute; width:22px; height:22px; border-radius:50%; background:${style.rocketFill}; opacity:.18; filter:blur(6px);"></div>
        <div style="position:relative; width:10px; height:10px; transform:rotate(45deg); background:${style.rocketFill}; border:2px solid ${style.rocketStroke}; border-radius:2px 10px 2px 10px; box-shadow:0 0 10px ${style.rocketFill};"></div>
      </div>
    `,
    iconSize: [22, 22],
    iconAnchor: [11, 11],
  });

  const rocket = L.marker(start, { icon: rocketIcon })
    .bindPopup(`<b>${zoneName}</b><br>מקור משוער: ${style.label}<br>זמן מיגון: ${zone.countdown || '—'} שנ׳`)
    .addTo(liveCountryLayer);

  activeAnimations.push(trail, glowTrail, rocket);

  let started = null;
  function step(ts) {
    if (!started) started = ts;
    const progress = Math.min((ts - started) / duration, 1);
    const lat = start[0] + (target[0] - start[0]) * progress;
    const lon = start[1] + (target[1] - start[1]) * progress;
    const pos = [lat, lon];
    rocket.setLatLng(pos);
    trail.addLatLng(pos);
    glowTrail.addLatLng(pos);
    
    // Поворот по направлению
    const angle = Math.atan2(target[1] - pos[1], target[0] - pos[0]) * 180 / Math.PI;
    const el = rocket.getElement();
    if (el) {
      const inner = el.querySelector('div > div:last-child');
      if (inner) inner.style.transform = `rotate(${angle + 135}deg)`;
    }

    if (progress < 1) {
      requestAnimationFrame(step);
      return;
    }

    const blast = L.circle(target, {
      radius: 1200,
      color: style.blastStroke,
      weight: 2,
      fillColor: style.blastFill,
      fillOpacity: 0.28,
    }).addTo(liveCountryLayer);
    activeAnimations.push(blast);

    setTimeout(() => {
      fadeAndRemoveLayer(rocket, 700, 0);
      fadeAndRemoveLayer(trail, 2200, 0);
      fadeAndRemoveLayer(glowTrail, 2600, 0);
      fadeAndRemoveLayer(blast, 1600, 0);
    }, 400);
  }

  setTimeout(() => requestAnimationFrame(step), delayMs);
}

// ПУЛЬСАЦИЯ ДЛЯ КАТЕГОРИЙ (Красный/Желтый)
function pulsePolygonCustom(layer, category) {
  const isDrone = category === 2;
  const started = Date.now();
  let on = false;
  
  const timer = setInterval(() => {
    // Если слой удален (например, пришла категория 13), останавливаем таймер
    if (!map.hasLayer(layer)) {
      clearInterval(timer);
      return;
    }
    
    on = !on;
    try {
      layer.setStyle({
        color: on ? (isDrone ? '#ffd166' : '#ffd166') : (isDrone ? '#f59e0b' : '#ff4d4d'),
        weight: on ? 3 : 2,
        fillColor: on ? (isDrone ? '#ffb703' : '#ff7d7d') : (isDrone ? '#f59e0b' : '#ff4d4d'),
        fillOpacity: on ? 0.35 : 0.15,
      });
    } catch (e) {}
  }, 600);
}

// НОВЫЙ ГЛАВНЫЙ ОБРАБОТЧИК МАССИВА СОБЫТИЙ (API SNAPSHOT)
function processNewFeedEvents(eventsArray) {
  // Сортируем от старых к новым
  eventsArray.sort((a, b) => new Date(a.alertDate) - new Date(b.alertDate));

  eventsArray.forEach(ev => {
    const city = ev.data.trim();
    const category = ev.category;
    const title = ev.title;
    
    // Создаем уникальный ключ, чтобы не обрабатывать одно событие дважды
    const eventKey = `${city}_${ev.alertDate}_${category}`;
    if (processedEventKeys.has(eventKey)) return;
    processedEventKeys.add(eventKey);

    // КАТЕГОРИЯ 13: ОТБОЙ ТРЕВОГИ
    if (category === 13) {
      if (activeAlertsMap[city]) {
        fadeAndRemoveLayer(activeAlertsMap[city], 1000, 0);
        delete activeAlertsMap[city];
      }
      return;
    }

    // КАТЕГОРИИ 1 (Ракета) или 2 (БПЛА)
    if (category === 1 || category === 2) {
      if (activeAlertsMap[city]) return; // Если уже горит, не дублируем

      const zone = zoneIndex[city];
      if (zone && zone.polygon && zone.polygon.length > 0) {
        const isDrone = category === 2;
        const baseColor = isDrone ? '#f59e0b' : '#ff4d4d';

        const polygon = L.polygon(zone.polygon, {
          color: baseColor,
          weight: 2,
          fillColor: baseColor,
          fillOpacity: 0.15,
        }).bindPopup(`
          <b>${city}</b><br>
          <span style="color:${baseColor}; font-weight:bold;">${title}</span><br>
          זמן מיגון: ${zone.countdown || '—'} שנ׳
        `);
        
        polygon.addTo(liveCountryLayer);
        pulsePolygonCustom(polygon, category);
        
        // ЗАПУСКАЕМ АНИМАЦИЮ ПОЛЕТА
        animateFlightToPolygon(city, zone, 0);

        activeAlertsMap[city] = polygon;

        // Фолбэк на случай, если Категория 13 не придет: удаляем через 5 минут
        setTimeout(() => {
          if (activeAlertsMap[city] === polygon) {
            fadeAndRemoveLayer(polygon, 1800, 0);
            delete activeAlertsMap[city];
            updateMapCaption();
          }
        }, 300000);

      } else {
        // Если полигона нет в локальной базе — ставим маркер
        const centroid = zoneCentroids[city] || fallbackCityCoords[city];
        if (centroid) {
          const isDrone = category === 2;
          const mColor = isDrone ? '#f59e0b' : '#ff2d55';
          const marker = L.circleMarker([centroid[0], centroid[1]], {
            radius: 8,
            color: mColor,
            weight: 2,
            fillColor: mColor,
            fillOpacity: 0.9,
          }).bindPopup(`<b>${city}</b><br>${title}`);
          
          marker.addTo(liveCountryLayer);
          activeAlertsMap[city] = marker;
          
          setTimeout(() => {
            if (activeAlertsMap[city] === marker) {
              fadeAndRemoveLayer(marker, 1000, 0);
              delete activeAlertsMap[city];
              updateMapCaption();
            }
          }, 300000);
        }
      }
    }
  });

  updateMapCaption();
}

function renderSimpleList(elementId, items, mapper) {
  document.getElementById(elementId).innerHTML = items.map(mapper).join('');
}

function renderDailyChart(days, city) {
  const labels = days.map(x => x.date);
  const values = days.map(x => x.count);
  const ctx = document.getElementById('dailyChart');
  if (dailyChart) dailyChart.destroy();
  dailyChart = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets: [{ label: city, data: values, tension: .25, fill: true, borderWidth: 2 }] },
    options: {
      responsive: true,
      plugins: { legend: { labels: { color: '#dbe5ff' } } },
      scales: {
        x: { ticks: { color: '#aebee4', maxRotation: 90, minRotation: 90, autoSkip: false }, grid: { color: 'rgba(255,255,255,.05)' } },
        y: { beginAtZero: true, ticks: { color: '#aebee4' }, grid: { color: 'rgba(255,255,255,.05)' } },
      }
    }
  });
  setText('chartCaption', `${city} · ${labels.length} נקודות`);
}

function renderHourlyChart(items) {
  const labels = items.map(x => x.hour);
  const values = items.map(x => x.count);
  const ctx = document.getElementById('hourlyChart');
  if (hourlyChart) hourlyChart.destroy();
  hourlyChart = new Chart(ctx, {
    type: 'bar',
    data: { labels, datasets: [{ label: 'כמות התרעות', data: values, borderWidth: 1 }] },
    options: {
      responsive: true,
      plugins: { legend: { labels: { color: '#dbe5ff' } } },
      scales: {
        x: { ticks: { color: '#aebee4' }, grid: { color: 'rgba(255,255,255,.05)' } },
        y: { beginAtZero: true, ticks: { color: '#aebee4' }, grid: { color: 'rgba(255,255,255,.05)' } },
      }
    }
  });
  setText('hourlyCaption', `${labels.length} שעות`);
}

async function loadMeta() {
  datasetMeta = await getJson('/api/meta');

  const minDate = datasetMeta?.min_date || '—';
  const dbMaxDate = datasetMeta?.max_date || '—'; 
  const actualToday = todayLocalISO(); 
  const refreshed = datasetMeta?.refreshed_at || '—';

  setText(
    'datasetMeta',
    `רשומות: ${fmtNum(datasetMeta.total_events)} · ערים/אזורים: ${fmtNum(datasetMeta.total_cities)} · אזורים עם פוליגון: ${fmtNum(datasetMeta.total_zones || 0)} · בסיס היסטורי: ${minDate} → ${dbMaxDate} · עדכון היסטוריה: ${refreshed}`
  );

  if (!document.getElementById('toDate').value) {
    document.getElementById('toDate').value = actualToday;
  }
  if (!document.getElementById('fromDate').value) {
    document.getElementById('fromDate').value = shiftDays(actualToday, -29);
  }
}

async function loadCities() {
  const data = await getJson('/api/cities');
  allCities = data.cities;
  const dl = document.getElementById('citiesList');
  dl.innerHTML = allCities.map(c => `<option value="${c}"></option>`).join('');
  const preferred = ['חולון', 'אשדוד', 'אשקלון', 'באר שבע', 'תל אביב - מרכז העיר'];
  const first = preferred.find(x => allCities.includes(x)) || allCities[0] || '';
  if (!document.getElementById('citySelect').value) {
    document.getElementById('citySelect').value = first;
  }
}

function applyPreset() {
  const preset = document.getElementById('preset').value;
  const actualToday = todayLocalISO();
  
  if (preset === 'all') {
    document.getElementById('fromDate').value = datasetMeta?.min_date || actualToday;
    document.getElementById('toDate').value = actualToday;
    return;
  }
  const days = parseInt(preset, 10);
  document.getElementById('toDate').value = actualToday;
  document.getElementById('fromDate').value = shiftDays(actualToday, -(days - 1));
}

function renderSummary(data) {
  setText('statToday', fmtNum(data.summary.today));
  setText('statWeek', fmtNum(data.summary.last_7_days));
  setText('statMonth', fmtNum(data.summary.last_30_days));
  setText('statTotal', fmtNum(data.summary.total_in_range));

  setText('statTodaySub', `עבור ${data.summary.today_date || '—'}`);
  setText('statWeekSub', `שעת שיא: ${data.summary.best_recent_hour?.hour || '—'}`);
  setText('statMonthSub', `שעת שיא נפוצה: ${data.summary.prediction?.best_hour || '—'}`);
  setText('statTotalSub', data.summary.prediction?.reason || '—');

  renderDailyChart(data.daily, data.city);
  renderHourlyChart(data.hourly_distribution);

  renderSimpleList('recentEventsList', data.recent_events, (r) => `
    <div class="row">
      <div>
        <div class="name">${r.datetime}</div>
        <div class="meta">${r.date} · ${r.hour}</div>
      </div>
      <div class="badge">התרעה</div>
    </div>
  `);

  const rows = [
    { title: 'שעת שיא', meta: data.summary.prediction?.best_hour || '—', badge: 'שעה' },
    { title: 'יום בולט', meta: data.summary.prediction?.best_weekday || '—', badge: 'יום' },
    { title: 'הסבר', meta: data.summary.prediction?.reason || '—', badge: 'ניתוח' },
  ];

  renderSimpleList('predictionList', rows, (r) => `
    <div class="row">
      <div>
        <div class="name">${r.title}</div>
        <div class="meta">${r.meta}</div>
      </div>
      <div class="badge">${r.badge}</div>
    </div>
  `);
}

async function loadDashboard() {
  let city = document.getElementById('citySelect').value.trim();
  const from = document.getElementById('fromDate').value;
  const to = document.getElementById('toDate').value;

  if (!city) {
    city = allCities.includes('חולון') ? 'חולון' : (allCities[0] || '');
    if (city) document.getElementById('citySelect').value = city;
  }

  if (!city) return;

  const safeFrom = document.getElementById('fromDate').value;
  const safeTo = document.getElementById('toDate').value;
  const params = new URLSearchParams({ city, from: safeFrom, to: safeTo });
  const data = await getJson(`/api/city-stats?${params.toString()}`);
  renderSummary(data);
}

async function refreshBackend() {
  setText('datasetMeta', 'מרענן נתונים...');
  await getJson('/api/refresh', { method: 'POST' });
  await loadMeta();
  await loadCities();
  await loadDashboard();
}

function connectLiveStream() {
  if (stream) {
    try { stream.close(); } catch (e) {}
  }

  stream = new EventSource('/api/stream');

  stream.onopen = () => {
    setText('liveStatus', '🟢 שידור חי מחובר');
  };

  stream.onerror = () => {
    setText('liveStatus', '🟠 בעיית חיבור לשידור חי, מנסה להתחבר מחדש...');
    ensureMap();
  };

  stream.onmessage = async (event) => {
    try {
      const payload = JSON.parse(event.data);
      if (payload.type === 'heartbeat') {
        setText('liveStatus', `🟢 שידור חי מחובר · פעימה אחרונה: ${payload.server_time}`);
        return;
      }

      // ПЕРЕДАЕМ НОВЫЙ МАССИВ В ОБРАБОТЧИК
      if (Array.isArray(payload)) {
        processNewFeedEvents(payload);
        
        // Выводим последний ивент в статус
        const activeAlerts = payload.filter(p => p.category === 1 || p.category === 2);
        if (activeAlerts.length > 0) {
            const latest = activeAlerts[activeAlerts.length - 1];
            setText('liveStatus', `🟢 אירוע לייב: ${latest.data} (${latest.title})`);
        }
      }

    } catch (err) {
      console.error('stream message error', err);
    }
  };
}

async function bootstrap() {
  await loadMeta();
  await loadCities();

  try {
    const zoneData = await getJson('/api/zones');
    zoneIndex = zoneData.zones || {};
    zoneCentroids = zoneData.centroids || {};
  } catch (e) {
    console.warn('zones unavailable', e);
    zoneIndex = {};
    zoneCentroids = {};
  }

  initCountryMapView();
  clearLiveLayers();
  await loadDashboard();
  connectLiveStream();

  document.getElementById('applyBtn').addEventListener('click', loadDashboard);
  document.getElementById('refreshBtn').addEventListener('click', refreshBackend);
  document.getElementById('preset').addEventListener('change', () => {
    applyPreset();
    loadDashboard();
  });
  document.getElementById('citySelect').addEventListener('change', loadDashboard);
}

bootstrap().catch(err => {
  console.error(err);
  setText('datasetMeta', 'שגיאת טעינה: ' + err.message);
});
</script>
</body>
</html>
'''

@dataclass
class EventRecord:
    ts: int
    date: str
    week: str
    month: str
    hour: str
    weekday: str
    cities: list[str]
    threat: int

class DataStore:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.last_refresh = 0.0
        self.events: list[EventRecord] = []
        self.city_daily: dict[str, Counter[str]] = {}
        self.city_weekly: dict[str, Counter[str]] = {}
        self.city_monthly: dict[str, Counter[str]] = {}
        self.city_hourly: dict[str, Counter[str]] = {}
        self.city_weekday_hourly: dict[str, dict[str, Counter[str]]] = {}
        self.city_totals: Counter[str] = Counter()
        self.all_cities: list[str] = []
        self.min_date: str | None = None
        self.max_date: str | None = None
        self.zones: dict[str, dict[str, Any]] = {}
        self.zone_centroids: dict[str, tuple[float, float]] = {}
        
        self.live_alerts: list[dict[str, Any]] = []
        self.last_live_refresh = 0.0

    def ensure_loaded(self, force: bool = False) -> None:
        now = time.time()
        with self.lock:
            if not self.zones:
                zone_raw = self._load_local_zones()
                self._build_zone_index(zone_raw)
            if not force and self.events and (now - self.last_refresh) < REFRESH_SECONDS:
                return
            raw = self._download_or_load(force=force)
            self._build_indexes(raw)
            self.last_refresh = now

    def _download_or_load(self, force: bool = False) -> Any:
        if not force and DATA_FILE.exists():
            age = time.time() - DATA_FILE.stat().st_mtime
            if age < REFRESH_SECONDS:
                try:
                    cached = DATA_FILE.read_text(encoding="utf-8").strip()
                    if cached:
                        return json.loads(cached)
                except Exception:
                    pass

        req = Request(
            DATA_URL,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json, text/plain, */*",
            },
        )
        with urlopen(req, timeout=60) as resp:
            raw_bytes = resp.read()

        raw_text = raw_bytes.decode("utf-8").strip()
        if not raw_text:
            raise RuntimeError("Downloaded all.json is empty")

        DATA_FILE.write_text(raw_text, encoding="utf-8")
        META_FILE.write_text(
            json.dumps({"refreshed_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return json.loads(raw_text)

    def _load_local_zones(self) -> dict[str, Any]:
        if not LOCAL_ZONE_SOURCE.exists():
            return {"zones": {}}
        try:
            text = LOCAL_ZONE_SOURCE.read_text(encoding="utf-8").strip()
            if text:
                return json.loads(text)
        except Exception as e:
            raise RuntimeError(f"Failed to read local polygons file: {e}")
        return {"zones": {}}

    def refresh_live_snapshot(self, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self.last_live_refresh) < LIVE_REFRESH_SECONDS:
            return

        req = Request(
            SNAPSHOT_URL,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json, text/plain, */*",
            },
        )

        try:
            with urlopen(req, timeout=10) as resp:
                raw = resp.read().decode("utf-8").strip()
            if not raw:
                self.live_alerts = []
                self.last_live_refresh = now
                return

            data = json.loads(raw)
            # Извлекаем тревоги по новой логике
            alerts = self._extract_live_alerts_from_snapshot(data)
            self.live_alerts = alerts
            self.last_live_refresh = now
        except Exception as e:
            print("snapshot refresh error:", e)
            self.last_live_refresh = now

    def _extract_live_alerts_from_snapshot(self, data: Any) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []

        if isinstance(data, dict) and "events" in data:
            for event in data.get("events", []):
                # Нас интересуют только "alert" (игнорируем социалки и самолеты)
                if event.get("eventType") == "alert":
                    props = event.get("properties", {})
                    city_he = props.get("cityHebrew")
                    
                    if not city_he:
                        continue
                        
                    city_name = str(city_he).strip()
                    alert_state = props.get("alertState", "")
                    alert_type = props.get("alertType", "")
                    title = props.get("title", "")
                    alert_date = event.get("time", "")
                    
                    # ОПРЕДЕЛЯЕМ КАТЕГОРИЮ
                    if alert_state == "cleared" or "הסתיים" in title:
                        category = 13 # Отбой
                    elif "כלי טיס" in alert_type or "UAV" in alert_type:
                        category = 2  # БПЛА
                    else:
                        category = 1  # Ракеты
                        
                    results.append({
                        "alertDate": alert_date,
                        "title": title if title else alert_type,
                        "data": city_name,
                        "category": category
                    })
                    
        return results

    def _build_indexes(self, raw: Any) -> None:
        events: list[EventRecord] = []
        city_daily: dict[str, Counter[str]] = defaultdict(Counter)
        city_weekly: dict[str, Counter[str]] = defaultdict(Counter)
        city_monthly: dict[str, Counter[str]] = defaultdict(Counter)
        city_hourly: dict[str, Counter[str]] = defaultdict(Counter)
        city_weekday_hourly: dict[str, dict[str, Counter[str]]] = defaultdict(lambda: defaultdict(Counter))
        city_totals: Counter[str] = Counter()
        city_set: set[str] = set()
        min_date = None
        max_date = None
        seen: set[tuple[int, int, tuple[str, ...]]] = set()

        for item in raw:
            if not isinstance(item, list) or len(item) < 4:
                continue
            threat = item[1]
            if threat not in DEFAULT_THREAT_TYPES:
                continue
            cities = item[2]
            ts = item[3]
            if not isinstance(cities, list) or not isinstance(ts, int):
                continue

            cities_clean = sorted({str(c).strip() for c in cities if str(c).strip()})
            if not cities_clean:
                continue

            key = (ts, threat, tuple(cities_clean))
            if key in seen:
                continue
            seen.add(key)

            dt = datetime.fromtimestamp(ts, tz=TZ)
            date = dt.strftime("%Y-%m-%d")
            iso_year, iso_week, _ = dt.isocalendar()
            week = f"{iso_year}-W{iso_week:02d}"
            month = dt.strftime("%Y-%m")
            hour = f"{dt.hour:02d}:00"
            weekday = str(dt.weekday())

            events.append(EventRecord(ts=ts, date=date, week=week, month=month, hour=hour, weekday=weekday, cities=cities_clean, threat=threat))

            if min_date is None or date < min_date:
                min_date = date
            if max_date is None or date > max_date:
                max_date = date

            for city in cities_clean:
                city_set.add(city)
                city_daily[city][date] += 1
                city_weekly[city][week] += 1
                city_monthly[city][month] += 1
                city_hourly[city][hour] += 1
                city_weekday_hourly[city][weekday][hour] += 1
                city_totals[city] += 1

        self.events = sorted(events, key=lambda x: x.ts)
        self.city_daily = dict(city_daily)
        self.city_weekly = dict(city_weekly)
        self.city_monthly = dict(city_monthly)
        self.city_hourly = dict(city_hourly)
        self.city_weekday_hourly = {city: dict(v) for city, v in city_weekday_hourly.items()}
        self.city_totals = city_totals
        self.all_cities = sorted(city_set)
        self.min_date = min_date
        self.max_date = max_date

    def _build_zone_index(self, zone_raw: dict[str, Any]) -> None:
        zones = zone_raw.get("zones", {}) if isinstance(zone_raw, dict) else {}
        parsed: dict[str, dict[str, Any]] = {}
        centroids: dict[str, tuple[float, float]] = {}

        for raw_name, payload in zones.items():
            if not isinstance(raw_name, str) or not isinstance(payload, dict):
                continue

            name = self._normalize_zone_name(raw_name)
            polygon = payload.get("polygon") or []
            latlngs: list[list[float]] = []
            for point in polygon:
                if isinstance(point, list) and len(point) >= 2:
                    lon = point[0]
                    lat = point[1]
                    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                        latlngs.append([float(lat), float(lon)])

            centroid = self._polygon_centroid(latlngs) if latlngs else CITY_COORDS.get(name)
            zone_entry = {
                "id": payload.get("id"),
                "en": payload.get("en"),
                "countdown": payload.get("countdown"),
                "polygon": latlngs,
            }
            parsed[name] = zone_entry
            parsed[raw_name] = zone_entry
            if centroid:
                centroids[name] = centroid
                centroids[raw_name] = centroid

        for alias, canonical in ZONE_NAME_ALIASES.items():
            if canonical in parsed:
                parsed[alias] = parsed[canonical]
                if canonical in centroids:
                    centroids[alias] = centroids[canonical]

        self.zones = parsed
        self.zone_centroids = centroids

    @staticmethod
    def _normalize_zone_name(name: str) -> str:
        normalized = " ".join(name.strip().split())
        normalized = normalized.replace("–", "-").replace("—", "-")
        normalized = normalized.replace(" - ", "-")
        return ZONE_NAME_ALIASES.get(normalized, normalized)

    @staticmethod
    def _polygon_centroid(points: list[list[float]]) -> tuple[float, float] | None:
        if not points:
            return None
        lat = sum(p[0] for p in points) / len(points)
        lon = sum(p[1] for p in points) / len(points)
        return (lat, lon)

    def meta(self) -> dict[str, Any]:
        refreshed_at = None
        if META_FILE.exists():
            try:
                cached = META_FILE.read_text(encoding="utf-8").strip()
                if cached:
                    refreshed_at = json.loads(cached).get("refreshed_at")
            except Exception:
                refreshed_at = None

        return {
            "total_events": len(self.events),
            "total_cities": len(self.all_cities),
            "total_zones": len(self.zones),
            "min_date": self.min_date,
            "max_date": self.max_date,
            "refreshed_at": refreshed_at,
        }

store = DataStore()

def normalize_city(city: str) -> str:
    city = city.strip()
    return CITY_ALIASES.get(city, city)

def daterange_days(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    days = []
    cur = s
    while cur <= e:
        days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days


@app.get("/")
def index() -> Response:
    store.ensure_loaded()
    return Response(HTML, mimetype="text/html")


@app.get("/api/meta")
def api_meta():
    store.ensure_loaded()
    return jsonify(store.meta())


@app.post("/api/refresh")
def api_refresh():
    store.ensure_loaded(force=True)
    return jsonify({"ok": True, **store.meta()})


@app.get("/api/cities")
def api_cities():
    store.ensure_loaded()
    return jsonify({"cities": store.all_cities})


@app.get("/api/zones")
def api_zones():
    store.ensure_loaded()
    return jsonify({"zones": store.zones, "centroids": {k: [v[0], v[1]] for k, v in store.zone_centroids.items()}})


@app.get("/api/city-stats")
def api_city_stats():
    store.ensure_loaded()

    city = normalize_city(request.args.get("city", ""))
    if not city:
        return jsonify({"error": "city is required"}), 400
    if city not in store.city_daily:
        return jsonify({"error": f"city not found: {city}"}), 404

    start = request.args.get("from") or store.min_date
    end = request.args.get("to") or datetime.now(TZ).strftime("%Y-%m-%d")
    if not start or not end:
        return jsonify({"error": "date range unavailable"}), 400
    if start > end:
        return jsonify({"error": "from must be <= to"}), 400

    daily_counter = store.city_daily[city]
    hourly_counter = store.city_hourly.get(city, Counter())
    weekday_hourly = store.city_weekday_hourly.get(city, {})

    daily_rows = [{"date": d, "count": daily_counter.get(d, 0)} for d in daterange_days(start, end)]
    total_in_range = sum(row["count"] for row in daily_rows)

    today_date = datetime.now(TZ).strftime("%Y-%m-%d")
    last_7_start = (datetime.strptime(today_date, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
    last_30_start = (datetime.strptime(today_date, "%Y-%m-%d") - timedelta(days=29)).strftime("%Y-%m-%d")

    today_val = daily_counter.get(today_date, 0)
    week_val = sum(v for k, v in daily_counter.items() if last_7_start <= k <= today_date)
    month_val = sum(v for k, v in daily_counter.items() if last_30_start <= k <= today_date)

    best_hour = "—"
    if hourly_counter:
        best_hour = max(hourly_counter.items(), key=lambda x: x[1])[0]

    best_weekday_name = "—"
    best_weekday_hour = "—"
    reason = "אין מספיק נתונים לאומדן סטטיסטי."

    if weekday_hourly:
        best_day = max(weekday_hourly.keys(), key=lambda d: sum(weekday_hourly[d].values()))
        best_weekday_name = WEEKDAY_NAMES_HE.get(str(best_day), "—")
        if weekday_hourly[best_day]:
            best_weekday_hour = max(weekday_hourly[best_day].items(), key=lambda x: x[1])[0]
            reason = f"מבוסס על היסטוריית האזעקות, נראה שהסבירות הגבוהה ביותר לירי היא ב{best_weekday_name} סביב השעה {best_weekday_hour}."

    recent_events = []
    for ev in reversed(store.events):
        if city in ev.cities:
            recent_events.append({
                "datetime": datetime.fromtimestamp(ev.ts, tz=TZ).strftime("%d/%m/%Y %H:%M:%S"),
                "date": ev.date,
                "hour": ev.hour
            })
            if len(recent_events) >= 15:
                break

    hourly_dist = [{"hour": f"{h:02d}:00", "count": hourly_counter.get(f"{h:02d}:00", 0)} for h in range(24)]

    return jsonify({
        "city": city,
        "summary": {
            "today": today_val,
            "last_7_days": week_val,
            "last_30_days": month_val,
            "total_in_range": total_in_range,
            "today_date": today_date,
            "best_recent_hour": {"hour": best_hour},
            "prediction": {
                "best_hour": best_hour,
                "best_weekday": best_weekday_name,
                "reason": reason
            }
        },
        "daily": daily_rows,
        "hourly_distribution": hourly_dist,
        "recent_events": recent_events
    })


@app.get("/api/stream")
def api_stream():
    """
    Server-Sent Events (SSE) endpoint for Live Alerts.
    """
    def generate():
        # Отправляем начальный heartbeat при подключении
        yield f"data: {json.dumps({'type': 'heartbeat', 'server_time': datetime.now(TZ).strftime('%H:%M:%S')})}\n\n"
        
        while True:
            time.sleep(STREAM_POLL_SECONDS)
            store.refresh_live_snapshot()
            
            alerts = store.live_alerts
            if alerts:
                yield f"data: {json.dumps(alerts)}\n\n"
            else:
                yield f"data: {json.dumps({'type': 'heartbeat', 'server_time': datetime.now(TZ).strftime('%H:%M:%S')})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream")


if __name__ == "__main__":
    print("Starting Iron Monitor Live...")
    # Загружаем данные до запуска сервера
    store.ensure_loaded()
    app.run(host="0.0.0.0", port=5000, threaded=True)