from __future__ import annotations

import json
import math
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
LOCAL_ZONE_SOURCE = Path("alert-zones-local.json")

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
DATA_FILE = CACHE_DIR / "all.json"
META_FILE = CACHE_DIR / "meta.json"

REFRESH_SECONDS = 600
STREAM_POLL_SECONDS = 3
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
        מפת לייב ארצית נפרדת מהחיפוש. החיפוש משפיע רק על הסטטיסטיקה. פוליגונים מופיעים רק באירועי לייב. אין ציור התחלתי של כל האזורים.
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
            <div class="pill"><span class="dot red"></span>פוליגוני לייב / סימונים</div>
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
let liveCountryMarkers = [];
let liveCountryPolygons = [];
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
  setText('mapCaption', `לייב ארצי פעיל · ${liveCountryPolygons.length} פוליגונים · ${liveCountryMarkers.length} סימונים`);
}

function clearLiveLayers() {
  if (!liveCountryLayer) return;
  liveCountryLayer.clearLayers();
  liveCountryMarkers = [];
  liveCountryPolygons = [];
  setText('mapCaption', 'לייב ארצי פעיל · 0 פוליגונים · 0 סימונים');
}

function pulsePolygon(layer, durationMs = 120000) {
  const started = Date.now();
  let on = false;
  const timer = setInterval(() => {
    const elapsed = Date.now() - started;
    if (elapsed >= durationMs) {
      clearInterval(timer);
      try {
        layer.setStyle({ color: '#ff4d4d', weight: 2, fillColor: '#ff4d4d', fillOpacity: 0.18 });
      } catch (e) {}
      return;
    }
    on = !on;
    try {
      layer.setStyle({
        color: on ? '#ffd166' : '#ff4d4d',
        weight: on ? 3 : 2,
        fillColor: on ? '#ff7d7d' : '#ff4d4d',
        fillOpacity: on ? 0.30 : 0.18,
      });
    } catch (e) {}
  }, 700);
}

function addLiveMarker(lat, lon, label, city) {
  ensureMap();
  const marker = L.circleMarker([lat, lon], {
    radius: 7,
    color: '#ff7d7d',
    weight: 2,
    fillColor: '#ff2d55',
    fillOpacity: 0.9,
  }).bindPopup(`<b>${city}</b><br>${label}<br>מיקום משוער`);
  marker.addTo(liveCountryLayer);
  liveCountryMarkers.push(marker);

  setTimeout(() => {
    try {
      liveCountryLayer.removeLayer(marker);
      liveCountryMarkers = liveCountryMarkers.filter(m => m !== marker);
      setText('mapCaption', `לייב ארצי פעיל · ${liveCountryPolygons.length} פוליגונים · ${liveCountryMarkers.length} סימונים`);
    } catch (e) {}
  }, 180000);
}

function addLivePolygon(zoneName, label) {
  ensureMap();
  const zone = zoneIndex[zoneName];
  if (!zone || !zone.polygon?.length) return false;

  const polygon = L.polygon(zone.polygon, {
    color: '#ff4d4d',
    weight: 2,
    fillColor: '#ff4d4d',
    fillOpacity: 0.18,
  }).bindPopup(`
    <b>${zoneName}</b><br>
    ${label}<br>
    זמן מיגון: ${zone.countdown || '—'} שנ׳<br>
    ${zone.en || ''}
  `);
  polygon.addTo(liveCountryLayer);
  pulsePolygon(polygon);
  liveCountryPolygons.push(polygon);

  polygon.on('click', () => polygon.openPopup());

  setTimeout(() => {
    try {
      liveCountryLayer.removeLayer(polygon);
      liveCountryPolygons = liveCountryPolygons.filter(p => p !== polygon);
      setText('mapCaption', `לייב ארצי פעיל · ${liveCountryPolygons.length} פוליגונים · ${liveCountryMarkers.length} סימונים`);
    } catch (e) {}
  }, 180000);

  return true;
}

function handleLiveCountryEvent(payload) {
  if (!payload?.cities?.length) return;
  const label = payload.datetime || 'אירוע חדש';

  payload.cities.forEach(city => {
    const hasPolygon = addLivePolygon(city, label);
    if (!hasPolygon) {
      const centroid = zoneCentroids[city] || fallbackCityCoords[city];
      if (centroid) {
        addLiveMarker(centroid[0], centroid[1], label, city);
      }
    }
  });

  setText('mapCaption', `לייב ארצי פעיל · ${liveCountryPolygons.length} פוליגונים · ${liveCountryMarkers.length} סימונים`);
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
  setText('datasetMeta', `רשומות: ${fmtNum(datasetMeta.total_events)} · ערים/אזורים: ${fmtNum(datasetMeta.total_cities)} · אזורים עם פוליגון: ${fmtNum(datasetMeta.total_zones || 0)} · עדכון אחרון: ${datasetMeta.refreshed_at || '—'}`);

  const today = todayLocalISO();
  if (!document.getElementById('toDate').value) document.getElementById('toDate').value = today;
  if (!document.getElementById('fromDate').value) document.getElementById('fromDate').value = shiftDays(today, -29);
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
  const today = todayLocalISO();
  if (preset === 'all') {
    document.getElementById('fromDate').value = datasetMeta?.min_date || today;
    document.getElementById('toDate').value = today;
    return;
  }
  const days = parseInt(preset, 10);
  document.getElementById('toDate').value = today;
  document.getElementById('fromDate').value = shiftDays(today, -(days - 1));
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

  const params = new URLSearchParams({ city, from, to });
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

      handleLiveCountryEvent(payload);

      const selectedCity = document.getElementById('citySelect').value.trim();
      if (payload.cities && payload.cities.includes(selectedCity)) {
        setText('liveStatus', `🟢 התקבל אירוע חדש עבור ${selectedCity} · ${payload.datetime}`);
        await loadMeta();
        await loadDashboard();
      } else {
        setText('liveStatus', `🟢 אירוע לייב חדש: ${payload.cities?.slice(0,3).join(', ') || '—'}`);
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

    today_real = datetime.now(TZ).strftime("%Y-%m-%d")
    last_7_start = (datetime.strptime(today_real, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
    last_30_start = (datetime.strptime(today_real, "%Y-%m-%d") - timedelta(days=29)).strftime("%Y-%m-%d")

    today_val = daily_counter.get(today_real, 0)
    week_val = sum(v for k, v in daily_counter.items() if last_7_start <= k <= today_real)
    month_val = sum(v for k, v in daily_counter.items() if last_30_start <= k <= today_real)

    hourly_distribution = []
    for h in range(24):
        hour_label = f"{h:02d}:00"
        hourly_distribution.append({"hour": hour_label, "count": hourly_counter.get(hour_label, 0)})

    top_hours = sorted(hourly_distribution, key=lambda x: (-x["count"], x["hour"]))[:5]
    best_recent_hour = top_hours[0] if top_hours else {"hour": "—", "count": 0}

    weekday_scores = []
    for weekday, hour_counter in weekday_hourly.items():
        total = sum(hour_counter.values())
        if total > 0:
            weekday_scores.append((weekday, total))
    weekday_scores.sort(key=lambda x: (-x[1], x[0]))

    best_weekday = WEEKDAY_NAMES_HE.get(weekday_scores[0][0], "—") if weekday_scores else "—"
    weekday_score = weekday_scores[0][1] if weekday_scores else 0

    recent_events = []
    for event in reversed(store.events):
        if city in event.cities:
            recent_events.append({
                "datetime": datetime.fromtimestamp(event.ts, tz=TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "date": event.date,
                "hour": event.hour,
            })
        if len(recent_events) >= 12:
            break

    prediction_summary = {
        "best_hour": best_recent_hour["hour"],
        "score": best_recent_hour["count"],
        "best_weekday": best_weekday,
        "weekday_score": weekday_score,
        "reason": f"השעה {best_recent_hour['hour']} בולטת סטטיסטית עבור {city}. זה אינו חיזוי אמיתי אלא סיכום דפוסים מהעבר.",
    }

    return jsonify({
        "city": city,
        "daily": daily_rows,
        "hourly_distribution": hourly_distribution,
        "top_hours": top_hours,
        "recent_events": recent_events,
        "summary": {
            "today": today_val,
            "today_date": today_real,
            "last_7_days": week_val,
            "last_30_days": month_val,
            "total_in_range": total_in_range,
            "best_recent_hour": best_recent_hour,
            "prediction": prediction_summary,
        },
    })


@app.get("/api/stream")
def api_stream() -> Response:
    @stream_with_context
    def generate():
        last_seen = 0
        while True:
            store.ensure_loaded()
            latest_ts = store.events[-1].ts if store.events else 0

            if latest_ts > last_seen:
                for event in store.events:
                    if event.ts <= last_seen:
                        continue
                    payload = {
                        "type": "alert",
                        "ts": event.ts,
                        "cities": event.cities,
                        "datetime": datetime.fromtimestamp(event.ts, tz=TZ).strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                    last_seen = event.ts
            else:
                heartbeat = {
                    "type": "heartbeat",
                    "server_time": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
                }
                yield f"data: {json.dumps(heartbeat, ensure_ascii=False)}\n\n"

            time.sleep(STREAM_POLL_SECONDS)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    import os

    store.ensure_loaded(force=True)
    port = int(os.environ.get("PORT", 5000))
    print(f"Iron Monitor Live ready: http://127.0.0.1:{port}")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
