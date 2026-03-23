from __future__ import annotations

import json
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from flask import Flask, jsonify, request, Response

app = Flask(__name__)

DATA_URL = "https://www.tzevaadom.co.il/static/historical/all.json"
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)
DATA_FILE = CACHE_DIR / "all.json"
META_FILE = CACHE_DIR / "meta.json"
REFRESH_SECONDS = 600

# Израильское время (в среднем UTC+2/+3). Для исторической точности 
# используем статичный оффсет, чтобы часы не съезжали на зарубежных серверах.
ISRAEL_TZ = timezone(timedelta(hours=2))

DEFAULT_THREAT_TYPES = {0}

CITY_ALIASES = {
    "אשקלון": "Ashkelon",
    "אשדוד": "Ashdod",
    "באר שבע": "Beer Sheva",
}

HTML = r'''<!doctype html>
<html lang="he" dir="rtl">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>לוח בקרה - 926</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {
      --bg: #0b1220;
      --panel: #131c2e;
      --soft: #1b2740;
      --line: #2b3a5e;
      --text: #eaf0ff;
      --muted: #9fb0d7;
      --accent: #7aa2ff;
      --good: #4bd18b;
      --warn: #ffb84d;
      --bad: #ff6b6b;
      --shadow: 0 10px 30px rgba(0,0,0,.25);
      --radius: 20px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      background: linear-gradient(180deg, #09101c 0%, #0d1528 100%);
      color: var(--text);
      direction: rtl;
      text-align: right;
    }
    .wrap {
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
    }
    .hero {
      display: grid;
      grid-template-columns: 1.5fr 1fr;
      gap: 20px;
      margin-bottom: 20px;
    }
    .card {
      background: rgba(19, 28, 46, 0.92);
      border: 1px solid rgba(255,255,255,.08);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 20px;
      backdrop-filter: blur(10px);
      display: flex;
      flex-direction: column;
    }
    h1, h2, h3 { margin: 0; }
    .title {
      font-size: 34px;
      font-weight: 800;
      letter-spacing: .3px;
      margin-bottom: 12px;
    }
    .sub {
      color: var(--muted);
      line-height: 1.5;
      font-size: 15px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 20px;
    }
    .span-12 { grid-column: span 12; }
    .span-8 { grid-column: span 8; }
    .span-6 { grid-column: span 6; }
    .span-4 { grid-column: span 4; }
    .controls {
      display: grid;
      grid-template-columns: 1.3fr 1fr 1fr 1fr auto;
      gap: 12px;
      align-items: end;
    }
    label {
      display: block;
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 8px;
    }
    input, select, button {
      width: 100%;
      background: var(--soft);
      color: var(--text);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px 14px;
      font-size: 14px;
      outline: none;
      direction: rtl;
      text-align: right;
    }
    button {
      cursor: pointer;
      font-weight: 700;
      transition: .2s ease;
      text-align: center;
    }
    button:hover { transform: translateY(-1px); }
    .btn-primary { background: linear-gradient(135deg, #4f7cff, #7aa2ff); border: none; }
    .btn-secondary { background: linear-gradient(135deg, #33405f, #24324f); }
    .stats {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 14px;
    }
    .stat {
      padding: 18px;
      border-radius: 18px;
      background: linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.02));
      border: 1px solid rgba(255,255,255,.08);
    }
    .stat .k {
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 10px;
    }
    .stat .v {
      font-size: 34px;
      font-weight: 800;
    }
    .stat .s {
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
    }
    .list {
      display: flex;
      flex-direction: column;
      gap: 10px;
      max-height: 440px;
      overflow: auto;
      padding-left: 4px;
    }
    .list-item {
      padding: 14px;
      border-radius: 16px;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.07);
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
    }
    .list-item .left { min-width: 0; }
    .list-item .name {
      font-weight: 700;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .list-item .meta {
      margin-top: 4px;
      color: var(--muted);
      font-size: 12px;
    }
    .badge {
      padding: 8px 10px;
      border-radius: 999px;
      background: rgba(122,162,255,.16);
      color: #b9cdff;
      font-size: 12px;
      white-space: nowrap;
    }
    .muted { color: var(--muted); }
    .small { font-size: 12px; }
    .footer-note { color: var(--muted); margin-top: auto; padding-top: 15px; font-size: 12px; }
    
    .pattern-box {
      background: rgba(255,184,77,0.1);
      border: 1px solid rgba(255,184,77,0.25);
      border-radius: 16px;
      padding: 16px;
      margin-top: auto;
    }
    .pattern-title {
      color: var(--warn);
      font-weight: 700;
      font-size: 14px;
      margin-bottom: 6px;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .pattern-body {
      color: #ffd8a8;
      font-size: 13px;
      line-height: 1.5;
    }

    @media (max-width: 1100px) {
      .hero, .controls, .stats { grid-template-columns: 1fr; }
      .span-8, .span-6, .span-4, .span-3 { grid-column: span 12; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="card">
        <div class="title">🚨 לוח בקרה - צבע אדום</div>
        <div class="sub">
          מערכת לניתוח התרעות היסטוריות וזיהוי דפוסים. אפשר לבחור עיר או אזור, לצפות בסטטיסטיקה מפורטת,
          לנתח את התפלגות השעות ולזהות שעות מועדות על בסיס נתוני עבר.
        </div>
        <div style="margin-top:16px" class="sub small" id="datasetMeta">טוען נתונים...</div>
      </div>
      <div class="card">
        <h3 style="margin-bottom:12px">מה חדש במערכת</h3>
        <div class="sub">
          • הוסרו טבלאות ארוכות לטובת ויזואליזציה<br>
          • נוסף תצוגת אזעקות אחרונות בעיר<br>
          • נוסף גרף התפלגות לפי שעות<br>
          • ניתוח שעות "חמות" בשבוע האחרון<br>
          • זיהוי פוטנציאל איום לפי דפוס היסטורי
        </div>
      </div>
    </div>

    <div class="card span-12" style="margin-bottom:20px;">
      <div class="controls">
        <div>
          <label for="citySelect">עיר / אזור</label>
          <input list="citiesList" id="citySelect" placeholder="התחל להקליד: אשדוד / אשקלון ...">
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
            <option value="30">30 ימים אחרונים</option>
            <option value="7">7 ימים אחרונים</option>
            <option value="90">90 ימים אחרונים</option>
            <option value="365">שנה אחרונה</option>
            <option value="all">כל בסיס הנתונים</option>
          </select>
        </div>
        <div style="display:flex; gap:10px; align-items:end;">
          <button class="btn-primary" id="applyBtn">הצג</button>
          <button class="btn-secondary" id="refreshBtn">רענן</button>
        </div>
      </div>
    </div>

    <div class="grid">
      <div class="card span-12">
        <h3 style="margin-bottom:16px">סיכום עבור העיר שנבחרה</h3>
        <div class="stats">
          <div class="stat">
            <div class="k">היום</div>
            <div class="v" id="statToday">—</div>
            <div class="s" id="statTodaySub">—</div>
          </div>
          <div class="stat">
            <div class="k">7 ימים אחרונים</div>
            <div class="v" id="statWeek">—</div>
            <div class="s" id="statWeekSub">—</div>
          </div>
          <div class="stat">
            <div class="k">30 ימים אחרונים</div>
            <div class="v" id="statMonth">—</div>
            <div class="s" id="statMonthSub">—</div>
          </div>
          <div class="stat">
            <div class="k">סה״כ בטווח</div>
            <div class="v" id="statTotal">—</div>
            <div class="s" id="statTotalSub">—</div>
          </div>
        </div>
      </div>

      <div class="card span-8">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px;">
          <h3>גרף התרעות לפי ימים</h3>
          <div class="muted small" id="chartCaption">—</div>
        </div>
        <canvas id="dailyChart" height="120"></canvas>
        <div class="footer-note">מציג את כמות ההתרעות בטווח התאריכים הנבחר.</div>
      </div>

      <div class="card span-4">
        <h3 style="margin-bottom:14px">אזעקות אחרונות בעיר</h3>
        <div class="list" id="latestAlertsList">
            </div>
        <div class="footer-note">מציג עד 10 אירועים אחרונים בטווח הנבחר.</div>
      </div>

      <div class="card span-8">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px;">
          <h3>התפלגות התרעות לפי שעות היממה (בטווח הנבחר)</h3>
        </div>
        <canvas id="hourlyChart" height="120"></canvas>
      </div>

      <div class="card span-4">
        <h3 style="margin-bottom:14px">ניתוח שעות ודפוסים</h3>
        
        <div style="margin-bottom: 20px;">
            <div class="muted small" style="margin-bottom: 10px;">השעות החמות ביותר (7 ימים מסוף הטווח):</div>
            <div class="list" id="weekHoursList">
                </div>
        </div>

        <div class="pattern-box">
            <div class="pattern-title">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line></svg>
                פוטנציאל - דפוס היסטורי
            </div>
            <div class="pattern-body">
                <strong>שימו לב: לא מדובר בתחזית מדעית או צבאית!</strong><br>
                <span id="patternText">—</span>
            </div>
        </div>
      </div>

      <div class="card span-12">
        <h3 style="margin-bottom:14px">הערים המובילות בטווח שנבחר</h3>
        <div class="list" style="flex-direction: row; flex-wrap: wrap;" id="topCitiesList"></div>
      </div>
    </div>
  </div>

<script>
let allCities = [];
let dailyChartObj = null;
let hourlyChartObj = null;
let datasetMeta = null;

function fmtNum(v) {
  return new Intl.NumberFormat('he-IL').format(v ?? 0);
}

function setText(id, value) {
  document.getElementById(id).textContent = value;
}

function parseISODate(dateStr) {
  const d = new Date(dateStr + 'T00:00:00');
  return d;
}

function shiftDays(dateStr, days) {
  const d = parseISODate(dateStr);
  d.setDate(d.getDate() + days);
  return d.toISOString().slice(0, 10);
}

async function getJson(url, options = {}) {
  const res = await fetch(url, options);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.json();
}

async function loadMeta() {
  datasetMeta = await getJson('/api/meta');
  const text = `רשומות: ${fmtNum(datasetMeta.total_events)} · ערים: ${fmtNum(datasetMeta.total_cities)} · עדכון: ${datasetMeta.refreshed_at || '—'} · טווח: ${datasetMeta.min_date || '—'} ← ${datasetMeta.max_date || '—'}`;
  setText('datasetMeta', text);

  if (datasetMeta.max_date) {
    document.getElementById('toDate').value = datasetMeta.max_date;
    document.getElementById('fromDate').value = shiftDays(datasetMeta.max_date, -29);
  }
}

async function loadCities() {
  const data = await getJson('/api/cities');
  allCities = data.cities;
  const dl = document.getElementById('citiesList');
  dl.innerHTML = allCities.map(c => `<option value="${c}"></option>`).join('');

  const preferred = ['אשדוד', 'אשקלון', 'באר שבע'];
  const first = preferred.find(x => allCities.includes(x)) || allCities[0] || '';
  document.getElementById('citySelect').value = first;
}

function applyPreset() {
  const preset = document.getElementById('preset').value;
  if (!datasetMeta?.max_date) return;
  if (preset === 'all') {
    document.getElementById('fromDate').value = datasetMeta.min_date;
    document.getElementById('toDate').value = datasetMeta.max_date;
    return;
  }
  const days = parseInt(preset, 10);
  document.getElementById('toDate').value = datasetMeta.max_date;
  document.getElementById('fromDate').value = shiftDays(datasetMeta.max_date, -(days - 1));
}

function renderTopCities(items) {
  const el = document.getElementById('topCitiesList');
  el.innerHTML = items.slice(0, 12).map((r, idx) => `
    <div class="list-item" style="width: calc(25% - 10px);">
      <div class="left">
        <div class="name">#${idx + 1} ${r.city}</div>
      </div>
      <div class="badge">${fmtNum(r.count)}</div>
    </div>
  `).join('');
}

function renderDailyChart(days, city) {
  const labels = days.map(x => x.date);
  const values = days.map(x => x.count);
  const ctx = document.getElementById('dailyChart');

  if (dailyChartObj) dailyChartObj.destroy();

  dailyChartObj = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        label: city,
        data: values,
        tension: 0.25,
        fill: true,
        backgroundColor: 'rgba(122, 162, 255, 0.1)',
        borderColor: '#7aa2ff',
        borderWidth: 2,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: { legend: { display: false }, tooltip: { intersect: false, mode: 'index' } },
      scales: {
        x: { ticks: { color: '#aebee4', maxRotation: 0, autoSkip: true }, grid: { color: 'rgba(255,255,255,.05)' } },
        y: { beginAtZero: true, ticks: { color: '#aebee4' }, grid: { color: 'rgba(255,255,255,.05)' } }
      }
    }
  });
  setText('chartCaption', `${city} · ${labels.length} ימים`);
}

function renderHourlyChart(hourlyCounts, city) {
  const labels = Array.from({length: 24}, (_, i) => `${i.toString().padStart(2, '0')}:00`);
  const ctx = document.getElementById('hourlyChart');

  if (hourlyChartObj) hourlyChartObj.destroy();

  hourlyChartObj = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'התרעות',
        data: hourlyCounts,
        backgroundColor: '#4f7cff',
        borderRadius: 4,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#aebee4' }, grid: { display: false } },
        y: { beginAtZero: true, ticks: { color: '#aebee4' }, grid: { color: 'rgba(255,255,255,.05)' } }
      }
    }
  });
}

function renderSummary(data) {
  setText('statToday', fmtNum(data.summary.today));
  setText('statWeek', fmtNum(data.summary.last_7_days));
  setText('statMonth', fmtNum(data.summary.last_30_days));
  setText('statTotal', fmtNum(data.summary.total_in_range));

  setText('statTodaySub', `עבור ${data.summary.today_date || '—'}`);
  setText('statWeekSub', `שבוע שיא: ${data.summary.best_week?.period || '—'}`);
  setText('statMonthSub', `חודש שיא: ${data.summary.best_month?.period || '—'}`);
  setText('statTotalSub', `יום שיא: ${data.summary.best_day?.period || '—'}`);

  // תרשימים
  renderDailyChart(data.daily, data.city);
  renderHourlyChart(data.hourly_counts, data.city);

  // אזעקות אחרונות
  const latestHtml = data.latest_alerts.map(a => `
    <div class="list-item">
      <div class="left"><div class="name" style="font-size: 16px;">${a.time}</div></div>
      <div class="badge">${a.date}</div>
    </div>
  `).join('') || '<div class="muted small" style="padding:10px;">אין נתונים בטווח הנבחר.</div>';
  document.getElementById('latestAlertsList').innerHTML = latestHtml;

  // שעות נפוצות בשבוע האחרון
  const weekHtml = data.top_week_hours.map(h => `
    <div class="list-item" style="padding: 10px 14px;">
        <div class="left"><div class="name">שעה ${h.hour}</div></div>
        <div class="badge" style="background: rgba(75, 209, 139, 0.15); color: #4bd18b;">${h.count} אירועים</div>
    </div>
  `).join('') || '<div class="muted small">אין אזעקות בשבוע האחרון של הטווח.</div>';
  document.getElementById('weekHoursList').innerHTML = weekHtml;

  // דפוס פוטנציאלי
  document.getElementById('patternText').textContent = data.pattern_text;
}

async function loadDashboard() {
  const city = document.getElementById('citySelect').value.trim();
  const from = document.getElementById('fromDate').value;
  const to = document.getElementById('toDate').value;
  if (!city) {
    alert('בחר עיר או אזור');
    return;
  }
  const params = new URLSearchParams({ city, from, to });
  
  try {
    const data = await getJson(`/api/city-stats?${params.toString()}`);
    renderSummary(data);

    const topCities = await getJson(`/api/top-cities?from=${from}&to=${to}&limit=12`);
    renderTopCities(topCities.items);
  } catch (err) {
    console.error("Failed to load dashboard data:", err);
  }
}

async function refreshBackend() {
  setText('datasetMeta', 'מרענן נתונים...');
  await getJson('/api/refresh', { method: 'POST' });
  await loadMeta();
  await loadCities();
  await loadDashboard();
}

async function bootstrap() {
  await loadMeta();
  await loadCities();
  await loadDashboard();

  document.getElementById('applyBtn').addEventListener('click', loadDashboard);
  document.getElementById('refreshBtn').addEventListener('click', refreshBackend);
  document.getElementById('preset').addEventListener('change', () => {
    applyPreset();
    loadDashboard();
  });
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
    time_str: str
    hour: int
    weekday: int
    cities: list[str]
    threat: int

class DataStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.events: list[EventRecord] = []
        self.city_daily: dict[str, Counter[str]] = defaultdict(Counter)
        self.city_weekly: dict[str, Counter[str]] = defaultdict(Counter)
        self.city_monthly: dict[str, Counter[str]] = defaultdict(Counter)
        self.city_events: dict[str, list[EventRecord]] = defaultdict(list)
        self.all_cities: list[str] = []
        self.min_date: str | None = None
        self.max_date: str | None = None
        self.last_refresh = 0

    def ensure_loaded(self, force: bool = False) -> None:
        now = time.time()
        with self.lock:
            if not force and self.events and (now - self.last_refresh) < REFRESH_SECONDS:
                return
            raw = self._download_or_load(force=force)
            self._build_indexes(raw)
            self.last_refresh = now

    def _download_or_load(self, force: bool = False) -> Any:
        if not force and DATA_FILE.exists():
            age = time.time() - DATA_FILE.stat().st_mtime
            if age < REFRESH_SECONDS:
                return json.loads(DATA_FILE.read_text(encoding="utf-8"))

        req = Request(
            DATA_URL,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json, text/plain, */*",
            },
        )
        with urlopen(req, timeout=60) as resp:
            raw_bytes = resp.read()

        DATA_FILE.write_bytes(raw_bytes)
        META_FILE.write_text(
            json.dumps(
                {"refreshed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return json.loads(raw_bytes.decode("utf-8"))

    def _build_indexes(self, raw: Any) -> None:
        events: list[EventRecord] = []
        city_daily: dict[str, Counter[str]] = defaultdict(Counter)
        city_weekly: dict[str, Counter[str]] = defaultdict(Counter)
        city_monthly: dict[str, Counter[str]] = defaultdict(Counter)
        city_events: dict[str, list[EventRecord]] = defaultdict(list)
        city_set: set[str] = set()
        
        min_date = None
        max_date = None
        seen: set[tuple[int, tuple[str, ...]]] = set()

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

            key = (ts, tuple(cities_clean))
            if key in seen:
                continue
            seen.add(key)

            # Перевод времени в таймзону Израиля
            dt = datetime.fromtimestamp(ts, tz=ISRAEL_TZ)
            date = dt.strftime("%Y-%m-%d")
            iso_year, iso_week, _ = dt.isocalendar()
            week = f"{iso_year}-W{iso_week:02d}"
            month = dt.strftime("%Y-%m")
            time_str = dt.strftime("%H:%M")
            hour = dt.hour
            weekday = dt.weekday()

            record = EventRecord(
                ts=ts, date=date, week=week, month=month, 
                time_str=time_str, hour=hour, weekday=weekday,
                cities=cities_clean, threat=threat
            )
            events.append(record)

            if min_date is None or date < min_date:
                min_date = date
            if max_date is None or date > max_date:
                max_date = date

            for city in cities_clean:
                city_set.add(city)
                city_daily[city][date] += 1
                city_weekly[city][week] += 1
                city_monthly[city][month] += 1
                city_events[city].append(record)

        self.events = sorted(events, key=lambda x: x.ts)
        self.city_daily = dict(city_daily)
        self.city_weekly = dict(city_weekly)
        self.city_monthly = dict(city_monthly)
        self.city_events = dict(city_events)
        self.all_cities = sorted(city_set)
        self.min_date = min_date
        self.max_date = max_date

    def meta(self) -> dict[str, Any]:
        refreshed_at = None
        if META_FILE.exists():
            try:
                refreshed_at = json.loads(META_FILE.read_text(encoding="utf-8")).get("refreshed_at")
            except Exception:
                pass
        return {
            "total_events": len(self.events),
            "total_cities": len(self.all_cities),
            "min_date": self.min_date,
            "max_date": self.max_date,
            "refreshed_at": refreshed_at,
        }

store = DataStore()

def daterange_days(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    days = []
    cur = s
    while cur <= e:
        days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return days

def normalize_city(city: str) -> str:
    city = city.strip()
    for he, en in CITY_ALIASES.items():
        if city == en:
            return he
    return city

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

@app.get("/api/top-cities")
def api_top_cities():
    store.ensure_loaded()
    start = request.args.get("from") or store.min_date
    end = request.args.get("to") or store.max_date
    limit = int(request.args.get("limit", 12))

    items = []
    for city, daily in store.city_daily.items():
        total = sum(v for k, v in daily.items() if start <= k <= end)
        if total > 0:
            items.append({"city": city, "count": total})

    items.sort(key=lambda x: (-x["count"], x["city"]))
    return jsonify({"items": items[:limit]})

@app.get("/api/city-stats")
def api_city_stats():
    store.ensure_loaded()
    city = normalize_city(request.args.get("city", ""))
    if not city:
        return jsonify({"error": "city is required"}), 400
    if city not in store.city_daily:
        return jsonify({"error": f"city not found: {city}"}), 404

    start = request.args.get("from") or store.min_date
    end = request.args.get("to") or store.max_date

    # Основные данные города
    daily_counter = store.city_daily[city]
    weekly_counter = store.city_weekly[city]
    monthly_counter = store.city_monthly[city]
    city_records = store.city_events.get(city, [])

    # Фильтрация записей по диапазону
    filtered_records = [r for r in city_records if start <= r.date <= end]

    # Ежедневные отсчеты (для первого графика)
    daily_rows = [{"date": d, "count": daily_counter.get(d, 0)} for d in daterange_days(start, end)]
    total_in_range = sum(row["count"] for row in daily_rows)

    # Статистика "Сирен за последнее время" (до 10 штук)
    latest_alerts = [{"date": r.date, "time": r.time_str} for r in filtered_records[-10:]]
    latest_alerts.reverse()  # Новые сверху

    # Распределение по часам (для второго графика)
    hourly_counts = [0] * 24
    for r in filtered_records:
        hourly_counts[r.hour] += 1

    # Анализ 7-ми дней от конца выбранного диапазона (Самые частые часы)
    try:
        end_dt = datetime.strptime(end, "%Y-%m-%d")
        week_start_dt = end_dt - timedelta(days=6)
        week_start_str = week_start_dt.strftime("%Y-%m-%d")
    except ValueError:
        week_start_str = start

    week_records = [r for r in filtered_records if week_start_str <= r.date <= end]
    week_hourly = [0] * 24
    for r in week_records:
        week_hourly[r.hour] += 1

    top_week_hours = []
    if sum(week_hourly) > 0:
        sorted_hours = sorted(enumerate(week_hourly), key=lambda x: x[1], reverse=True)
        # Берем топ-3 часа
        top_week_hours = [{"hour": f"{h:02d}:00", "count": c} for h, c in sorted_hours[:3] if c > 0]

    # Анализ паттернов (ПО ВСЕЙ истории города, не зависимо от выбранного диапазона)
    all_hourly = [0] * 24
    for r in city_records:
        all_hourly[r.hour] += 1
    
    if sum(all_hourly) > 0:
        best_hist_hour = max(range(24), key=lambda i: all_hourly[i])
        pattern_text = f"על בסיס היסטוריה מלאה של נתוני העיר, נראה כי רוב אירועי ההתרעה מתרכזים סביב השעה {best_hist_hour:02d}:00."
    else:
        pattern_text = "אין מספיק נתונים היסטוריים לניתוח דפוסים בעיר זו."

    # Сводка (Сегодня / Неделя / Месяц)
    today_date = store.max_date
    last_7_start = (datetime.strptime(today_date, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
    last_30_start = (datetime.strptime(today_date, "%Y-%m-%d") - timedelta(days=29)).strftime("%Y-%m-%d")

    today_val = daily_counter.get(today_date, 0)
    week_val = sum(v for k, v in daily_counter.items() if last_7_start <= k <= today_date)
    month_val = sum(v for k, v in daily_counter.items() if last_30_start <= k <= today_date)

    best_day = max(daily_counter.items(), key=lambda x: (x[1], x[0])) if daily_counter else None
    best_week = max(weekly_counter.items(), key=lambda x: (x[1], x[0])) if weekly_counter else None
    best_month = max(monthly_counter.items(), key=lambda x: (x[1], x[0])) if monthly_counter else None

    return jsonify({
        "city": city,
        "daily": daily_rows,
        "hourly_counts": hourly_counts,
        "latest_alerts": latest_alerts,
        "top_week_hours": top_week_hours,
        "pattern_text": pattern_text,
        "summary": {
            "today": today_val,
            "today_date": today_date,
            "last_7_days": week_val,
            "last_30_days": month_val,
            "total_in_range": total_in_range,
            "best_day": {"period": best_day[0], "count": best_day[1]} if best_day else None,
            "best_week": {"period": best_week[0], "count": best_week[1]} if best_week else None,
            "best_month": {"period": best_month[0], "count": best_month[1]} if best_month else None,
        },
    })

if __name__ == "__main__":
    import os

    store.ensure_loaded()
    port = int(os.environ.get("PORT", 5000))
    print(f"Dashboard ready: http://127.0.0.1:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)