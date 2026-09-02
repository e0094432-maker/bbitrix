#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Дашборд активности сотрудников. Локальный/облачный веб-сервис.

КАК ЗАПУСТИТЬ:
  python app.py --webhook "https://ваш-портал.bitrix24.kz/rest/USER/CODE/"
  (или переменные окружения BITRIX_WEBHOOK_URL и PORT — так работает на Render)

ДВЕ ВКЛАДКИ:
  - Сводка: по каждому сотруднику — сколько сделок в работе сейчас,
    сколько закрыто (успех/провал) за выбранный период, конверсия.
    Клик по сотруднику — разбивка его активных сделок по стадиям.
  - Звонки: список звонков за период, фильтр по сотруднику и
    успех/провал, поиск по номеру, ссылка на сделку в Bitrix24.

ВЫБОР ПЕРИОДА: кнопки-пресеты (Сегодня/Вчера/Неделя/Месяц) + два поля
для ручного выбора дат — применяется к обеим вкладкам сразу.
"""

import argparse
import json
import os
import re
import sys
import threading
import time
import urllib.request
import urllib.error
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CATEGORY_NAME = "Досудебный отдел"
MATCH_THRESHOLD = 0.55
REQUEST_DELAY = 0.3
MAX_RETRIES = 6

SLOW_REFRESH_SECONDS = 300     # активные сделки "сейчас" + время прихода — раз в 5 минут
RANGE_REFRESH_SECONDS = 120    # звонки/закрытые за выбранный период — раз в 2 минуты
ACTIVE_DEALS_WORKERS = 6       # параллельных запросов при подсчёте активных сделок (снижено, чтобы не ловить 429)

WEBHOOK_URL = None
PORTAL_DOMAIN = None


# ---------------------------------------------------------------------------
# Bitrix24 REST helpers
# ---------------------------------------------------------------------------
def call_bitrix(method, params):
    url = WEBHOOK_URL.rstrip("/") + "/" + method + ".json"
    data = urllib.parse.urlencode(params, doseq=True).encode("utf-8")
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, data=data, method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8")
                result = json.loads(body)
                if "error" in result:
                    err_code = str(result.get("error", ""))
                    err_desc = result.get("error_description", err_code)
                    if err_code in ("QUERY_LIMIT_EXCEEDED", "OPERATION_TIME_LIMIT"):
                        time.sleep(3 + attempt * 2)
                        continue
                    if err_code in ("INSUFFICIENT_SCOPE", "ERROR_METHOD_NOT_FOUND", "NOT_FOUND") or "insufficient_scope" in str(err_desc).lower():
                        return None, None, err_desc
                    return None, None, err_desc
                return result.get("result"), result.get("total"), None
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="ignore")
            last_error = f"HTTP {e.code}: {body_text}"
            if e.code == 429 or "operation_time_limit" in body_text.lower() or "query_limit_exceeded" in body_text.lower():
                time.sleep(3 + attempt * 2)
                continue
            if "insufficient_scope" in body_text.lower() or "method_not_found" in body_text.lower() or e.code == 401 or e.code == 404:
                return None, None, last_error
            time.sleep(1.0 * attempt)
        except Exception as e:
            last_error = str(e)
            time.sleep(1.0 * attempt)
    return None, None, last_error


def get_all_pages(method, params, max_pages=200):
    all_items = []
    start = 0
    pages = 0
    while True:
        p = dict(params)
        p["start"] = start
        result, total, error = call_bitrix(method, p)
        time.sleep(REQUEST_DELAY)
        if error:
            return None, error
        if not result:
            break
        all_items.extend(result)
        pages += 1
        if total is not None and len(all_items) >= int(total):
            break
        if len(result) == 0:
            break
        if pages >= max_pages:
            print(f"[!] {method}: достигнут лимит {max_pages} страниц ({len(all_items)} записей) — останавливаюсь.")
            break
        start += 50
    return all_items, None


def fetch_all_users():
    all_users = []
    seen_ids = set()
    for active_filter in ({"ACTIVE": "Y"},):
        params = dict(active_filter)
        params["ADMIN_MODE"] = "Y"
        users, error = get_all_pages("user.get", params)
        if error:
            users, error = get_all_pages("user.get", active_filter)
        if error:
            print(f"[ОШИБКА] user.get: {error}")
            continue
        for u in users or []:
            if u["ID"] not in seen_ids:
                seen_ids.add(u["ID"])
                all_users.append(u)
    return all_users


def full_name(user):
    parts = [user.get("NAME", ""), user.get("LAST_NAME", "")]
    return " ".join(p for p in parts if p).strip()


def is_active_user(user):
    val = user.get("ACTIVE", "Y")
    return str(val).strip().upper() in ("Y", "TRUE", "1")


def find_deal_category_id():
    categories, error = get_all_pages("crm.dealcategory.list", {})
    if error:
        print(f"[ОШИБКА] crm.dealcategory.list: {error}")
        return None, None
    scored = []
    for c in categories:
        name = c.get("NAME", "")
        s = SequenceMatcher(None, CATEGORY_NAME.lower(), name.lower()).ratio()
        scored.append((s, c["ID"], name))
    scored.sort(key=lambda x: -x[0])
    best_score, best_id, best_name = scored[0]
    if best_score < MATCH_THRESHOLD:
        print(f"[ОШИБКА] воронка '{CATEGORY_NAME}' не найдена (лучшее совпадение {best_score:.2f})")
        return None, None
    return best_id, best_name


def fetch_stages(category_id):
    """Список стадий воронки в правильном порядке (STATUS_ID -> NAME)."""
    stages, total, error = call_bitrix("crm.dealcategory.stage.list", {"id": category_id})
    if error:
        print(f"[ОШИБКА] crm.dealcategory.stage.list: {error}")
        return {}
    stages = sorted(stages or [], key=lambda s: int(s.get("SORT", 0)))
    return {s["STATUS_ID"]: s["NAME"] for s in stages}


def get_active_deal_count(user_id, category_id):
    result, total, error = call_bitrix(
        "crm.deal.list",
        {"filter[ASSIGNED_BY_ID]": user_id, "filter[CLOSED]": "N", "filter[CATEGORY_ID]": category_id, "select[]": "ID"},
    )
    if error:
        return 0, error
    return (total if total is not None else len(result or [])), None


def fetch_employee_stage_breakdown(user_id, category_id, stage_names):
    """Разбивка активных сделок ОДНОГО сотрудника по стадиям — считается
    по клику, дёшево (один человек), поэтому можно делать "живьём"."""
    deals, error = get_all_pages(
        "crm.deal.list",
        {"filter[ASSIGNED_BY_ID]": user_id, "filter[CLOSED]": "N", "filter[CATEGORY_ID]": category_id, "select[]": ["STAGE_ID"]},
        max_pages=100,
    )
    if error:
        return [], error
    counts = {}
    for d in deals or []:
        sid = d.get("STAGE_ID", "?")
        counts[sid] = counts.get(sid, 0) + 1
    result = [{"stage_id": sid, "stage_name": stage_names.get(sid, sid), "count": cnt} for sid, cnt in counts.items()]
    result.sort(key=lambda x: -x["count"])
    return result, None


def fetch_attendance_today(user_ids):
    global TIMEMAN_AVAILABLE
    if TIMEMAN_AVAILABLE is False:
        return {}
    result = {}
    for uid in user_ids:
        status, _, error = call_bitrix("timeman.status", {"USER_ID": uid})
        if error:
            if TIMEMAN_AVAILABLE is None:
                print(f"[!] timeman.status недоступен ({error}). Пропускаю время прихода/перерыва для всех.")
            TIMEMAN_AVAILABLE = False
            return result
        TIMEMAN_AVAILABLE = True
        if isinstance(status, dict):
            result[uid] = {
                "start": status.get("TIME_START", "") or "",
                "break": status.get("START_ENTRY", "") or status.get("BREAK_START", "") or "",
            }
        time.sleep(REQUEST_DELAY)
    return result


TIMEMAN_AVAILABLE = None
VOXIMPLANT_AVAILABLE = None


def parse_bitrix_datetime(s):
    """Парсит дату из ответа Bitrix24 (ISO или ДД.ММ.ГГГГ). Возвращает
    naive datetime (без временной зоны) или None."""
    if not s:
        return None
    s = str(s)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.replace(tzinfo=None)
    except ValueError:
        pass
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def fetch_calls_in_range(start_dt, end_dt):
    """Звонки за произвольный период. НАДЁЖНЫЙ способ, не зависящий от
    того, сработает ли фильтр Bitrix24 по дате: запрашиваем звонки от
    новых к старым (ORDER=DESC) и сами останавливаемся, как только видим
    звонок старше начала периода — дальше все звонки будут ещё старше."""
    date_fmt = "%d.%m.%Y %H:%M:%S"
    calls = []
    start_offset = 0
    page_size = 50
    max_iterations = 150

    for _ in range(max_iterations):
        params = {
            "FILTER[CALL_START_DATE_from]": start_dt.strftime(date_fmt),
            "FILTER[CALL_START_DATE_to]": end_dt.strftime(date_fmt),
            "SORT": "CALL_START_DATE",
            "ORDER": "DESC",
            "start": start_offset,
        }
        result, total, error = call_bitrix("voximplant.statistic.get", params)
        time.sleep(REQUEST_DELAY)
        if error:
            return None, error
        if not result:
            break

        reached_lower_bound = False
        for item in result:
            item_dt = parse_bitrix_datetime(item.get("CALL_START_DATE"))
            if item_dt is not None:
                if item_dt < start_dt:
                    reached_lower_bound = True
                    break
                if item_dt > end_dt:
                    continue
            calls.append(item)

        if reached_lower_bound or len(result) < page_size:
            break
        start_offset += page_size
    else:
        print(f"[!] fetch_calls_in_range: достигнут защитный лимит {max_iterations} страниц")

    incoming, outgoing, calls_by_user = {}, {}, {}
    for item in calls:
        uid = item.get("PORTAL_USER_ID")
        if not uid:
            continue
        call_type = str(item.get("CALL_TYPE", ""))
        failed_code = str(item.get("CALL_FAILED_CODE", "0"))
        duration = item.get("CALL_DURATION", "0")
        try:
            duration = int(duration)
        except (ValueError, TypeError):
            duration = 0
        is_failed = failed_code != "0" or duration == 0

        if call_type == "2":
            incoming[uid] = incoming.get(uid, 0) + 1
        else:
            outgoing[uid] = outgoing.get(uid, 0) + 1

        entity_id = item.get("CRM_ENTITY_ID")
        entity_type = str(item.get("CRM_ENTITY_TYPE", ""))
        deal_link = None
        if entity_id and entity_type.upper() in ("DEAL", "3", "CRM_DEAL"):
            deal_link = f"https://{PORTAL_DOMAIN}/crm/deal/details/{entity_id}/" if PORTAL_DOMAIN else None

        calls_by_user.setdefault(uid, []).append({
            "time": item.get("CALL_START_DATE", ""),
            "direction": "in" if call_type == "2" else "out",
            "duration": duration,
            "failed": is_failed,
            "phone": item.get("PHONE_NUMBER", ""),
            "deal_link": deal_link,
        })

    return {"incoming": incoming, "outgoing": outgoing, "calls_by_user": calls_by_user}, None


def fetch_closed_in_range(category_id, date_from_str, date_to_str):
    """Закрытые сделки (успех/провал) за период, по ответственному."""
    period_start = f"{date_from_str}T00:00:00"
    period_end = f"{date_to_str}T00:00:00"
    items, error = get_all_pages(
        "crm.deal.list",
        {
            "filter[CLOSED]": "Y",
            "filter[>=CLOSEDATE]": period_start,
            "filter[<CLOSEDATE]": period_end,
            "filter[CATEGORY_ID]": category_id,
            "select[]": ["ASSIGNED_BY_ID", "STAGE_SEMANTIC_ID"],
        },
        max_pages=400,
    )
    if error:
        return {}, error
    won, lost = {}, {}
    for item in items or []:
        uid = item.get("ASSIGNED_BY_ID")
        if not uid:
            continue
        if item.get("STAGE_SEMANTIC_ID") == "S":
            won[uid] = won.get(uid, 0) + 1
        else:
            lost[uid] = lost.get(uid, 0) + 1
    return {"won": won, "lost": lost}, None


# ---------------------------------------------------------------------------
# Общее состояние
# ---------------------------------------------------------------------------
STATE_LOCK = threading.Lock()
STATE = {
    "users_by_id": {},
    "category_name": "",
    "category_id": None,
    "stage_names": {},
    "active_deals": {},
    "attendance": {},
    "last_slow_update": None,
    "range_cache": {},       # {"from_to": {"calls":..., "closed":..., "computed_at":..., "call_error":..., "closed_error":...}}
    "range_in_progress": set(),
    "errors": [],
}
RANGE_LOCK = threading.Lock()


def slow_refresh_loop():
    category_id, category_name = find_deal_category_id()
    stage_names = fetch_stages(category_id) if category_id else {}
    with STATE_LOCK:
        STATE["category_name"] = category_name or CATEGORY_NAME
        STATE["category_id"] = category_id
        STATE["stage_names"] = stage_names

    while True:
        try:
            t_start = time.time()
            users = fetch_all_users()
            users_by_id = {u["ID"]: full_name(u) for u in users if full_name(u)}
            active_deals = {}
            if category_id:
                with ThreadPoolExecutor(max_workers=ACTIVE_DEALS_WORKERS) as pool:
                    futures = {pool.submit(get_active_deal_count, uid, category_id): uid for uid in users_by_id}
                    for future in as_completed(futures):
                        uid = futures[future]
                        try:
                            count, error = future.result()
                            if not error:
                                active_deals[uid] = count
                        except Exception:
                            pass

            relevant_ids = [uid for uid, c in active_deals.items() if c and c > 0]
            attendance = fetch_attendance_today(relevant_ids) if relevant_ids else {}

            with STATE_LOCK:
                STATE["users_by_id"] = users_by_id
                STATE["active_deals"] = active_deals
                STATE["attendance"] = attendance
                STATE["last_slow_update"] = datetime.now().strftime("%H:%M:%S")
            elapsed = time.time() - t_start
            print(f"[slow refresh] {datetime.now().strftime('%H:%M:%S')} — сотрудников: {len(users_by_id)}, с активными сделками: {len(relevant_ids)}, заняло {elapsed:.1f} сек")
        except Exception as e:
            print(f"[slow refresh] ОШИБКА: {e}")

        time.sleep(SLOW_REFRESH_SECONDS)


def refresh_range(date_from, date_to):
    with STATE_LOCK:
        category_id = STATE["category_id"]
    if not category_id:
        return

    start_dt = datetime.strptime(date_from, "%Y-%m-%d")
    # "по" включительно — конец периода это НАЧАЛО СЛЕДУЮЩЕГО дня после date_to,
    # иначе при date_from == date_to получается нулевой по ширине интервал (0 звонков всегда)
    end_dt = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
    date_to_exclusive = end_dt.strftime("%Y-%m-%d")

    calls_data, call_error = fetch_calls_in_range(start_dt, end_dt)
    closed_data, closed_error = fetch_closed_in_range(category_id, date_from, date_to_exclusive)

    key = f"{date_from}_{date_to}"
    with STATE_LOCK:
        STATE["range_cache"][key] = {
            "calls": calls_data or {"incoming": {}, "outgoing": {}, "calls_by_user": {}},
            "closed": closed_data or {"won": {}, "lost": {}},
            "call_error": call_error,
            "closed_error": closed_error,
            "computed_at": datetime.now().strftime("%H:%M:%S"),
        }
    calls_count = sum((calls_data or {}).get("incoming", {}).values()) + sum((calls_data or {}).get("outgoing", {}).values())
    closed_count = sum((closed_data or {}).get("won", {}).values()) + sum((closed_data or {}).get("lost", {}).values())
    print(f"[range refresh] {date_from}..{date_to} — звонков: {calls_count}, закрыто: {closed_count}")


def get_range_data(date_from, date_to):
    """Отдаёт кэш за период, запускает фоновый пересчёт если нужно.
    Не блокирует страницу — сразу отвечает тем, что есть (может быть пусто
    при самом первом запросе этого периода)."""
    key = f"{date_from}_{date_to}"
    with STATE_LOCK:
        entry = STATE["range_cache"].get(key)

    if entry is None:
        with RANGE_LOCK:
            already_running = key in STATE["range_in_progress"]
            if not already_running:
                STATE["range_in_progress"].add(key)
        if not already_running:
            def _bg(f=date_from, t=date_to, k=key):
                try:
                    refresh_range(f, t)
                except Exception as e:
                    print(f"[range on-demand] ОШИБКА: {e}")
                finally:
                    with RANGE_LOCK:
                        STATE["range_in_progress"].discard(k)
            threading.Thread(target=_bg, daemon=True).start()
        return None, True

    return entry, False


def range_refresh_loop():
    """Раз в RANGE_REFRESH_SECONDS обновляет данные для уже запрошенных
    периодов, чтобы они не протухали, пока страница открыта."""
    while True:
        time.sleep(RANGE_REFRESH_SECONDS)
        with STATE_LOCK:
            keys = list(STATE["range_cache"].keys())
        for key in keys:
            try:
                date_from, date_to = key.split("_", 1)
                refresh_range(date_from, date_to)
            except Exception as e:
                print(f"[range refresh] ОШИБКА для {key}: {e}")


# ---------------------------------------------------------------------------
# Веб-сервер
# ---------------------------------------------------------------------------
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Активность сотрудников</title>
<style>
  :root {
    --accent: #4f46e5; --accent-light: #eef2ff;
    --green: #16a34a; --green-light: #ecfdf3;
    --red: #dc2626; --red-light: #fef2f2;
    --amber: #b45309; --amber-light: #fffbeb;
    --text: #1f2937; --muted: #6b7280; --border: #e5e7eb; --bg: #f8f9fc;
  }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; background: var(--bg); margin: 0; padding: 28px; color: var(--text); }
  h1 { font-size: 22px; font-weight: 700; margin: 0; }
  .subtitle { color: var(--muted); font-size: 13px; margin-top: 4px; }

  .tabs { display: flex; gap: 6px; margin: 18px 0; border-bottom: 1px solid var(--border); }
  .tab-btn { background: none; border: none; padding: 10px 18px; font-size: 14px; font-weight: 600; color: var(--muted); cursor: pointer; border-bottom: 2px solid transparent; }
  .tab-btn.active { color: var(--accent); border-bottom-color: var(--accent); }
  .tab-panel { display: none; }
  .tab-panel.active { display: block; }

  .date-controls { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; margin-bottom: 14px; }
  .preset-btn { background: white; border: 1px solid var(--border); border-radius: 999px; padding: 7px 14px; font-size: 12.5px; font-weight: 600; cursor: pointer; color: var(--text); }
  .preset-btn:hover { background: var(--accent-light); }
  .preset-btn.active { background: var(--accent); color: white; border-color: var(--accent); }
  .date-box { display: flex; align-items: center; gap: 6px; background: white; padding: 6px 12px; border-radius: 10px; border: 1px solid var(--border); }
  .date-box label { font-size: 12px; color: var(--muted); font-weight: 600; }
  input[type=date] { border: none; font-size: 13px; font-family: inherit; color: var(--text); background: transparent; }
  .search-box { display: flex; align-items: center; gap: 6px; background: white; padding: 6px 12px; border-radius: 10px; border: 1px solid var(--border); }
  .search-box input { border: none; font-size: 13px; font-family: inherit; outline: none; min-width: 180px; }
  .export-btn { background: white; border: 1px solid var(--border); border-radius: 10px; padding: 8px 16px; font-size: 13px; font-weight: 600; cursor: pointer; }
  .export-btn:hover { background: #f3f4f6; }

  .live-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); display: inline-block; margin-right: 6px; animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: .35; } }
  .status { font-size: 12px; color: var(--muted); margin-left: auto; }
  .warning-banner { background: var(--amber-light); border: 1px solid #fcd34d; color: var(--amber); padding: 10px 16px; border-radius: 10px; font-size: 13px; margin-bottom: 12px; }
  .loading-banner { background: var(--accent-light); border: 1px solid #c7d2fe; color: var(--accent); padding: 10px 16px; border-radius: 10px; font-size: 13px; margin-bottom: 12px; }

  .stats-row { display: flex; gap: 14px; margin-bottom: 20px; flex-wrap: wrap; }
  .stat-card { background: white; border-radius: 14px; padding: 16px 20px; min-width: 140px; box-shadow: 0 1px 3px rgba(0,0,0,.05); border: 1px solid var(--border); }
  .stat-card .num { font-size: 24px; font-weight: 700; }
  .stat-card .label { font-size: 12px; color: var(--muted); margin-top: 2px; }
  .stat-card.green .num { color: var(--green); }
  .stat-card.red .num { color: var(--red); }
  .stat-card.accent .num { color: var(--accent); }

  .table-card { background: white; border-radius: 16px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,.06); border: 1px solid var(--border); margin-bottom: 20px; }
  table { border-collapse: collapse; width: 100%; }
  th, td { padding: 11px 14px; text-align: center; font-size: 13px; }
  th { background: var(--accent); color: white; font-weight: 600; font-size: 11.5px; text-transform: uppercase; letter-spacing: .03em; cursor: pointer; user-select: none; }
  th:hover { background: #433ccb; }
  td:first-child, th:first-child { text-align: left; }
  tbody tr { border-bottom: 1px solid var(--border); }
  tbody tr:hover { background: #fafafa; }
  tbody tr.low-load { background: var(--green-light); }
  tbody tr.expand-row td { background: #fafbff; padding: 14px 20px; text-align: left; }
  .name-cell { font-weight: 600; cursor: pointer; }
  .name-cell:hover { color: var(--accent); }
  .name-link { color: inherit; text-decoration: none; }
  .badge { display: inline-block; padding: 3px 10px; border-radius: 999px; font-size: 11.5px; font-weight: 600; }
  .badge.rec { background: var(--green); color: white; }
  .in-call { color: var(--accent); font-weight: 600; }
  .out-call { color: #7c3aed; font-weight: 600; }
  .won { color: var(--green); font-weight: 600; }
  .lost { color: var(--red); font-weight: 600; }
  .conv-cell { font-weight: 700; }
  .conv-high { color: var(--green); }
  .conv-mid { color: var(--amber); }
  .conv-low { color: var(--red); }
  .empty-cell { color: #cbd5e1; }
  .stage-chip { display: inline-block; background: var(--accent-light); color: var(--accent); padding: 4px 10px; border-radius: 8px; font-size: 12px; margin: 3px; }

  .calls-toolbar { display: flex; gap: 10px; margin-bottom: 14px; flex-wrap: wrap; align-items: center; }
  .calls-filter-btn { background: white; border: 1px solid var(--border); border-radius: 999px; padding: 6px 14px; font-size: 12.5px; font-weight: 600; cursor: pointer; }
  .calls-filter-btn.active { background: var(--text); color: white; }
  .call-row-failed { color: var(--red); }
  .call-dir-badge { font-weight: 700; }
</style>
</head>
<body>
  <div>
    <h1>📊 Активность сотрудников</h1>
    <div class="subtitle" id="subtitle">воронка «Досудебный отдел»</div>
  </div>

  <div class="tabs">
    <button class="tab-btn active" data-tab="summary">Сводка</button>
    <button class="tab-btn" data-tab="calls">Звонки</button>
  </div>

  <div class="date-controls">
    <button class="preset-btn" data-preset="today">Сегодня</button>
    <button class="preset-btn" data-preset="yesterday">Вчера</button>
    <button class="preset-btn" data-preset="yesterday_today">Вчера+сегодня</button>
    <button class="preset-btn" data-preset="week">Неделя</button>
    <button class="preset-btn" data-preset="month">Месяц</button>
    <div class="date-box"><label>с</label><input type="date" id="rangeFrom"></div>
    <div class="date-box"><label>по</label><input type="date" id="rangeTo"></div>
    <button id="exportBtn" class="export-btn">⬇ Экспорт CSV</button>
    <div class="status" id="status"><span class="live-dot"></span>обновление...</div>
  </div>

  <div id="banners"></div>
  <div class="stats-row" id="statsRow"></div>

  <div id="tab-summary" class="tab-panel active">
    <div class="search-box" style="margin-bottom:14px; display:inline-flex;">
      <span>🔍</span>
      <input type="text" id="searchBox" placeholder="Поиск по сотруднику...">
    </div>
    <div class="table-card">
      <table id="summaryTbl">
        <thead>
          <tr>
            <th data-sort="name">Сотрудник</th>
            <th data-sort="active_deals">Сделок в работе</th>
            <th data-sort="calls_total">Звонков</th>
            <th data-sort="closed_won">Закрыто успех</th>
            <th data-sort="closed_lost">Закрыто провал</th>
            <th data-sort="conversion">Конверсия</th>
            <th>Начало работы</th>
            <th>Перерыв</th>
            <th>Рекомендация</th>
          </tr>
        </thead>
        <tbody id="summaryBody"></tbody>
      </table>
    </div>
  </div>

  <div id="tab-calls" class="tab-panel">
    <div class="calls-toolbar">
      <select id="callsEmployeeFilter" class="calls-filter-btn"><option value="">Все сотрудники</option></select>
      <button class="calls-filter-btn active" data-callfilter="all">Все</button>
      <button class="calls-filter-btn" data-callfilter="ok">Успешные</button>
      <button class="calls-filter-btn" data-callfilter="failed">Неудачные</button>
      <div class="search-box"><span>🔍</span><input type="text" id="callsSearchBox" placeholder="Поиск по номеру телефона..."></div>
    </div>
    <div class="table-card">
      <table id="callsTbl">
        <thead>
          <tr>
            <th>Время</th>
            <th>Сотрудник</th>
            <th>Направление</th>
            <th>Длительность</th>
            <th>Телефон</th>
            <th>Статус</th>
            <th>Сделка</th>
          </tr>
        </thead>
        <tbody id="callsBody"></tbody>
      </table>
    </div>
  </div>

<script>
const rangeFrom = document.getElementById('rangeFrom');
const rangeTo = document.getElementById('rangeTo');
const today = new Date();
const todayStr = today.toISOString().slice(0,10);

function fmtDate(d) { return d.toISOString().slice(0,10); }

function applyPreset(preset) {
  const t = new Date();
  let from, to;
  if (preset === 'today') { from = new Date(t); to = new Date(t); }
  else if (preset === 'yesterday') { from = new Date(t); from.setDate(from.getDate()-1); to = new Date(from); }
  else if (preset === 'yesterday_today') { to = new Date(t); from = new Date(t); from.setDate(from.getDate()-1); }
  else if (preset === 'week') { to = new Date(t); from = new Date(t); from.setDate(from.getDate()-6); }
  else if (preset === 'month') { to = new Date(t); from = new Date(t); from.setDate(from.getDate()-29); }
  else return;
  rangeFrom.value = fmtDate(from);
  rangeTo.value = fmtDate(to);
  document.querySelectorAll('.preset-btn').forEach(b => b.classList.toggle('active', b.dataset.preset === preset));
  saveRange();
  loadData();
}

function saveRange() {
  localStorage.setItem('range_from', rangeFrom.value);
  localStorage.setItem('range_to', rangeTo.value);
}

const savedFrom = localStorage.getItem('range_from');
const savedTo = localStorage.getItem('range_to');
if (savedFrom && savedTo) {
  rangeFrom.value = savedFrom;
  rangeTo.value = savedTo;
} else {
  rangeFrom.value = todayStr;
  rangeTo.value = todayStr;
  document.querySelector('[data-preset="today"]').classList.add('active');
}

document.querySelectorAll('.preset-btn').forEach(btn => {
  btn.addEventListener('click', () => applyPreset(btn.dataset.preset));
});
rangeFrom.addEventListener('change', () => { clearPresetActive(); saveRange(); loadData(); });
rangeTo.addEventListener('change', () => { clearPresetActive(); saveRange(); loadData(); });
function clearPresetActive() { document.querySelectorAll('.preset-btn').forEach(b => b.classList.remove('active')); }

// --- вкладки ---
document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('tab-' + btn.dataset.tab).classList.add('active');
    if (window.__lastData) renderCurrentTab();
  });
});

let sortState = {key: 'active_deals', dir: 1};
document.querySelectorAll('#summaryTbl th[data-sort]').forEach(th => {
  th.addEventListener('click', () => {
    const key = th.dataset.sort;
    sortState.dir = (sortState.key === key) ? -sortState.dir : 1;
    sortState.key = key;
    renderSummary(window.__lastData);
  });
});

let callFilter = 'all';
document.querySelectorAll('[data-callfilter]').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('[data-callfilter]').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    callFilter = btn.dataset.callfilter;
    renderCalls(window.__lastData);
  });
});
document.getElementById('callsEmployeeFilter').addEventListener('change', () => renderCalls(window.__lastData));
document.getElementById('callsSearchBox').addEventListener('input', () => renderCalls(window.__lastData));
document.getElementById('searchBox').addEventListener('input', () => renderSummary(window.__lastData));

async function loadData() {
  try {
    const resp = await fetch(`/api/data?from=${rangeFrom.value}&to=${rangeTo.value}`);
    const data = await resp.json();
    window.__lastData = data;
    renderCommon(data);
    renderCurrentTab();
  } catch (e) {
    document.getElementById('status').innerText = 'Ошибка загрузки: ' + e;
  }
}

function renderCurrentTab() {
  const activeTab = document.querySelector('.tab-btn.active').dataset.tab;
  if (activeTab === 'summary') renderSummary(window.__lastData);
  else renderCalls(window.__lastData);
}

function formatTime(iso) {
  if (!iso) return {text: '', stale: false};
  const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
  if (!m) return {text: iso, stale: false};
  const [, y, mo, d, h, mi] = m;
  const dateStr = `${y}-${mo}-${d}`;
  if (dateStr === todayStr) return {text: `${h}:${mi}`, stale: false};
  return {text: `${d}.${mo} ${h}:${mi}`, stale: true};
}

function renderCommon(data) {
  document.getElementById('subtitle').innerText = `воронка «${data.category_name}» · период: ${data.range_label}`;
  document.getElementById('status').innerHTML =
    `<span class="live-dot"></span>сделки: ${data.last_slow_update || '—'} · период обновлён: ${data.range_computed_at || '—'}`;

  const banners = document.getElementById('banners');
  banners.innerHTML = '';
  if (data.still_loading) banners.innerHTML += `<div class="loading-banner">⏳ Первая загрузка данных ещё идёт (может занять минуту-две).</div>`;
  if (data.range_loading) banners.innerHTML += `<div class="loading-banner">⏳ Считаю звонки и закрытые сделки за этот период — обновится само.</div>`;
  if (data.call_error) banners.innerHTML += `<div class="warning-banner">⚠️ Ошибка при получении звонков: ${data.call_error}</div>`;
  if (data.closed_error) banners.innerHTML += `<div class="warning-banner">⚠️ Ошибка при получении закрытых сделок: ${data.closed_error}</div>`;

  const rows = data.rows || [];
  const totalDeals = rows.reduce((s,r) => s + r.active_deals, 0);
  const totalCalls = rows.reduce((s,r) => s + r.calls_in + r.calls_out, 0);
  const totalWon = rows.reduce((s,r) => s + r.closed_won, 0);
  const totalLost = rows.reduce((s,r) => s + r.closed_lost, 0);
  document.getElementById('statsRow').innerHTML = `
    <div class="stat-card accent"><div class="num">${rows.length}</div><div class="label">Сотрудников в работе</div></div>
    <div class="stat-card accent"><div class="num">${totalDeals}</div><div class="label">Сделок в работе всего</div></div>
    <div class="stat-card"><div class="num">${totalCalls}</div><div class="label">Звонков за период</div></div>
    <div class="stat-card green"><div class="num">${totalWon}</div><div class="label">Закрыто успешно</div></div>
    <div class="stat-card red"><div class="num">${totalLost}</div><div class="label">Закрыто провал</div></div>
  `;

  const empSelect = document.getElementById('callsEmployeeFilter');
  const currentVal = empSelect.value;
  empSelect.innerHTML = '<option value="">Все сотрудники</option>' +
    rows.filter(r => r.calls_in + r.calls_out > 0).map(r => `<option value="${r.name}">${r.name}</option>`).join('');
  empSelect.value = currentVal;
}

function convClass(pct) {
  if (pct >= 60) return 'conv-high';
  if (pct >= 35) return 'conv-mid';
  return 'conv-low';
}

function renderSummary(data) {
  if (!data) return;
  const query = document.getElementById('searchBox').value.trim().toLowerCase();
  let rows = (data.rows || []).filter(r => !query || r.name.toLowerCase().includes(query));

  rows = rows.map(r => ({...r, calls_total: r.calls_in + r.calls_out,
    conversion: (r.closed_won + r.closed_lost) ? Math.round(100 * r.closed_won / (r.closed_won + r.closed_lost)) : 0}));

  rows.sort((a,b) => {
    const k = sortState.key;
    if (k === 'name') return sortState.dir * a.name.localeCompare(b.name);
    return sortState.dir * ((a[k]||0) - (b[k]||0));
  });

  const lowLoadIds = new Set([...data.rows].sort((a,b)=>a.active_deals-b.active_deals).slice(0,5).map(r=>r.name));

  const tbody = document.getElementById('summaryBody');
  tbody.innerHTML = '';
  rows.forEach(r => {
    const tr = document.createElement('tr');
    if (lowLoadIds.has(r.name)) tr.className = 'low-load';
    const workStart = formatTime(r.work_start);
    const breakTime = formatTime(r.break_time);
    const fmt = t => t.text ? (t.stale ? `<span title="Не сегодня">${t.text}</span>` : t.text) : '<span class="empty-cell">—</span>';
    const nameHtml = r.crm_link ? `<a class="name-link" href="${r.crm_link}" target="_blank">${r.name} ↗</a>` : r.name;
    const convCls = convClass(r.conversion);

    tr.innerHTML = `
      <td class="name-cell" data-uid="${r.uid}">${nameHtml} <span style="font-size:11px;color:#94a3b8;">(клик — стадии)</span></td>
      <td>${r.active_deals}</td>
      <td><span class="in-call">${r.calls_in}</span> / <span class="out-call">${r.calls_out}</span></td>
      <td class="won">${r.closed_won}</td>
      <td class="lost">${r.closed_lost}</td>
      <td class="conv-cell ${convCls}">${r.conversion}%</td>
      <td>${fmt(workStart)}</td>
      <td>${fmt(breakTime)}</td>
      <td>${lowLoadIds.has(r.name) ? '<span class="badge rec">Можно догрузить</span>' : ''}</td>
    `;
    tr.querySelector('.name-cell').addEventListener('click', () => toggleStageBreakdown(tr, r.uid, r.name));
    tbody.appendChild(tr);
  });
}

async function toggleStageBreakdown(tr, uid, name) {
  const next = tr.nextElementSibling;
  if (next && next.classList.contains('expand-row')) { next.remove(); return; }
  document.querySelectorAll('.expand-row').forEach(e => e.remove());

  const expandTr = document.createElement('tr');
  expandTr.className = 'expand-row';
  const td = document.createElement('td');
  td.colSpan = 9;
  td.innerHTML = 'Загружаю разбивку по стадиям...';
  expandTr.appendChild(td);
  tr.after(expandTr);

  try {
    const resp = await fetch(`/api/employee_stages?uid=${uid}`);
    const data = await resp.json();
    if (data.error) { td.innerHTML = `Ошибка: ${data.error}`; return; }
    if (!data.stages.length) { td.innerHTML = 'Нет активных сделок.'; return; }
    td.innerHTML = `<b>${name}</b> — сделки по стадиям: ` +
      data.stages.map(s => `<span class="stage-chip">${s.stage_name}: ${s.count}</span>`).join('');
  } catch (e) {
    td.innerHTML = 'Ошибка загрузки: ' + e;
  }
}

function renderCalls(data) {
  if (!data) return;
  const empFilter = document.getElementById('callsEmployeeFilter').value;
  const phoneQuery = document.getElementById('callsSearchBox').value.trim();

  let allCalls = [];
  (data.rows || []).forEach(r => {
    if (empFilter && r.name !== empFilter) return;
    (r.calls_list || []).forEach(c => allCalls.push({...c, employee: r.name}));
  });

  if (callFilter === 'ok') allCalls = allCalls.filter(c => !c.failed);
  if (callFilter === 'failed') allCalls = allCalls.filter(c => c.failed);
  if (phoneQuery) allCalls = allCalls.filter(c => (c.phone||'').includes(phoneQuery));

  allCalls.sort((a,b) => (b.time||'').localeCompare(a.time||''));

  const tbody = document.getElementById('callsBody');
  tbody.innerHTML = '';
  if (!allCalls.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#94a3b8;padding:24px;">Нет звонков по этим фильтрам за выбранный период</td></tr>';
    return;
  }
  allCalls.slice(0, 500).forEach(c => {
    const tr = document.createElement('tr');
    if (c.failed) tr.className = 'call-row-failed';
    const time = (c.time||'').replace('T',' ').slice(0,16);
    const dirLabel = c.direction === 'in' ? '↓ Вход' : '↑ Исход';
    const durMin = Math.floor(c.duration/60), durSec = c.duration%60;
    const durText = `${durMin}:${String(durSec).padStart(2,'0')}`;
    const statusText = c.failed ? '⚠️ Неудачный' : '✅ Успешный';
    const link = c.deal_link ? `<a href="${c.deal_link}" target="_blank">Открыть ↗</a>` : '—';
    tr.innerHTML = `
      <td>${time}</td>
      <td>${c.employee}</td>
      <td class="call-dir-badge">${dirLabel}</td>
      <td>${durText}</td>
      <td>${c.phone || '—'}</td>
      <td>${statusText}</td>
      <td>${link}</td>
    `;
    tbody.appendChild(tr);
  });
  if (allCalls.length > 500) {
    const tr = document.createElement('tr');
    tr.innerHTML = `<td colspan="7" style="text-align:center;color:#94a3b8;">Показаны первые 500 из ${allCalls.length} — сузьте фильтр или период</td>`;
    tbody.appendChild(tr);
  }
}

function exportCSV() {
  const data = window.__lastData;
  if (!data) return;
  const rows = data.rows || [];
  const headers = ['Сотрудник','Сделок в работе','Звонков вход','Звонков исход','Закрыто успех','Закрыто провал','Конверсия %'];
  const lines = [headers.join(';')];
  rows.forEach(r => {
    const conv = (r.closed_won + r.closed_lost) ? Math.round(100*r.closed_won/(r.closed_won+r.closed_lost)) : 0;
    lines.push([r.name, r.active_deals, r.calls_in, r.calls_out, r.closed_won, r.closed_lost, conv].join(';'));
  });
  const blob = new Blob(["\uFEFF" + lines.join('\n')], {type: 'text/csv;charset=utf-8;'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `активность_${rangeFrom.value}_${rangeTo.value}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}
document.getElementById('exportBtn').addEventListener('click', exportCSV);

loadData();
setInterval(loadData, 25000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))
        elif parsed.path == "/api/data":
            qs = urllib.parse.parse_qs(parsed.query)
            today_str = datetime.now().strftime("%Y-%m-%d")
            date_from = qs.get("from", [today_str])[0]
            date_to = qs.get("to", [today_str])[0]
            payload = self.build_payload(date_from, date_to)
            self._send_json(payload)
        elif parsed.path == "/api/employee_stages":
            qs = urllib.parse.parse_qs(parsed.query)
            uid = qs.get("uid", [None])[0]
            with STATE_LOCK:
                category_id = STATE["category_id"]
                stage_names = dict(STATE["stage_names"])
            if not uid or not category_id:
                self._send_json({"stages": [], "error": "нет данных"})
                return
            stages, error = fetch_employee_stage_breakdown(uid, category_id, stage_names)
            self._send_json({"stages": stages, "error": error})
        else:
            self.send_response(404)
            self.end_headers()

    def _send_json(self, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def build_payload(self, date_from, date_to):
        with STATE_LOCK:
            users_by_id = dict(STATE["users_by_id"])
            active_deals = dict(STATE["active_deals"])
            attendance = dict(STATE["attendance"])
            category_name = STATE["category_name"]
            last_slow = STATE["last_slow_update"]

        range_entry, range_loading = get_range_data(date_from, date_to)
        range_entry = range_entry or {}
        calls_data = range_entry.get("calls", {})
        closed_data = range_entry.get("closed", {})
        incoming = calls_data.get("incoming", {})
        outgoing = calls_data.get("outgoing", {})
        calls_by_user = calls_data.get("calls_by_user", {})
        won = closed_data.get("won", {})
        lost = closed_data.get("lost", {})

        crm_base = f"https://{PORTAL_DOMAIN}/crm/deal/list/?apply_filter=Y&FILTER%5BASSIGNED_BY_ID%5D%5B0%5D=" if PORTAL_DOMAIN else None

        rows = []
        for uid, name in users_by_id.items():
            ad = active_deals.get(uid, 0)
            ci = incoming.get(uid, 0)
            co = outgoing.get(uid, 0)
            cw = won.get(uid, 0)
            cl = lost.get(uid, 0)
            att = attendance.get(uid, {})
            if ad or ci or co or cw or cl:
                rows.append({
                    "uid": uid,
                    "name": name,
                    "active_deals": ad,
                    "calls_in": ci,
                    "calls_out": co,
                    "closed_won": cw,
                    "closed_lost": cl,
                    "work_start": att.get("start", ""),
                    "break_time": att.get("break", ""),
                    "crm_link": (crm_base + str(uid)) if crm_base else None,
                    "calls_list": calls_by_user.get(uid, []),
                })

        rows.sort(key=lambda r: r["active_deals"])

        def fmt_date(s):
            try:
                return datetime.strptime(s, "%Y-%m-%d").strftime("%d.%m.%Y")
            except ValueError:
                return s
        range_label = f"{fmt_date(date_from)} – {fmt_date(date_to)}" if date_from != date_to else fmt_date(date_from)

        return {
            "category_name": category_name,
            "last_slow_update": last_slow,
            "range_computed_at": range_entry.get("computed_at"),
            "range_label": range_label,
            "still_loading": last_slow is None,
            "range_loading": range_loading,
            "call_error": range_entry.get("call_error"),
            "closed_error": range_entry.get("closed_error"),
            "rows": rows,
        }


def main():
    global WEBHOOK_URL, PORTAL_DOMAIN
    parser = argparse.ArgumentParser(description="Дашборд активности сотрудников")
    parser.add_argument("--webhook", default=None)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()

    WEBHOOK_URL = args.webhook or os.environ.get("BITRIX_WEBHOOK_URL")
    if not WEBHOOK_URL:
        print("ОШИБКА: не задан вебхук. Передайте --webhook или переменную окружения BITRIX_WEBHOOK_URL.")
        sys.exit(1)

    m = re.match(r"https?://([^/]+)", WEBHOOK_URL)
    PORTAL_DOMAIN = m.group(1) if m else None

    port = args.port or int(os.environ.get("PORT", 8000))

    print("Запускаю фоновое обновление данных...")
    threading.Thread(target=slow_refresh_loop, daemon=True).start()
    threading.Thread(target=range_refresh_loop, daemon=True).start()

    print(f"Сервер слушает на порту {port}")
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nОстановлено.")


if __name__ == "__main__":
    main()
