#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Живой дашборд активности сотрудников. Локальный веб-сервер: открываете
страницу в браузере один раз и оставляете открытой — данные обновляются
сами в фоне, без перезапуска скрипта.

КАК ЗАПУСТИТЬ:
  python live_dashboard.py --webhook "https://ваш-портал.bitrix24.kz/rest/USER/CODE/"

Затем откройте в браузере: http://localhost:8000

ЧТО ПОКАЗЫВАЕТ:
  - Сделок в работе сейчас (только воронка "Досудебный отдел")
  - Звонков сегодня: входящие / исходящие отдельно
  - Закрыто сделок: успех / провал отдельно
  - Начало работы / уход на перерыв (модуль учёта рабочего времени, только
    для сегодняшней даты — за прошлые дни Bitrix24 не всегда отдаёт эти
    данные через API)
  - Можно выбрать другую дату вверху страницы (для звонков и закрытых
    сделок; "сделок в работе" и время прихода — это всегда "сейчас",
    задним числом не пересчитываются)

ЕСЛИ ЧТО-ТО НЕ РАБОТАЕТ:
  Если звонки/время прихода не показываются — скорее всего, у вебхука не
  хватает прав "Телефония (telephony)" и/или "Учёт рабочего времени
  (timeman)". Добавьте их так же, как добавляли "Структура компании" —
  Разработчикам → Другое → ваш вебхук → отметить галочки → Сохранить.
  Скрипт покажет в консоли, какой именно запрос не прошёл.
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

REQUEST_DELAY = 0.25
MAX_RETRIES = 3
CATEGORY_NAME = "Досудебный отдел"
MONTHLY_PLAN = 660  # план закрытых сделок на месяц на каждого сотрудника
PORTAL_DOMAIN = None  # определяется из webhook при запуске, для ссылок в CRM
MATCH_THRESHOLD = 0.55

SLOW_REFRESH_SECONDS = 300   # активные сделки + время прихода — раз в 5 минут
FAST_REFRESH_SECONDS = 45    # звонки + закрытые сделки за выбранную дату — чаще
PLAN_REFRESH_SECONDS = 180   # обновление плана за выбранный период — раз в 3 минуты
LOW_LOAD_TOP_N = 5
ACTIVE_DEALS_WORKERS = 10    # сколько сотрудников опрашивать параллельно (ускоряет медленный шаг)

WEBHOOK_URL = None  # заполняется при запуске
VOXIMPLANT_AVAILABLE = None  # None = ещё не проверяли, True/False = уже знаем
TIMEMAN_AVAILABLE = None


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
                    if err_code == "QUERY_LIMIT_EXCEEDED":
                        time.sleep(2)
                        continue
                    if err_code in ("INSUFFICIENT_SCOPE", "ERROR_METHOD_NOT_FOUND", "NOT_FOUND") or "insufficient_scope" in str(err_desc).lower():
                        return None, None, err_desc  # постоянная ошибка — повторять бессмысленно
                    return None, None, err_desc
                return result.get("result"), result.get("total"), None
        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="ignore")
            last_error = f"HTTP {e.code}: {body_text}"
            if "insufficient_scope" in body_text.lower() or "method_not_found" in body_text.lower() or e.code == 401 or e.code == 404:
                return None, None, last_error  # постоянная ошибка — повторять бессмысленно
            time.sleep(1.0 * attempt)
        except Exception as e:
            last_error = str(e)
            time.sleep(1.0 * attempt)
    return None, None, last_error


def get_all_pages(method, params, max_pages=200):
    """max_pages — защита от зависания: если фильтр случайно не сработал
    и метод пытается отдать огромную историю целиком, остановимся сами
    вместо того чтобы висеть бесконечно (200 страниц = до 10 000 записей)."""
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
            print(f"[!] {method}: достигнут лимит {max_pages} страниц ({len(all_items)} записей) — останавливаюсь, чтобы не зависнуть. Возможно, фильтр не сработал как ожидалось.")
            break
        start += 50
    return all_items, None


def full_name(user):
    parts = [user.get("NAME", ""), user.get("LAST_NAME", "")]
    return " ".join(p for p in parts if p).strip()


def fetch_all_users():
    users, error = get_all_pages("user.get", {"ACTIVE": "Y", "ADMIN_MODE": "Y"})
    if error:
        users, error = get_all_pages("user.get", {"ACTIVE": "Y"})
    if error:
        print(f"[ОШИБКА] user.get: {error}")
        return []
    return users or []


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


def get_active_deal_count(user_id, category_id):
    result, total, error = call_bitrix(
        "crm.deal.list",
        {"filter[ASSIGNED_BY_ID]": user_id, "filter[CLOSED]": "N", "filter[CATEGORY_ID]": category_id, "select[]": "ID"},
    )
    if error:
        return 0, error
    return (total if total is not None else len(result or [])), None


def fetch_monthly_plan_stats(category_id, date_from_str, date_to_str):
    """Закрытые сделки за ВЫБРАННЫЙ ПЕРИОД (план 660 на человека), с разбивкой
    успех/провал и обнаружением дублей — когда у одного клиента (CONTACT_ID)
    несколько закрытых сделок, это может означать задвоенный учёт.
    date_from_str / date_to_str — строки 'YYYY-MM-DD'."""
    period_start = f"{date_from_str}T00:00:00"
    period_end = f"{date_to_str}T00:00:00"

    items, error = get_all_pages(
        "crm.deal.list",
        {
            "filter[CLOSED]": "Y",
            "filter[>=CLOSEDATE]": period_start,
            "filter[<CLOSEDATE]": period_end,
            "filter[CATEGORY_ID]": category_id,
            "select[]": ["ASSIGNED_BY_ID", "STAGE_SEMANTIC_ID", "CONTACT_ID"],
        },
        max_pages=400,
    )
    if error:
        return {}, error

    per_user = {}  # uid -> {"won": n, "lost": n, "contacts": {contact_id: count}}
    for item in items or []:
        uid = item.get("ASSIGNED_BY_ID")
        if not uid:
            continue
        bucket = per_user.setdefault(uid, {"won": 0, "lost": 0, "contacts": {}})
        if item.get("STAGE_SEMANTIC_ID") == "S":
            bucket["won"] += 1
        else:
            bucket["lost"] += 1
        contact_id = item.get("CONTACT_ID")
        if contact_id:
            bucket["contacts"][contact_id] = bucket["contacts"].get(contact_id, 0) + 1

    result = {}
    for uid, b in per_user.items():
        dup_count = sum(1 for c, n in b["contacts"].items() if n > 1)
        result[uid] = {
            "won": b["won"],
            "lost": b["lost"],
            "total": b["won"] + b["lost"],
            "duplicate_clients": dup_count,
        }
    return result, None


def fetch_calls_split(day_start_dt, day_end_dt):
    """Звонки за период: входящие/исходящие, по ответственному, плюс список
    отдельных звонков с деталями (для показа по клику) и пометкой неудачных.

    ВАЖНО про формат даты: voximplant.statistic.get на некоторых порталах
    (с региональными настройками ДД.ММ.ГГГГ) не понимает ISO-формат дат
    (2026-09-02T00:00:00) и тогда просто игнорирует фильтр целиком, отдавая
    всю историю звонков. Поэтому здесь используем формат ДД.ММ.ГГГГ ЧЧ:ММ:СС,
    которого Bitrix24 в большинстве случаев ждёт для этого метода.
    """
    date_fmt = "%d.%m.%Y %H:%M:%S"
    day_start = day_start_dt.strftime(date_fmt)
    day_end = day_end_dt.strftime(date_fmt)

    items, error = get_all_pages(
        "voximplant.statistic.get",
        {
            "FILTER[CALL_START_DATE_from]": day_start,
            "FILTER[CALL_START_DATE_to]": day_end,
            "SORT": "CALL_START_DATE",
            "ORDER": "DESC",
        },
        max_pages=60,  # день реально не может содержать >3000 звонков; если упёрлись — фильтр снова не сработал
    )

    if error:
        print(f"[!] voximplant.statistic.get недоступен ({error}) — считаю звонки без разделения через CRM-активности.")
        items2, error2 = get_all_pages(
            "crm.activity.list",
            {
                "filter[TYPE_ID]": 2,
                "filter[>=CREATED]": day_start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                "filter[<CREATED]": day_end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                "select[]": ["RESPONSIBLE_ID"],
            },
        )
        if error2:
            return {}, {}, {}, error2
        outgoing = {}
        for item in items2 or []:
            uid = item.get("RESPONSIBLE_ID")
            if uid:
                outgoing[uid] = outgoing.get(uid, 0) + 1
        return {}, outgoing, {}, "no_split"

    incoming = {}
    outgoing = {}
    calls_by_user = {}  # uid -> list of call dicts (для показа деталей по клику)

    out_of_range = 0
    for item in items or []:
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

    if items and len(items) > 20:
        # быстрая проверка: если много звонков вне запрошенного дня — фильтр не сработал
        sample_date = str(items[0].get("CALL_START_DATE", ""))
        if sample_date and day_start_dt.strftime("%Y-%m-%d") not in sample_date and day_start_dt.strftime("%d.%m.%Y") not in sample_date:
            print(f"[!] ПОДОЗРЕНИЕ: фильтр по дате звонков не сработал — первая запись датирована '{sample_date}', а ожидался {day_start_dt.date()}")

    return incoming, outgoing, calls_by_user, None


def fetch_closed_split(day_start, day_end):
    """Закрытые сделки за период, отдельно успех/провал, по ответственному."""
    items, error = get_all_pages(
        "crm.deal.list",
        {
            "filter[CLOSED]": "Y",
            "filter[>=CLOSEDATE]": day_start,
            "filter[<CLOSEDATE]": day_end,
            "select[]": ["ASSIGNED_BY_ID", "STAGE_SEMANTIC_ID"],
        },
    )
    if error:
        return {}, {}, error
    won = {}
    lost = {}
    for item in items or []:
        uid = item.get("ASSIGNED_BY_ID")
        if not uid:
            continue
        if item.get("STAGE_SEMANTIC_ID") == "S":
            won[uid] = won.get(uid, 0) + 1
        else:
            lost[uid] = lost.get(uid, 0) + 1
    return won, lost, None


def fetch_attendance_today(user_ids):
    """Время начала работы / ухода на перерыв — только на сегодня."""
    global TIMEMAN_AVAILABLE
    if TIMEMAN_AVAILABLE is False:
        return {}

    result = {}
    for i, uid in enumerate(user_ids):
        status, _, error = call_bitrix("timeman.status", {"USER_ID": uid})
        if error:
            if TIMEMAN_AVAILABLE is None:
                print(f"[!] timeman.status недоступен ({error}). Пропускаю время прихода/перерыва для всех — добавьте право 'Учёт рабочего времени (timeman)' в вебхук, если это нужно.")
            TIMEMAN_AVAILABLE = False
            return result  # прекращаем сразу, не тратим время на остальных
        TIMEMAN_AVAILABLE = True
        if isinstance(status, dict):
            result[uid] = {
                "start": status.get("TIME_START", "") or "",
                "break": status.get("START_ENTRY", "") or status.get("BREAK_START", "") or "",
            }
        time.sleep(REQUEST_DELAY)
    return result


# ---------------------------------------------------------------------------
# Общее состояние (кэш), обновляемое фоновыми потоками
# ---------------------------------------------------------------------------
STATE_LOCK = threading.Lock()
STATE = {
    "users_by_id": {},          # {uid: name}
    "category_name": "",
    "active_deals": {},         # {uid: count} — обновляется медленно
    "attendance": {},           # {uid: {"start":..,"break":..}} — только сегодня
    "category_id": None,
    "last_slow_update": None,
    "last_fast_update": None,
    "fast_cache": {},           # {date_str: {"in":{}, "out":{}, "won":{}, "lost":{}, "no_split":bool}}
    "fast_in_progress": set(),  # даты, которые прямо сейчас считаются в фоне (чтобы не запускать повторно)
    "plan_cache": {},           # {"from_to": {"data":..., "computed_at":...}}
    "plan_in_progress": set(),
    "errors": [],
}
FAST_IN_PROGRESS_LOCK = threading.Lock()
PLAN_IN_PROGRESS_LOCK = threading.Lock()


def slow_refresh_loop():
    category_id, category_name = find_deal_category_id()
    with STATE_LOCK:
        STATE["category_name"] = category_name or CATEGORY_NAME
        STATE["category_id"] = category_id

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


def fast_refresh_for_date(date_str):
    day_start_dt = datetime.strptime(date_str, "%Y-%m-%d")
    day_end_dt = day_start_dt + timedelta(days=1)
    day_start = day_start_dt.strftime("%Y-%m-%dT00:00:00")
    day_end = day_end_dt.strftime("%Y-%m-%dT00:00:00")

    incoming, outgoing, calls_by_user, call_err = fetch_calls_split(day_start_dt, day_end_dt)
    won, lost, closed_err = fetch_closed_split(day_start, day_end)

    bad_calls = {}
    for uid, calls in calls_by_user.items():
        bad_calls[uid] = sum(1 for c in calls if c["failed"])

    entry = {
        "in": incoming,
        "out": outgoing,
        "won": won,
        "lost": lost,
        "calls_detail": calls_by_user,
        "bad_calls": bad_calls,
        "no_split": call_err == "no_split",
        "call_error": call_err if call_err != "no_split" else None,
        "closed_error": closed_err,
    }
    with STATE_LOCK:
        STATE["fast_cache"][date_str] = entry
        STATE["last_fast_update"] = datetime.now().strftime("%H:%M:%S")
    total_bad = sum(bad_calls.values())
    print(f"[fast refresh] {date_str} — звонков вход:{sum(incoming.values())} исход:{sum(outgoing.values())} (плохих:{total_bad}) успех:{sum(won.values())} провал:{sum(lost.values())}")


def fast_refresh_loop():
    while True:
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            fast_refresh_for_date(today)
        except Exception as e:
            print(f"[fast refresh] ОШИБКА: {e}")
        time.sleep(FAST_REFRESH_SECONDS)


def refresh_plan_for_range(date_from, date_to):
    with STATE_LOCK:
        category_id = STATE["category_id"]
    if not category_id:
        return
    data, error = fetch_monthly_plan_stats(category_id, date_from, date_to)
    key = f"{date_from}_{date_to}"
    with STATE_LOCK:
        STATE["plan_cache"][key] = {
            "data": data if not error else {},
            "error": error,
            "computed_at": datetime.now().strftime("%H:%M:%S"),
        }
    if error:
        print(f"[plan] ОШИБКА расчёта плана {date_from}..{date_to}: {error}")
    else:
        total = sum(v["total"] for v in data.values())
        print(f"[plan] {date_from}..{date_to} — сотрудников с закрытыми: {len(data)}, всего закрыто: {total}")


def get_plan_data(date_from, date_to):
    """Отдаёт кэш плана за период, запускает фоновый пересчёт если нужно
    (кэш старше 5 минут или отсутствует). Не блокирует страницу."""
    key = f"{date_from}_{date_to}"
    with STATE_LOCK:
        entry = STATE["plan_cache"].get(key)

    if entry is None:
        with PLAN_IN_PROGRESS_LOCK:
            already_running = key in STATE["plan_in_progress"]
            if not already_running:
                STATE["plan_in_progress"].add(key)
        if not already_running:
            def _bg(f=date_from, t=date_to, k=key):
                try:
                    refresh_plan_for_range(f, t)
                except Exception as e:
                    print(f"[plan on-demand] ОШИБКА: {e}")
                finally:
                    with PLAN_IN_PROGRESS_LOCK:
                        STATE["plan_in_progress"].discard(k)
            threading.Thread(target=_bg, daemon=True).start()
        return {}, True, None  # пусто, ещё грузится

    return entry.get("data", {}), False, entry.get("error")


def plan_refresh_loop():
    """Раз в PLAN_REFRESH_SECONDS обновляет план для последнего запрошенного
    периода (чтобы данные не протухали, пока страница открыта)."""
    while True:
        time.sleep(PLAN_REFRESH_SECONDS)
        with STATE_LOCK:
            keys = list(STATE["plan_cache"].keys())
        for key in keys:
            try:
                date_from, date_to = key.split("_", 1)
                refresh_plan_for_range(date_from, date_to)
            except Exception as e:
                print(f"[plan refresh] ОШИБКА для {key}: {e}")


# ---------------------------------------------------------------------------
# Веб-сервер
# ---------------------------------------------------------------------------
HTML_PAGE = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Активность сотрудников</title>
<style>
  :root {
    --accent: #4f46e5;
    --accent-light: #eef2ff;
    --green: #16a34a;
    --green-light: #ecfdf3;
    --red: #dc2626;
    --red-light: #fef2f2;
    --amber: #b45309;
    --amber-light: #fffbeb;
    --text: #1f2937;
    --muted: #6b7280;
    --border: #e5e7eb;
    --bg: #f8f9fc;
  }
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
    background: var(--bg);
    margin: 0;
    padding: 32px;
    color: var(--text);
  }
  .header { display: flex; align-items: baseline; justify-content: space-between; flex-wrap: wrap; gap: 12px; margin-bottom: 20px; }
  h1 { font-size: 24px; font-weight: 700; margin: 0; }
  .subtitle { color: var(--muted); font-size: 13px; margin-top: 4px; }

  .toolbar { display: flex; align-items: center; gap: 14px; margin-bottom: 22px; flex-wrap: wrap; }
  .date-box { display: flex; align-items: center; gap: 8px; background: white; padding: 8px 14px; border-radius: 10px; border: 1px solid var(--border); box-shadow: 0 1px 2px rgba(0,0,0,0.03); }
  .date-box label { font-size: 13px; color: var(--muted); font-weight: 600; }
  input[type=date] { border: none; font-size: 14px; font-family: inherit; color: var(--text); background: transparent; }
  .live-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); display: inline-block; margin-right: 6px; animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.35; } }
  .status { font-size: 12.5px; color: var(--muted); }
  .warning-banner { background: var(--amber-light); border: 1px solid #fcd34d; color: var(--amber); padding: 10px 16px; border-radius: 10px; font-size: 13px; margin-bottom: 18px; }
  .loading-banner { background: var(--accent-light); border: 1px solid #c7d2fe; color: var(--accent); padding: 10px 16px; border-radius: 10px; font-size: 13px; margin-bottom: 18px; }

  .stats-row { display: flex; gap: 14px; margin-bottom: 22px; flex-wrap: wrap; }
  .stat-card { background: white; border-radius: 14px; padding: 16px 20px; min-width: 150px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); border: 1px solid var(--border); }
  .stat-card .num { font-size: 26px; font-weight: 700; }
  .stat-card .label { font-size: 12.5px; color: var(--muted); margin-top: 2px; }
  .stat-card.green .num { color: var(--green); }
  .stat-card.red .num { color: var(--red); }
  .stat-card.accent .num { color: var(--accent); }

  .table-card { background: white; border-radius: 16px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.06); border: 1px solid var(--border); }
  table { border-collapse: collapse; width: 100%; }
  th, td { padding: 12px 16px; text-align: center; font-size: 13.5px; }
  th { background: var(--accent); color: white; font-weight: 600; font-size: 12.5px; text-transform: uppercase; letter-spacing: 0.03em; position: sticky; top: 0; }
  td:first-child, th:first-child { text-align: left; }
  tbody tr { border-bottom: 1px solid var(--border); transition: background 0.15s; }
  tbody tr:hover { background: #fafafa; }
  tbody tr.low-load { background: var(--green-light); }
  tbody tr.low-load:hover { background: #dcfce7; }
  .name-cell { font-weight: 600; }
  .badge { display: inline-block; padding: 3px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .badge.rec { background: var(--green); color: white; }
  .num-cell { font-variant-numeric: tabular-nums; }
  .in-call { color: var(--accent); font-weight: 600; }
  .out-call { color: #7c3aed; font-weight: 600; }
  .won { color: var(--green); font-weight: 600; }
  .lost { color: var(--red); font-weight: 600; }
  .empty-cell { color: #cbd5e1; }
  .export-btn { background: white; border: 1px solid var(--border); border-radius: 10px; padding: 8px 16px; font-size: 13px; font-weight: 600; color: var(--text); cursor: pointer; box-shadow: 0 1px 2px rgba(0,0,0,0.03); }
  .export-btn:hover { background: #f3f4f6; }
  .name-link { color: var(--text); text-decoration: none; }
  .name-link:hover { color: var(--accent); text-decoration: underline; }
  .plan-cell { min-width: 150px; }
  .plan-bar { display: flex; height: 10px; border-radius: 5px; overflow: hidden; background: #f1f5f9; margin-bottom: 4px; }
  .plan-bar .won-part { background: var(--green); }
  .plan-bar .lost-part { background: var(--red); }
  .plan-label { font-size: 11.5px; color: var(--muted); }
  .plan-label .over-limit { color: var(--red); font-weight: 700; }
  .dup-badge { font-size: 10.5px; color: var(--amber); margin-left: 4px; cursor: help; }

  .leaderboard { display: flex; gap: 14px; margin-bottom: 22px; flex-wrap: wrap; }
  .lb-card { background: white; border-radius: 14px; padding: 14px 20px; flex: 1; min-width: 200px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); border: 1px solid var(--border); display: flex; align-items: center; gap: 12px; }
  .lb-card .medal { font-size: 28px; }
  .lb-card .lb-name { font-weight: 700; font-size: 14px; }
  .lb-card .lb-pct { font-size: 13px; color: var(--green); font-weight: 700; }
  .lb-card.gold { border-color: #fbbf24; background: linear-gradient(135deg, #fffbeb, white); }
  .lb-card.silver { border-color: #cbd5e1; }
  .lb-card.bronze { border-color: #fb923c33; }

  .plan-status { font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 999px; display: inline-block; margin-bottom: 3px; }
  .plan-status.done { background: var(--green-light); color: var(--green); }
  .plan-status.pending { background: #f1f5f9; color: var(--muted); }
  .plan-status.danger { background: var(--red-light); color: var(--red); }
  .plan-period { font-size: 10px; color: var(--muted); }
  .calls-clickable { cursor: pointer; text-decoration: underline dotted; }
  .calls-clickable:hover { color: var(--accent); }
  .bad-call-num { color: var(--red); font-weight: 600; }

  .modal-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.4); display: flex; align-items: center; justify-content: center; z-index: 1000; }
  .modal-box { background: white; border-radius: 14px; width: 520px; max-width: 92vw; max-height: 80vh; overflow: hidden; display: flex; flex-direction: column; }
  .modal-header { display: flex; justify-content: space-between; align-items: center; padding: 16px 20px; border-bottom: 1px solid var(--border); }
  .modal-header h3 { margin: 0; font-size: 16px; }
  .modal-close { background: none; border: none; font-size: 18px; cursor: pointer; color: var(--muted); }
  .modal-body { padding: 12px 20px; overflow-y: auto; }
  .call-row { display: flex; align-items: center; gap: 10px; padding: 8px 0; border-bottom: 1px solid #f1f5f9; font-size: 13px; }
  .call-row .call-dir { width: 18px; text-align: center; }
  .call-row .call-time { color: var(--muted); width: 60px; }
  .call-row .call-dur { width: 50px; }
  .call-row.failed { color: var(--red); }
  .call-row a { color: var(--accent); text-decoration: none; margin-left: auto; font-size: 12px; }
</style>
</head>
<body>
  <div class="header">
    <div>
      <h1>📊 Активность сотрудников</h1>
      <div class="subtitle" id="subtitle">воронка «Досудебный отдел»</div>
    </div>
  </div>

  <div class="toolbar">
    <div class="date-box">
      <label>Дата (звонки/сделки)</label>
      <input type="date" id="datePicker">
    </div>
    <div class="date-box">
      <label>План с</label>
      <input type="date" id="planFromPicker">
    </div>
    <div class="date-box">
      <label>по</label>
      <input type="date" id="planToPicker">
    </div>
    <div class="date-box">
      <label>🔍</label>
      <input type="text" id="searchBox" placeholder="Поиск по сотруднику..." style="border:none; font-size:14px; font-family:inherit; outline:none; min-width:180px;">
    </div>
    <button id="exportBtn" class="export-btn">⬇ Экспорт CSV</button>
    <div class="status" id="status"><span class="live-dot"></span>обновление...</div>
  </div>

  <div id="banners"></div>

  <div class="leaderboard" id="leaderboard"></div>

  <div class="stats-row" id="statsRow"></div>

  <div class="table-card">
    <table id="tbl">
      <thead>
        <tr>
          <th>Сотрудник</th>
          <th>Сделок в работе</th>
          <th>Звонков вход</th>
          <th>Звонков исход</th>
          <th>Плохих звонков</th>
          <th>Закрыто успех</th>
          <th>Закрыто провал</th>
          <th id="planHeader">План (месяц)</th>
          <th>Начало работы</th>
          <th>Перерыв</th>
          <th>Рекомендация</th>
        </tr>
      </thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>

  <div id="callsModalOverlay" class="modal-overlay" style="display:none;">
    <div class="modal-box">
      <div class="modal-header">
        <h3 id="callsModalTitle">Звонки</h3>
        <button id="callsModalClose" class="modal-close">✕</button>
      </div>
      <div id="callsModalBody" class="modal-body"></div>
    </div>
  </div>

<script>
const datePicker = document.getElementById('datePicker');
const planFromPicker = document.getElementById('planFromPicker');
const planToPicker = document.getElementById('planToPicker');
const today = new Date().toISOString().slice(0,10);
datePicker.value = today;

// План-период сохраняется в localStorage — "на постоянной основе",
// не сбрасывается при обновлении страницы. По умолчанию — текущий месяц.
function defaultPlanRange() {
  const now = new Date();
  const from = new Date(now.getFullYear(), now.getMonth(), 1);
  const to = new Date(now.getFullYear(), now.getMonth() + 1, 1);
  return {from: from.toISOString().slice(0,10), to: to.toISOString().slice(0,10)};
}
const savedFrom = localStorage.getItem('plan_from');
const savedTo = localStorage.getItem('plan_to');
const defaults = defaultPlanRange();
planFromPicker.value = savedFrom || defaults.from;
planToPicker.value = savedTo || defaults.to;

function savePlanRange() {
  localStorage.setItem('plan_from', planFromPicker.value);
  localStorage.setItem('plan_to', planToPicker.value);
}

async function loadData() {
  const date = datePicker.value;
  const planFrom = planFromPicker.value;
  const planTo = planToPicker.value;
  try {
    const resp = await fetch(`/api/data?date=${date}&plan_from=${planFrom}&plan_to=${planTo}`);
    const data = await resp.json();
    render(data);
  } catch (e) {
    document.getElementById('status').innerText = 'Ошибка загрузки: ' + e;
  }
}

function formatTime(iso) {
  if (!iso) return {text: '', stale: false};
  const m = iso.match(/^(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2})/);
  if (!m) return {text: iso, stale: false};
  const [, y, mo, d, h, mi] = m;
  const todayStr = new Date().toISOString().slice(0,10);
  const dateStr = `${y}-${mo}-${d}`;
  if (dateStr === todayStr) return {text: `${h}:${mi}`, stale: false};
  return {text: `${d}.${mo} ${h}:${mi}`, stale: true};
}

function render(data) {
  window.__lastData = data; // для экспорта CSV и модалки звонков
  document.getElementById('subtitle').innerText = `воронка «${data.category_name}»`;
  document.getElementById('planHeader').innerText = `План (${data.plan_period_label})`;
  document.getElementById('status').innerHTML =
    `<span class="live-dot"></span>сделки: ${data.last_slow_update || '—'} · звонки/закрытые: ${data.last_fast_update || '—'}`;

  const banners = document.getElementById('banners');
  banners.innerHTML = '';
  if (data.still_loading) {
    banners.innerHTML += `<div class="loading-banner">⏳ Первая загрузка данных ещё идёт (может занять пару минут) — цифры по сделкам появятся, как только фон досчитает.</div>`;
  }
  if (data.calls_loading) {
    banners.innerHTML += `<div class="loading-banner">⏳ Считаю звонки и закрытые сделки за эту дату в фоне — обновится само через 10-20 секунд, страницу перезагружать не нужно.</div>`;
  }
  if (data.plan_loading) {
    banners.innerHTML += `<div class="loading-banner">⏳ Считаю план за выбранный период (${data.plan_period_label}) в фоне — обновится само, страницу перезагружать не нужно.</div>`;
  }
  if (data.plan_error) {
    banners.innerHTML += `<div class="warning-banner">⚠️ Ошибка при расчёте плана: ${data.plan_error}</div>`;
  }
  if (data.call_error) {
    banners.innerHTML += `<div class="warning-banner">⚠️ Ошибка при получении звонков: ${data.call_error}</div>`;
  }
  if (data.closed_error) {
    banners.innerHTML += `<div class="warning-banner">⚠️ Ошибка при получении закрытых сделок: ${data.closed_error}</div>`;
  }
  if (data.no_split) {
    banners.innerHTML += `<div class="warning-banner">⚠️ Не удалось разделить звонки на входящие/исходящие (не хватает права «Телефония» у вебхука) — все звонки показаны в столбце «исход».</div>`;
  }
  const overLimitPeople = data.rows.filter(r => r.plan_lost > data.monthly_plan_half);
  if (overLimitPeople.length > 0) {
    banners.innerHTML += `<div class="warning-banner">🔴 У ${overLimitPeople.length} сотрудник(ов) провалов за месяц уже больше нормы (${data.monthly_plan_half} из плана ${data.monthly_plan_target}) — конверсия ниже допустимой: ${overLimitPeople.map(r=>r.name).join(', ')}</div>`;
  }

  const medals = ['🥇','🥈','🥉'];
  const medalClass = ['gold','silver','bronze'];
  const lb = document.getElementById('leaderboard');
  if (data.leaderboard && data.leaderboard.length) {
    lb.innerHTML = data.leaderboard.map((r,i) => {
      const pct = Math.round((r.plan_total / data.monthly_plan_target) * 100);
      return `<div class="lb-card ${medalClass[i]}">
        <div class="medal">${medals[i]}</div>
        <div>
          <div class="lb-name">${r.name}</div>
          <div class="lb-pct">${pct}% плана (${r.plan_total}/${data.monthly_plan_target})</div>
        </div>
      </div>`;
    }).join('');
  } else {
    lb.innerHTML = '';
  }

  const totalDeals = data.rows.reduce((s,r) => s + r.active_deals, 0);
  const totalCalls = data.rows.reduce((s,r) => s + r.calls_in + r.calls_out, 0);
  const totalBad = data.rows.reduce((s,r) => s + (r.bad_calls||0), 0);
  const totalWon = data.rows.reduce((s,r) => s + r.closed_won, 0);
  const totalLost = data.rows.reduce((s,r) => s + r.closed_lost, 0);

  document.getElementById('statsRow').innerHTML = `
    <div class="stat-card accent"><div class="num">${data.rows.length}</div><div class="label">Сотрудников в работе</div></div>
    <div class="stat-card accent"><div class="num">${totalDeals}</div><div class="label">Сделок в работе всего</div></div>
    <div class="stat-card"><div class="num">${totalCalls}</div><div class="label">Звонков за день</div></div>
    <div class="stat-card red"><div class="num">${totalBad}</div><div class="label">Плохих звонков</div></div>
    <div class="stat-card green"><div class="num">${totalWon}</div><div class="label">Закрыто успешно</div></div>
    <div class="stat-card red"><div class="num">${totalLost}</div><div class="label">Закрыто провал</div></div>
  `;

  renderTable(data);
}

function renderTable(data) {
  const query = document.getElementById('searchBox').value.trim().toLowerCase();
  const filteredRows = query ? data.rows.filter(r => r.name.toLowerCase().includes(query)) : data.rows;

  const tbody = document.getElementById('tbody');
  tbody.innerHTML = '';
  filteredRows.forEach((r, i) => {
    const tr = document.createElement('tr');
    if (i < data.low_load_top_n) tr.className = 'low-load';
    const workStart = formatTime(r.work_start);
    const breakTime = formatTime(r.break_time);
    const fmt = t => t.text ? (t.stale ? `<span class="stale" title="Не сегодня">${t.text}</span>` : t.text) : '<span class="empty-cell">—</span>';

    const nameHtml = r.crm_link
      ? `<a class="name-link" href="${r.crm_link}" target="_blank" title="Открыть сделки в Bitrix24">${r.name} ↗</a>`
      : r.name;

    const planTotal = r.plan_total || 0;
    const wonPct = planTotal ? (r.plan_won / data.monthly_plan_target) * 100 : 0;
    const lostPct = planTotal ? (r.plan_lost / data.monthly_plan_target) * 100 : 0;
    const overLimit = r.plan_lost > data.monthly_plan_half;
    const planDone = planTotal >= data.monthly_plan_target;
    const dupBadge = r.duplicate_clients > 0
      ? `<span class="dup-badge" title="${r.duplicate_clients} клиент(ов) с несколькими закрытыми сделками — возможен задвоенный учёт">⚠ ${r.duplicate_clients} дубл.</span>`
      : '';
    const statusBadge = planDone
      ? '<span class="plan-status done">✅ План выполнен</span>'
      : overLimit
        ? '<span class="plan-status danger">⚠️ Провалов больше нормы</span>'
        : '<span class="plan-status pending">В процессе</span>';
    const planHtml = `
      ${statusBadge}
      <div class="plan-bar">
        <div class="won-part" style="width:${wonPct}%"></div>
        <div class="lost-part" style="width:${lostPct}%"></div>
      </div>
      <div class="plan-label">${planTotal}/${data.monthly_plan_target} · ${r.plan_won} усп / ${r.plan_lost} пров${dupBadge}</div>
    `;

    const totalCallsRow = (r.calls_in || 0) + (r.calls_out || 0);
    const callsInHtml = r.calls_in
      ? `<span class="calls-clickable" onclick="openCallsModal('${r.name.replace(/'/g,"\\'")}', 'in')">${r.calls_in}</span>`
      : '0';
    const callsOutHtml = r.calls_out
      ? `<span class="calls-clickable" onclick="openCallsModal('${r.name.replace(/'/g,"\\'")}', 'out')">${r.calls_out}</span>`
      : '0';
    const badHtml = r.bad_calls
      ? `<span class="bad-call-num calls-clickable" onclick="openCallsModal('${r.name.replace(/'/g,"\\'")}', 'bad')">${r.bad_calls}</span>`
      : '0';

    tr.innerHTML = `
      <td class="name-cell">${nameHtml}</td>
      <td class="num-cell">${r.active_deals}</td>
      <td class="num-cell in-call">${callsInHtml}</td>
      <td class="num-cell out-call">${callsOutHtml}</td>
      <td class="num-cell">${badHtml}</td>
      <td class="num-cell won">${r.closed_won || 0}</td>
      <td class="num-cell lost">${r.closed_lost || 0}</td>
      <td class="plan-cell">${planHtml}</td>
      <td>${fmt(workStart)}</td>
      <td>${fmt(breakTime)}</td>
      <td>${i < data.low_load_top_n ? '<span class="badge rec">Можно догрузить</span>' : ''}</td>
    `;
    tbody.appendChild(tr);
  });
}

function openCallsModal(employeeName, filterType) {
  const data = window.__lastData;
  if (!data) return;
  const row = data.rows.find(r => r.name === employeeName);
  if (!row) return;

  let calls = row.calls_list || [];
  if (filterType === 'in') calls = calls.filter(c => c.direction === 'in');
  if (filterType === 'out') calls = calls.filter(c => c.direction === 'out');
  if (filterType === 'bad') calls = calls.filter(c => c.failed);

  const titleMap = {in: 'входящие звонки', out: 'исходящие звонки', bad: 'плохие звонки'};
  document.getElementById('callsModalTitle').innerText = `${employeeName} — ${titleMap[filterType] || 'звонки'}`;

  const body = document.getElementById('callsModalBody');
  if (!calls.length) {
    body.innerHTML = '<p style="color:#94a3b8;">Нет данных по этим звонкам.</p>';
  } else {
    body.innerHTML = calls.map(c => {
      const time = (c.time || '').replace('T', ' ').slice(0, 16);
      const dirIcon = c.direction === 'in' ? '↓' : '↑';
      const durMin = Math.floor(c.duration / 60);
      const durSec = c.duration % 60;
      const durText = c.duration ? `${durMin}:${String(durSec).padStart(2,'0')}` : '0:00';
      const link = c.deal_link ? `<a href="${c.deal_link}" target="_blank">Открыть сделку ↗</a>` : '';
      return `<div class="call-row ${c.failed ? 'failed' : ''}">
        <span class="call-dir">${dirIcon}</span>
        <span class="call-time">${time}</span>
        <span class="call-dur">${durText}</span>
        <span>${c.phone || ''}</span>
        ${c.failed ? '<span title="Неудачный звонок">⚠️</span>' : ''}
        ${link}
      </div>`;
    }).join('');
  }

  document.getElementById('callsModalOverlay').style.display = 'flex';
}

document.getElementById('callsModalClose').addEventListener('click', () => {
  document.getElementById('callsModalOverlay').style.display = 'none';
});
document.getElementById('callsModalOverlay').addEventListener('click', (e) => {
  if (e.target.id === 'callsModalOverlay') e.target.style.display = 'none';
});

function exportCSV() {
  const data = window.__lastData;
  if (!data) return;
  const query = document.getElementById('searchBox').value.trim().toLowerCase();
  const rows = query ? data.rows.filter(r => r.name.toLowerCase().includes(query)) : data.rows;
  const headers = ['Сотрудник','Сделок в работе','Звонков вход','Звонков исход','Закрыто успех','Закрыто провал','План всего','План успех','План провал','Дублей клиентов','Начало работы','Перерыв'];
  const lines = [headers.join(';')];
  rows.forEach(r => {
    lines.push([
      r.name, r.active_deals, r.calls_in||0, r.calls_out||0, r.closed_won||0, r.closed_lost||0,
      r.plan_total||0, r.plan_won||0, r.plan_lost||0, r.duplicate_clients||0,
      r.work_start||'', r.break_time||''
    ].join(';'));
  });
  const blob = new Blob(["\\uFEFF" + lines.join('\\n')], {type: 'text/csv;charset=utf-8;'});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `активность_сотрудников_${datePicker.value}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

document.getElementById('searchBox').addEventListener('input', () => renderTable(window.__lastData));
document.getElementById('exportBtn').addEventListener('click', exportCSV);
datePicker.addEventListener('change', loadData);
planFromPicker.addEventListener('change', () => { savePlanRange(); loadData(); });
planToPicker.addEventListener('change', () => { savePlanRange(); loadData(); });
loadData();
setInterval(loadData, 20000); // обновление раз в 20 секунд
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # тише в консоли, у нас свои print'ы

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))
        elif parsed.path == "/api/data":
            qs = urllib.parse.parse_qs(parsed.query)
            date_str = qs.get("date", [datetime.now().strftime("%Y-%m-%d")])[0]
            now = datetime.now()
            default_plan_from = now.replace(day=1).strftime("%Y-%m-%d")
            next_month = now.month % 12 + 1
            next_month_year = now.year + (1 if now.month == 12 else 0)
            default_plan_to = f"{next_month_year}-{next_month:02d}-01"
            plan_from = qs.get("plan_from", [default_plan_from])[0]
            plan_to = qs.get("plan_to", [default_plan_to])[0]
            payload = self.build_payload(date_str, plan_from, plan_to)
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def build_payload(self, date_str, plan_from, plan_to):
        with STATE_LOCK:
            users_by_id = dict(STATE["users_by_id"])
            active_deals = dict(STATE["active_deals"])
            attendance = dict(STATE["attendance"])
            category_name = STATE["category_name"]
            last_slow = STATE["last_slow_update"]
            last_fast = STATE["last_fast_update"]
            fast_entry = STATE["fast_cache"].get(date_str)

        monthly_plan, plan_loading, plan_error = get_plan_data(plan_from, plan_to)

        # если по этой дате ещё нет данных в кэше — запускаем расчёт В ФОНЕ,
        # а сейчас сразу отвечаем тем, что есть (не блокируем страницу)
        calls_loading = False
        if fast_entry is None:
            calls_loading = True
            with FAST_IN_PROGRESS_LOCK:
                already_running = date_str in STATE["fast_in_progress"]
                if not already_running:
                    STATE["fast_in_progress"].add(date_str)

            if not already_running:
                def _bg(d=date_str):
                    try:
                        fast_refresh_for_date(d)
                    except Exception as e:
                        print(f"[on-demand refresh] ОШИБКА: {e}")
                    finally:
                        with FAST_IN_PROGRESS_LOCK:
                            STATE["fast_in_progress"].discard(d)
                threading.Thread(target=_bg, daemon=True).start()

        fast_entry = fast_entry or {}
        incoming = fast_entry.get("in", {})
        outgoing = fast_entry.get("out", {})
        won = fast_entry.get("won", {})
        lost = fast_entry.get("lost", {})
        no_split = fast_entry.get("no_split", False)
        calls_detail = fast_entry.get("calls_detail", {})
        bad_calls = fast_entry.get("bad_calls", {})

        is_today = date_str == datetime.now().strftime("%Y-%m-%d")

        crm_base = f"https://{PORTAL_DOMAIN}/crm/deal/list/?apply_filter=Y&FILTER%5BASSIGNED_BY_ID%5D%5B0%5D=" if PORTAL_DOMAIN else None

        rows = []
        for uid, name in users_by_id.items():
            ad = active_deals.get(uid, 0)
            ci = incoming.get(uid, 0)
            co = outgoing.get(uid, 0)
            cw = won.get(uid, 0)
            cl = lost.get(uid, 0)
            att = attendance.get(uid, {}) if is_today else {}
            plan = monthly_plan.get(uid, {"won": 0, "lost": 0, "total": 0, "duplicate_clients": 0})
            bad = bad_calls.get(uid, 0)
            if ad or ci or co or cw or cl or plan["total"]:
                rows.append({
                    "name": name,
                    "active_deals": ad,
                    "calls_in": ci,
                    "calls_out": co,
                    "bad_calls": bad,
                    "closed_won": cw,
                    "closed_lost": cl,
                    "work_start": att.get("start", ""),
                    "break_time": att.get("break", ""),
                    "plan_won": plan["won"],
                    "plan_lost": plan["lost"],
                    "plan_total": plan["total"],
                    "duplicate_clients": plan["duplicate_clients"],
                    "crm_link": (crm_base + str(uid)) if crm_base else None,
                    "calls_list": calls_detail.get(uid, []),
                })

        rows.sort(key=lambda r: r["active_deals"])

        # красивая подпись периода из фактически выбранных дат (dd.mm.yyyy)
        def fmt_date(s):
            try:
                return datetime.strptime(s, "%Y-%m-%d").strftime("%d.%m.%Y")
            except ValueError:
                return s
        plan_period_label = f"{fmt_date(plan_from)} – {fmt_date(plan_to)}"

        leaderboard = sorted(
            [r for r in rows if r["plan_total"] > 0],
            key=lambda r: r["plan_total"] / MONTHLY_PLAN,
            reverse=True,
        )[:3]

        return {
            "category_name": category_name,
            "last_slow_update": last_slow,
            "last_fast_update": last_fast,
            "no_split": no_split,
            "low_load_top_n": LOW_LOAD_TOP_N,
            "still_loading": last_slow is None,
            "calls_loading": calls_loading,
            "plan_loading": plan_loading,
            "plan_error": plan_error,
            "call_error": fast_entry.get("call_error"),
            "closed_error": fast_entry.get("closed_error"),
            "plan_period_label": plan_period_label,
            "plan_from": plan_from,
            "plan_to": plan_to,
            "leaderboard": leaderboard,
            "monthly_plan_target": MONTHLY_PLAN,
            "monthly_plan_half": MONTHLY_PLAN // 2,
            "rows": rows,
        }


def main():
    global WEBHOOK_URL
    parser = argparse.ArgumentParser(description="Живой дашборд активности сотрудников")
    parser.add_argument("--webhook", default=None, help="URL входящего вебхука Bitrix24 (иначе берётся из переменной окружения BITRIX_WEBHOOK_URL)")
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()

    WEBHOOK_URL = args.webhook or os.environ.get("BITRIX_WEBHOOK_URL")
    if not WEBHOOK_URL:
        print("ОШИБКА: не задан вебхук. Передайте --webhook или переменную окружения BITRIX_WEBHOOK_URL.")
        sys.exit(1)

    global PORTAL_DOMAIN
    m = re.match(r"https?://([^/]+)", WEBHOOK_URL)
    PORTAL_DOMAIN = m.group(1) if m else None

    port = args.port or int(os.environ.get("PORT", 8000))

    print("Запускаю фоновое обновление данных...")
    threading.Thread(target=slow_refresh_loop, daemon=True).start()
    threading.Thread(target=fast_refresh_loop, daemon=True).start()
    threading.Thread(target=plan_refresh_loop, daemon=True).start()

    print(f"Сервер слушает на порту {port}")
    print("Ctrl+C — остановить (при локальном запуске).")

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nОстановлено.")


if __name__ == "__main__":
    main()
