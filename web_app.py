#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FSC 電子支付資料 Web 系統
啟動：python3 web_app.py
瀏覽：http://127.0.0.1:5000
"""

import sqlite3
import json
import csv
import re
import os
import io
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, request, render_template, send_file

BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "fsc_ebank.db"
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB

TABLE_DESC = {
    "EC002W": "儲值卡發行資料",      "EC011W": "儲值卡類型",
    "EP005B": "電支帳戶戶數/人數",    "EP005W": "電支帳戶使用者別交易",
    "EP006B": "業務帳戶別交易",       "EP006W": "業務別交易",
    "EP007W": "電支帳戶支付工具別交易","EP007X": "儲值卡支付工具別交易",
    "EP008W": "特約機構交易",         "EP010W": "實體通路支付服務",
    "EP010X": "代理收付通路別交易",   "EP014W": "收受支付款項餘額",
    "EP015W": "申訴案件統計",         "EP105B": "境外業務客戶數",
    "EP105W": "境外業務客戶",         "EP106B": "境外業務客戶交易",
    "EP106W": "境外業務業務別交易",   "EP106X": "與大陸地區合作業務",
    "EP107W": "境外業務支付工具",     "EP108W": "境外業務收款方交易",
    "EP108X": "境外業務付款方交易",   "EP114W": "境外業務餘額",
    "EP115W": "境外業務申訴",         "WB031W": "電話申訴辦理",
    "WB032W": "申訴服務專線",         "WB033W": "人民陳情案件",
    "WB041W": "行動支付業務",         "WB056W": "端末設備共用",
}

SKIP_COLS = {"維護部門名稱","主管姓名","主管電話","承辦人姓名","承辦人電話","承辦人E_MAIL"}
SKIP_IMPORT = {"_url", "editable"}

# ── DB helpers ─────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def table_list():
    conn = get_conn()
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE '\_%' ESCAPE '\\' ORDER BY name"
    ).fetchall()
    conn.close()
    return [r["name"] for r in rows]

def get_columns(conn, tbl):
    return [r[1] for r in conn.execute(f'PRAGMA table_info("{tbl}")').fetchall()]

def is_numeric_col(conn, tbl, col):
    """只有欄位值實際上是數字才回傳 True（避免 SQLite 把中文 CAST 成 0.0 的誤判）"""
    try:
        rows = conn.execute(
            f'SELECT "{col}" FROM "{tbl}" '
            f'WHERE "{col}" IS NOT NULL AND "{col}" != "" LIMIT 10'
        ).fetchall()
        if not rows:
            return False
        # 嘗試用 Python 解析，全部能轉成 float 才算數值欄
        for r in rows:
            val = r[0]
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                continue
            try:
                float(str(val).replace(",", ""))
            except (ValueError, TypeError):
                return False
        return True
    except Exception:
        return False

def is_chinese_col(name: str) -> bool:
    """欄位名稱第一個字為中文字"""
    return bool(name) and '\u4e00' <= name[0] <= '\u9fff'

# 顯示用：保留中文欄位 + yr/mn 作為日期錨點（內部用）
DISPLAY_ANCHOR = {"yr", "mn"}
# 比較/趨勢：排除這些非數值中文欄
TEXT_CHINESE_COLS = {"機構名稱","資料月份","卡片名稱","交易類型","項目","申訴類別",
                     "客戶類別","核准業務別","業務項目_合作對象","機構別",
                     "儲值卡類型","付款方式","使用服務種類","服務種類"}

def safe_col_name(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_\u4e00-\u9fff]", "_", str(name))
    if s and s[0].isdigit():
        s = "c_" + s
    return s

def resolve_safe_map(col_list):
    seen = {}
    result = {}
    for col in col_list:
        s = safe_col_name(col)
        sl = s.lower()
        if sl in seen:
            while True:
                seen[sl] += 1
                candidate = f"{s}_{seen[sl]}"
                if candidate.lower() not in seen:
                    s = candidate
                    seen[s.lower()] = 0
                    break
        else:
            seen[sl] = 0
        result[col] = s
    return result

def ensure_import_log(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS "_import_log" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT, source_file TEXT,
        table_code TEXT, rows_added INTEGER, rows_skipped INTEGER
    )''')

# ── 儀表板 KPI 定義（參考 CSV 檔欄位） ───────────────────────
# col: 資料庫欄位名, label: 顯示名稱, unit: 單位, filter: 額外 WHERE 條件
DASHBOARD_KPI = [
    # 儲值卡發行 (EC002W)
    {"table": "EC002W", "group": "儲值卡發行",
     "kpis": [
         {"col": "發卡總數",   "label": "累計發卡數", "unit": "張"},
         {"col": "流通卡數",   "label": "流通卡數",   "unit": "張"},
         {"col": "當月發卡數", "label": "當月發卡數", "unit": "張"},
         {"col": "當月停卡數", "label": "當月停卡數", "unit": "張"},
     ]},
    # 電子支付帳戶戶數 (EP005B)
    {"table": "EP005B", "group": "電支帳戶",
     "kpis": [
         {"col": "總計", "label": "電支帳戶總數", "unit": "戶"},
         {"col": "可從事與境外機構合作或協助從事電子支付機構業務相關行為交易之使用者人數",
          "label": "跨境可用使用者", "unit": "人"},
     ]},
    # 業務別交易 (EP006W) — 交易類型='電子支付帳戶' 行為金額行
    {"table": "EP006W", "group": "業務別交易",
     "kpis": [
         {"col": "合計", "label": "業務交易總計", "unit": "筆",
          "filter": "\"交易類型\" = '電子支付帳戶'"},
     ]},
    # 帳戶別交易 (EP006B) — 類別='筆數'
    {"table": "EP006B", "group": "帳戶別交易",
     "kpis": [
         {"col": "總計", "label": "帳戶別交易筆數", "unit": "筆",
          "filter": "\"類別\" = '筆數'"},
     ]},
    # 代理收付通路別 (EP010X) — 筆數='金額' 行為金額行
    {"table": "EP010X", "group": "代理收付通路別",
     "kpis": [
         {"col": "代理收付實質交易款項", "label": "代理收付金額", "unit": "元",
          "filter": "\"筆數\" = '金額'"},
     ]},
    # 收受支付款項餘額 (EP014W)
    {"table": "EP014W", "group": "支付款項餘額",
     "kpis": [
         {"col": "支付款項餘額總計",   "label": "支付款項餘額總計", "unit": "元",
          "filter": "\"支付工具\" = '類型'"},
         {"col": "支付款項餘額_C_A_B_","label": "電支帳戶餘額",    "unit": "元",
          "filter": "\"支付工具\" = '類型'"},
     ]},
    # 申訴案件 (EP015W) — 申訴類別='本月新增申訴案件'
    {"table": "EP015W", "group": "申訴案件",
     "kpis": [
         {"col": "小計", "label": "本月新增申訴案件", "unit": "件",
          "filter": "\"申訴類別\" = '本月新增申訴案件'"},
     ]},
    # 行動支付業務 (WB041W) — 業務項目_合作對象='發卡數' 為總計行
    {"table": "WB041W", "group": "行動支付業務",
     "kpis": [
         {"col": "amt",   "label": "行動支付金額", "unit": "元",
          "filter": "\"業務項目_合作對象\" = '發卡數'"},
         {"col": "xact",  "label": "交易筆數",     "unit": "筆",
          "filter": "\"業務項目_合作對象\" = '發卡數'"},
         {"col": "cards", "label": "發卡數",       "unit": "張",
          "filter": "\"業務項目_合作對象\" = '發卡數'"},
     ]},
]

def _fetch_kpi_value(conn, tbl, col, yr, mn, filter_sql=""):
    """取得特定月份某欄位的加總值，回傳 float or None"""
    try:
        extra = f"AND ({filter_sql})" if filter_sql else ""
        row = conn.execute(
            f'SELECT SUM(CAST("{col}" AS REAL)) FROM "{tbl}" '
            f'WHERE yr=? AND mn=? {extra}',
            [yr, mn]
        ).fetchone()
        return round(float(row[0]), 2) if row and row[0] is not None else None
    except Exception:
        return None

# ══════════════════════════════════════════════════════════
# REST API
# ══════════════════════════════════════════════════════════

@app.route("/api/dashboard")
def api_dashboard():
    conn = get_conn()
    result = []
    for grp in DASHBOARD_KPI:
        tbl = grp["table"]
        if not _table_exists(conn, tbl):
            continue
        # 取最新兩個月
        periods = conn.execute(
            f'SELECT DISTINCT yr, mn FROM "{tbl}" ORDER BY yr DESC, mn DESC LIMIT 2'
        ).fetchall()
        if not periods:
            continue
        yr_cur, mn_cur = periods[0]
        yr_prv, mn_prv = periods[1] if len(periods) > 1 else (None, None)

        kpi_results = []
        for k in grp["kpis"]:
            col    = k["col"]
            label  = k["label"]
            unit   = k["unit"]
            flt    = k.get("filter", "")
            v_cur  = _fetch_kpi_value(conn, tbl, col, yr_cur, mn_cur, flt)
            v_prv  = _fetch_kpi_value(conn, tbl, col, yr_prv, mn_prv, flt) if yr_prv else None
            diff   = None
            pct    = None
            if v_cur is not None and v_prv is not None and v_prv != 0:
                diff = round(v_cur - v_prv, 2)
                pct  = round((v_cur - v_prv) / abs(v_prv) * 100, 2)
            kpi_results.append({
                "col": col, "label": label, "unit": unit,
                "value": v_cur, "prev": v_prv,
                "diff": diff, "pct": pct
            })

        result.append({
            "table":      tbl,
            "group":      grp["group"],
            "latest_ym":  f"{yr_cur}/{mn_cur:02d}",
            "prev_ym":    f"{yr_prv}/{mn_prv:02d}" if yr_prv else None,
            "kpis":       kpi_results
        })
    conn.close()
    return jsonify(result)


@app.route("/api/tables")
def api_tables():
    conn = get_conn()
    tables = table_list()
    result = []
    for t in tables:
        cnt = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        cols = get_columns(conn, t)
        has_yr = "yr" in cols
        yr_range = None
        latest_ym = None
        if has_yr and cnt > 0:
            r = conn.execute(f'SELECT MIN(yr), MAX(yr), MAX(mn) FROM "{t}"').fetchone()
            yr_range = {"min": r[0], "max": r[1]}
            # Latest yr/mn
            lr = conn.execute(
                f'SELECT yr, mn FROM "{t}" ORDER BY yr DESC, mn DESC LIMIT 1'
            ).fetchone()
            if lr:
                latest_ym = f"{lr[0]}/{lr[1]:02d}"
        result.append({
            "code": t, "desc": TABLE_DESC.get(t, t),
            "count": cnt, "yr_range": yr_range, "latest_ym": latest_ym
        })
    conn.close()
    return jsonify(result)


@app.route("/api/table/<code>/months")
def api_months(code):
    conn = get_conn()
    cols = get_columns(conn, code)
    if "yr" not in cols:
        conn.close()
        return jsonify([])
    rows = conn.execute(
        f'SELECT DISTINCT yr, mn FROM "{code}" ORDER BY yr DESC, mn DESC'
    ).fetchall()
    conn.close()
    return jsonify([{"yr": r[0], "mn": r[1], "label": f"{r[0]}/{r[1]:02d}"} for r in rows])


@app.route("/api/table/<code>/columns")
def api_columns(code):
    conn = get_conn()
    all_cols = get_columns(conn, code)
    result = []
    for c in all_cols:
        if c in SKIP_COLS:
            continue
        if not is_chinese_col(c):   # 只回傳中文欄位
            continue
        numeric = is_numeric_col(conn, code, c)
        result.append({"name": c, "numeric": numeric})
    conn.close()
    return jsonify(result)


@app.route("/api/table/<code>/data")
def api_data(code):
    yr    = request.args.get("yr", type=int)
    mn    = request.args.get("mn", type=int)
    yr_to = request.args.get("yr_to", type=int)
    org   = request.args.get("org", "")
    page  = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)

    conn = get_conn()
    cols = get_columns(conn, code)
    # 只顯示中文欄位（排除維護人員欄）
    display_cols = [c for c in cols
                    if (is_chinese_col(c) or c in DISPLAY_ANCHOR)
                    and c not in SKIP_COLS]

    conds, params = [], []
    if yr:      conds.append("yr >= ?");      params.append(yr)
    if yr_to:   conds.append("yr <= ?");      params.append(yr_to)
    if mn:      conds.append("mn = ?");       params.append(mn)
    if org and "機構名稱" in cols:
        conds.append('機構名稱 = ?');           params.append(org)

    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    total = conn.execute(f'SELECT COUNT(*) FROM "{code}" {where}', params).fetchone()[0]

    col_sql = ", ".join(f'"{c}"' for c in display_cols)
    rows = conn.execute(
        f'SELECT {col_sql} FROM "{code}" {where} ORDER BY yr DESC, mn DESC '
        f'LIMIT {per_page} OFFSET {(page-1)*per_page}',
        params
    ).fetchall()

    # Distinct orgs
    orgs = []
    if "機構名稱" in cols:
        orgs = [r[0] for r in conn.execute(
            f'SELECT DISTINCT 機構名稱 FROM "{code}" ORDER BY 機構名稱'
        ).fetchall()]

    conn.close()
    return jsonify({
        "columns": display_cols,
        "rows": [list(r) for r in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "orgs": orgs
    })


@app.route("/api/table/<code>/compare")
def api_compare(code):
    yr1 = request.args.get("yr1", type=int)
    mn1 = request.args.get("mn1", type=int)
    yr2 = request.args.get("yr2", type=int)
    mn2 = request.args.get("mn2", type=int)
    org = request.args.get("org", "")

    if not all([yr1, mn1, yr2, mn2]):
        return jsonify({"error": "缺少參數"}), 400

    conn = get_conn()
    cols = get_columns(conn, code)
    skip = SKIP_COLS | {"yr", "mn", "ym", "sn", "cardname", "editable"}
    # 只取中文欄位且能轉數值
    numeric_cols = [c for c in cols
                    if c not in skip
                    and is_chinese_col(c)
                    and c not in TEXT_CHINESE_COLS
                    and is_numeric_col(conn, code, c)]

    cond_extra = ""
    params_extra = []
    if org and "機構名稱" in cols:
        cond_extra = "AND 機構名稱 = ?"
        params_extra = [org]

    def fetch_period(yr, mn):
        rows = conn.execute(
            f'SELECT * FROM "{code}" WHERE yr=? AND mn=? {cond_extra}',
            [yr, mn] + params_extra
        ).fetchall()
        if not rows:
            return {}
        # Aggregate numeric cols (sum if multiple rows)
        agg = {}
        for col in numeric_cols:
            try:
                idx = cols.index(col)
                vals = [float(r[idx]) for r in rows if r[idx] is not None and str(r[idx]) != ""]
                agg[col] = sum(vals) if vals else None
            except Exception:
                agg[col] = None
        return agg

    data1 = fetch_period(yr1, mn1)
    data2 = fetch_period(yr2, mn2)

    result = []
    for col in numeric_cols:
        v1 = data1.get(col)
        v2 = data2.get(col)
        diff = None
        pct  = None
        if v1 is not None and v2 is not None:
            diff = v2 - v1
            if v1 != 0:
                pct = round((v2 - v1) / abs(v1) * 100, 2)
        result.append({
            "col": col,
            "v1": v1, "v2": v2,
            "diff": round(diff, 2) if diff is not None else None,
            "pct": pct
        })

    conn.close()
    return jsonify({
        "label1": f"{yr1}/{mn1:02d}",
        "label2": f"{yr2}/{mn2:02d}",
        "rows": result
    })


@app.route("/api/table/<code>/trend")
def api_trend(code):
    col = request.args.get("col", "")
    org = request.args.get("org", "")

    conn = get_conn()
    cols = get_columns(conn, code)
    if col not in cols or "yr" not in cols:
        conn.close()
        return jsonify({"error": "欄位不存在"}), 400

    cond_extra = ""
    params_extra = []
    if org and "機構名稱" in cols:
        cond_extra = "AND 機構名稱 = ?"
        params_extra = [org]

    rows = conn.execute(
        f'SELECT yr, mn, SUM(CAST("{col}" AS REAL)) as val '
        f'FROM "{code}" WHERE "{col}" IS NOT NULL AND "{col}" != "" {cond_extra} '
        f'GROUP BY yr, mn ORDER BY yr, mn',
        params_extra
    ).fetchall()
    conn.close()

    return jsonify([{
        "yr": r[0], "mn": r[1],
        "label": f"{r[0]}/{r[1]:02d}",
        "val": round(r[2], 2) if r[2] is not None else None
    } for r in rows])


@app.route("/api/table/<code>/export")
def api_export(code):
    yr    = request.args.get("yr", type=int)
    mn    = request.args.get("mn", type=int)
    yr_to = request.args.get("yr_to", type=int)
    org   = request.args.get("org", "")

    conn = get_conn()
    cols = get_columns(conn, code)
    display_cols = [c for c in cols if c not in SKIP_COLS]

    conds, params = [], []
    if yr:      conds.append("yr >= ?");  params.append(yr)
    if yr_to:   conds.append("yr <= ?");  params.append(yr_to)
    if mn:      conds.append("mn = ?");   params.append(mn)
    if org and "機構名稱" in cols:
        conds.append('機構名稱 = ?');       params.append(org)

    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    col_sql = ", ".join(f'"{c}"' for c in display_cols)
    rows = conn.execute(
        f'SELECT {col_sql} FROM "{code}" {where} ORDER BY yr DESC, mn DESC', params
    ).fetchall()
    conn.close()

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(display_cols)
    writer.writerows(rows)
    out.seek(0)

    fname = f"{code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return send_file(
        io.BytesIO(('\ufeff' + out.read()).encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        attachment_filename=fname
    )


@app.route("/api/import", methods=["POST"])
def api_import():
    if "file" not in request.files:
        return jsonify({"error": "請選擇檔案"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "空檔名"}), 400

    fname = f.filename
    ext = fname.rsplit(".", 1)[-1].lower()
    content = f.read().decode("utf-8-sig")

    conn = get_conn()
    conn.row_factory = None
    ensure_import_log(conn)

    results = []
    try:
        if ext == "json":
            data = json.loads(content)
            # Format 1: {itemName, details}
            if "details" in data and "itemName" in data:
                tbl = data["itemName"].split("_")[0]
                records = data["details"]
                added, skipped = _upsert(conn, tbl, records, fname)
                results.append({"table": tbl, "added": added, "skipped": skipped})
            # Format 2: {tableKey: {details}}
            elif isinstance(data, dict):
                for key, content_v in data.items():
                    if isinstance(content_v, dict) and "details" in content_v:
                        tbl = key.split("_")[0]
                        records = content_v["details"]
                        added, skipped = _upsert(conn, tbl, records, fname)
                        results.append({"table": tbl, "added": added, "skipped": skipped})
        elif ext == "csv":
            tbl_guess = re.split(r"[_\.]", fname)[0].upper()
            reader = csv.DictReader(io.StringIO(content))
            records = [dict(r) for r in reader]
            added, skipped = _upsert(conn, tbl_guess, records, fname)
            results.append({"table": tbl_guess, "added": added, "skipped": skipped})
        else:
            conn.close()
            return jsonify({"error": "僅支援 .json 或 .csv 檔案"}), 400

        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

    conn.close()
    total_added   = sum(r["added"]   for r in results)
    total_skipped = sum(r["skipped"] for r in results)
    return jsonify({
        "success": True,
        "results": results,
        "total_added": total_added,
        "total_skipped": total_skipped
    })


@app.route("/api/import_log")
def api_import_log():
    conn = get_conn()
    ensure_import_log(conn)
    rows = conn.execute(
        'SELECT imported_at, source_file, table_code, rows_added, rows_skipped '
        'FROM "_import_log" ORDER BY id DESC LIMIT 50'
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


def _upsert(conn, tbl, records, source):
    if not records:
        return 0, 0

    clean_records = [{k: v for k, v in r.items() if k not in SKIP_IMPORT}
                     for r in records]

    # Collect cols
    col_set = {}
    col_order = []
    for r in clean_records:
        for k, v in r.items():
            if k not in col_set:
                col_set[k] = ("INTEGER" if isinstance(v, int) and not isinstance(v, bool)
                               else "REAL" if isinstance(v, float) else "TEXT")
                col_order.append(k)

    safe_map = resolve_safe_map(col_order)

    # Ensure table & columns exist
    existing_cols_info = conn.execute(
        f'PRAGMA table_info("{tbl}")'
    ).fetchall() if _table_exists(conn, tbl) else []

    if not existing_cols_info:
        col_defs = ", ".join(f'"{safe_map[c]}" {col_set[c]}' for c in col_order)
        conn.execute(f'CREATE TABLE "{tbl}" ({col_defs})')
    else:
        existing = {r[1] for r in existing_cols_info}
        for orig_c, safe_c in safe_map.items():
            if safe_c not in existing:
                conn.execute(f'ALTER TABLE "{tbl}" ADD COLUMN "{safe_c}" {col_set[orig_c]}')

    # Hash existing rows
    ex_rows = conn.execute(f'SELECT * FROM "{tbl}"').fetchall()
    ex_col_names = [r[1] for r in conn.execute(f'PRAGMA table_info("{tbl}")').fetchall()]
    existing_hashes = set()
    for row in ex_rows:
        d = {ex_col_names[i]: row[i] for i in range(len(ex_col_names))}
        existing_hashes.add(json.dumps(d, sort_keys=True, ensure_ascii=False))

    insert_safe_cols = [safe_map[c] for c in col_order]
    ph = ", ".join("?" for _ in insert_safe_cols)
    ins_sql = (f'INSERT INTO "{tbl}" '
               f'({", ".join(chr(34)+c+chr(34) for c in insert_safe_cols)}) VALUES ({ph})')

    added = skipped = 0
    for r in clean_records:
        mapped = {safe_map[k]: (int(v) if isinstance(v, bool) else v)
                  for k, v in r.items() if k in safe_map}
        h = json.dumps(mapped, sort_keys=True, ensure_ascii=False)
        if h in existing_hashes:
            skipped += 1
            continue
        conn.execute(ins_sql, [mapped.get(safe_map[c]) for c in col_order])
        existing_hashes.add(h)
        added += 1

    conn.execute(
        'INSERT INTO "_import_log" (imported_at,source_file,table_code,rows_added,rows_skipped) VALUES(?,?,?,?,?)',
        (datetime.now().isoformat(), source, tbl, added, skipped)
    )
    return added, skipped


def _table_exists(conn, tbl):
    return conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
    ).fetchone()[0] > 0


# ══════════════════════════════════════════════════════════
# Page
# ══════════════════════════════════════════════════════════
@app.route("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    print("FSC 電子支付資料系統")
    print(f"資料庫: {DB_PATH}")
    port = int(os.environ.get("PORT", 5000))
    host = "0.0.0.0" if os.environ.get("RENDER") else "127.0.0.1"
    debug = not os.environ.get("RENDER")
    print(f"啟動中... http://{host}:{port}")
    app.run(debug=debug, host=host, port=port)
