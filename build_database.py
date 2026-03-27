#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FSC電子支付資料 → SQLite 資料庫建立腳本
- 主要來源: ebank_all_data_final.json
- 補充歷史資料: ebank_all_data.json (針對更早月份)
- 排除重複資料 (拷貝檔案)
- 輸出: fsc_ebank.db
"""

import json
import sqlite3
import re
import os
import csv
from pathlib import Path

BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "fsc_ebank.db"

# ── 載入主要資料源 ──────────────────────────────────────────
def load_json(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

final_data = load_json(BASE_DIR / "ebank_all_data_final.json")
old_data   = load_json(BASE_DIR / "ebank_all_data.json")

# ── 表名/欄位名清理 (SQLite 不接受特殊字元) ──────────────────
def safe_name(name: str) -> str:
    """將中文/特殊字元欄位名轉為安全識別符"""
    # 保留英數字、底線；其他替換為 _
    s = re.sub(r"[^a-zA-Z0-9_\u4e00-\u9fff]", "_", str(name))
    # SQLite 識別符不能以數字開頭
    if s and s[0].isdigit():
        s = "c_" + s
    return s

def table_name_from_key(key: str) -> str:
    """EC002W_儲值卡發行資料維護 → EC002W"""
    return key.split("_")[0]

# ── 合併資料：final 為主，old 補充更早月份 ───────────────────
# 對於 EP010W / EP015W / EP105B 等欄位格式不同的表，只用 final
# 對於 EP010X / EP105W / EP106B / EP106W，old 有更多歷史月份需合併
SUPPLEMENT_TABLES = {
    "EP010X_電子支付機構代理收付實質交易款項業務通路別交易資料維護",
    "EP105W_與境外機構合作或協助相關電子支付業務客戶資料維護",
    "EP106B_與境外機構合作或協助相關電子支付機構業務客戶交易資料維護",
    "EP106W_與境外機構合作或協助相關電子支付機構業務業務別客戶交易資料維護",
}

def build_merged_records(table_key):
    """回傳去重後的 list[dict]（已移除 _url / editable 欄位）"""
    skip_keys = {"_url", "editable"}

    final_records = final_data[table_key]["details"]

    # 取得 final 已涵蓋的 (yr, mn) 組合
    final_ym = set((r.get("yr"), r.get("mn")) for r in final_records)

    all_records = []

    # 先放 final 所有記錄
    for r in final_records:
        clean = {k: v for k, v in r.items() if k not in skip_keys}
        all_records.append(clean)

    # 補充 old 中 final 沒有的月份（僅限指定表）
    if table_key in SUPPLEMENT_TABLES and table_key in old_data:
        for r in old_data[table_key]["details"]:
            if (r.get("yr"), r.get("mn")) not in final_ym:
                clean = {k: v for k, v in r.items() if k not in skip_keys}
                all_records.append(clean)

    # JSON-level dedup
    seen = {}
    for r in all_records:
        k = json.dumps(r, sort_keys=True, ensure_ascii=False)
        seen[k] = r

    return list(seen.values())

# ── 動態建表並插入資料 ───────────────────────────────────────
def infer_col_type(val):
    if isinstance(val, bool):
        return "INTEGER"
    if isinstance(val, int):
        return "INTEGER"
    if isinstance(val, float):
        return "REAL"
    return "TEXT"

def create_and_insert(conn, table_key, records):
    if not records:
        print(f"  [SKIP] {table_key} — 0 筆資料")
        return 0

    tbl = table_name_from_key(table_key)

    # 收集所有欄位（取 union）
    all_cols = {}   # original_name -> type
    col_order = []  # preserve order
    for r in records:
        for col, val in r.items():
            if col not in all_cols:
                all_cols[col] = infer_col_type(val)
                col_order.append(col)
            else:
                if all_cols[col] == "INTEGER" and isinstance(val, float):
                    all_cols[col] = "REAL"

    # 解決 safe_name 後的重名衝突：加數字後綴
    # 注意：SQLite 欄位名稱不分大小寫，必須用 lower() 比對
    safe_map = {}   # original_name -> final_safe_name
    seen_safe = {}  # safe_name.lower() -> count
    used_safe = set()  # 已使用的 safe_name（保留原始大小寫比較，用 lower set 防重）
    for col in col_order:
        s = safe_name(col)
        s_lower = s.lower()
        if s_lower in seen_safe:
            while True:
                seen_safe[s_lower] += 1
                candidate = f"{s}_{seen_safe[s_lower]}"
                if candidate.lower() not in seen_safe:
                    s = candidate
                    seen_safe[s.lower()] = 0
                    break
        else:
            seen_safe[s_lower] = 0
        safe_map[col] = s

    col_defs = ", ".join(f'"{safe_map[c]}" {all_cols[c]}' for c in col_order)
    conn.execute(f'DROP TABLE IF EXISTS "{tbl}"')
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{tbl}" ({col_defs})')

    safe_cols = [safe_map[c] for c in col_order]
    placeholders = ", ".join("?" for _ in safe_cols)
    insert_sql = f'INSERT INTO "{tbl}" ({", ".join(chr(34)+c+chr(34) for c in safe_cols)}) VALUES ({placeholders})'

    for r in records:
        row = []
        for col in col_order:
            val = r.get(col)
            if isinstance(val, bool):
                val = int(val)
            row.append(val)
        conn.execute(insert_sql, row)

    return len(records)

# ── WB032W 申訴服務專線 (獨立 CSV 格式) ────────────────────
def insert_wb032w(conn):
    csv_path = BASE_DIR / "WB032W_申訴服務專線維護.csv"
    if not csv_path.exists():
        return
    rows = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        raw = list(reader)
    # 格式: 第1行是機構資訊, 第2行是欄頭, 之後是資料
    # Row0: 機構名稱, <名稱>, 作業人員, <姓名>
    # Row1: 受理申訴部門, 服務電話, 傳真號碼
    # Row2+: 資料行
    if len(raw) < 3:
        return
    org_name = raw[0][1] if len(raw[0]) > 1 else ""
    operator = raw[0][3] if len(raw[0]) > 3 else ""
    headers = raw[1]
    conn.execute('DROP TABLE IF EXISTS "WB032W"')
    conn.execute('''CREATE TABLE "WB032W" (
        機構名稱 TEXT,
        作業人員 TEXT,
        受理申訴部門 TEXT,
        服務電話 TEXT,
        傳真號碼 TEXT
    )''')
    for row in raw[2:]:
        if len(row) >= 3:
            conn.execute('INSERT INTO "WB032W" VALUES (?,?,?,?,?)',
                         (org_name, operator, row[0], row[1], row[2]))
    print(f"  WB032W: {len(raw)-2} 筆")

# ── 主流程 ───────────────────────────────────────────────────
def main():
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    total_rows = 0
    print("建立資料庫...")

    for table_key in final_data:
        records = build_merged_records(table_key)
        n = create_and_insert(conn, table_key, records)
        tbl = table_name_from_key(table_key)
        print(f"  {tbl}: {n} 筆")
        total_rows += n

    insert_wb032w(conn)

    # ── 建立 metadata 表 ────────────────────────────────────
    conn.execute('DROP TABLE IF EXISTS "_metadata"')
    conn.execute('''CREATE TABLE "_metadata" (
        table_code TEXT PRIMARY KEY,
        table_name TEXT,
        row_count  INTEGER,
        description TEXT
    )''')
    TABLE_DESC = {
        "EC002W": "儲值卡發行資料維護",
        "EC011W": "儲值卡類型資料維護",
        "EP005B": "電子支付帳戶戶數及使用者人數資料維護",
        "EP005W": "電子支付帳戶使用者別交易資料維護",
        "EP006B": "電子支付機構業務帳戶別交易資訊應申報資料維護",
        "EP006W": "電子支付機構業務業務別交易資訊應申報資料維護",
        "EP007W": "電子支付帳戶支付工具別交易資料維護",
        "EP007X": "儲值卡支付工具別交易資料維護",
        "EP008W": "電子支付機構特約機構交易資料維護",
        "EP010W": "電子支付機構實體通路支付服務交易資料維護",
        "EP010X": "電子支付機構代理收付實質交易款項業務通路別交易資料維護",
        "EP014W": "電子支付機構收受使用者支付款項餘額資料維護",
        "EP015W": "電子支付機構申訴案件統計情形資料維護",
        "EP105B": "與境外機構合作或協助相關電子支付機構業務客戶數資料維護",
        "EP105W": "與境外機構合作或協助相關電子支付業務客戶資料維護",
        "EP106B": "與境外機構合作或協助相關電子支付機構業務客戶交易資料維護",
        "EP106W": "與境外機構合作或協助相關電子支付機構業務業務別客戶交易資料維護",
        "EP106X": "與境外機構大陸地區合作或協助相關電子支付機構業務資料維護",
        "EP107W": "與境外機構合作或協助相關電子支付機構業務客戶帳戶支付工具資料維護",
        "EP108W": "與境外機構合作或協助相關電子支付機構業務收款方客戶交易資料維護",
        "EP108X": "與境外機構合作或協助相關電子支付機構業務付款方客戶交易資料維護",
        "EP114W": "與境外機構合作或協助相關電子支付機構業務收受客戶支付款項餘額資料",
        "EP115W": "與境外機構合作或協助相關電子支付機構業務申訴案件統計情形資料維護",
        "WB031W": "電話申訴辦理情形資料維護",
        "WB032W": "申訴服務專線維護",
        "WB033W": "人民陳情案件辦理情形資料維護",
        "WB041W": "行動支付業務資料維護",
        "WB056W": "電子支付機構端末設備共用情形資料維護",
    }
    for code, desc in TABLE_DESC.items():
        # Check if table exists before querying
        exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (code,)
        ).fetchone()[0]
        cnt = conn.execute(f'SELECT COUNT(*) FROM "{code}"').fetchone()[0] if exists else 0
        conn.execute('INSERT INTO "_metadata" VALUES (?,?,?,?)',
                     (code, f"{code}_{desc}", cnt, desc))

    conn.commit()
    conn.close()

    size_kb = DB_PATH.stat().st_size / 1024
    print(f"\n完成！資料庫: {DB_PATH}")
    print(f"總計 {total_rows} 筆資料，檔案大小: {size_kb:.1f} KB")

if __name__ == "__main__":
    main()
