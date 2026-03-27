#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FSC 電子支付資料工具
功能：查詢 / 分析 / 匯入新資料
資料庫：fsc_ebank.db
"""

import sqlite3
import json
import csv
import re
import os
import sys
from pathlib import Path
from datetime import datetime

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

# ── 設定 ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / "fsc_ebank.db"

SKIP_FIELDS = {"_url", "editable"}

# 各表說明
TABLE_DESC = {
    "EC002W": "儲值卡發行資料維護",
    "EC011W": "儲值卡類型資料維護",
    "EP005B": "電子支付帳戶戶數及使用者人數",
    "EP005W": "電子支付帳戶使用者別交易",
    "EP006B": "業務帳戶別交易資訊",
    "EP006W": "業務業務別交易資訊",
    "EP007W": "電子支付帳戶支付工具別交易",
    "EP007X": "儲值卡支付工具別交易",
    "EP008W": "特約機構交易",
    "EP010W": "實體通路支付服務交易",
    "EP010X": "代理收付通路別交易",
    "EP014W": "收受使用者支付款項餘額",
    "EP015W": "申訴案件統計",
    "EP105B": "境外業務客戶數",
    "EP105W": "境外業務客戶",
    "EP106B": "境外業務客戶交易",
    "EP106W": "境外業務業務別客戶交易",
    "EP106X": "與大陸地區合作業務",
    "EP107W": "境外業務客戶帳戶支付工具",
    "EP108W": "境外業務收款方客戶交易",
    "EP108X": "境外業務付款方客戶交易",
    "EP114W": "境外業務收受客戶支付款項餘額",
    "EP115W": "境外業務申訴案件統計",
    "WB031W": "電話申訴辦理情形",
    "WB032W": "申訴服務專線",
    "WB033W": "人民陳情案件辦理情形",
    "WB041W": "行動支付業務",
    "WB056W": "端末設備共用情形",
}

# ── 工具函數 ───────────────────────────────────────────────

def get_conn():
    if not DB_PATH.exists():
        print(f"[錯誤] 找不到資料庫：{DB_PATH}")
        print("請先執行 build_database.py 建立資料庫。")
        sys.exit(1)
    return sqlite3.connect(DB_PATH)

def safe_col(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_\u4e00-\u9fff]", "_", str(name))
    if s and s[0].isdigit():
        s = "c_" + s
    return s

def table_exists(conn, tbl):
    r = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
    ).fetchone()
    return r[0] > 0

def get_table_list(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE '\_%' ESCAPE '\\' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]

def get_columns(conn, tbl):
    rows = conn.execute(f'PRAGMA table_info("{tbl}")').fetchall()
    return [(r[1], r[2]) for r in rows]

def get_chinese_cols(conn, tbl):
    """取得中文欄位名稱（排除維護人員欄位）"""
    skip_patterns = {"維護部門名稱", "主管姓名", "主管電話", "承辦人姓名", "承辦人電話", "承辦人E_MAIL"}
    cols = get_columns(conn, tbl)
    return [c[0] for c in cols
            if c[0][0] >= '\u4e00' and c[0][-1] <= '\u9fff'
            and c[0] not in skip_patterns]

def print_hr(char="─", width=60):
    print(char * width)

def print_title(title):
    print_hr("═")
    print(f"  {title}")
    print_hr("═")

def print_section(title):
    print()
    print_hr()
    print(f"  {title}")
    print_hr()

def format_number(val):
    try:
        n = int(str(val).replace(",", ""))
        return f"{n:,}"
    except (ValueError, TypeError):
        return str(val) if val is not None else "-"

def display_df(df, max_rows=20, numeric_cols=None):
    """以文字格式顯示 DataFrame"""
    if df.empty:
        print("  （無資料）")
        return
    df_show = df.head(max_rows)
    if numeric_cols:
        for c in numeric_cols:
            if c in df_show.columns:
                df_show = df_show.copy()
                df_show[c] = df_show[c].apply(format_number)
    # 計算欄寬
    widths = {}
    for col in df_show.columns:
        max_w = max(
            len(str(col)),
            df_show[col].astype(str).str.len().max() if not df_show[col].empty else 0
        )
        widths[col] = min(max_w, 20)
    header = "  " + "  ".join(str(c)[:widths[c]].ljust(widths[c]) for c in df_show.columns)
    print(header)
    print("  " + "  ".join("-" * widths[c] for c in df_show.columns))
    for _, row in df_show.iterrows():
        line = "  " + "  ".join(str(row[c])[:widths[c]].ljust(widths[c]) for c in df_show.columns)
        print(line)
    if len(df) > max_rows:
        print(f"  ... 共 {len(df)} 筆，僅顯示前 {max_rows} 筆")

def input_int(prompt, min_val=None, max_val=None, default=None):
    while True:
        try:
            raw = input(prompt).strip()
            if raw == "" and default is not None:
                return default
            val = int(raw)
            if min_val is not None and val < min_val:
                print(f"  請輸入 {min_val} 以上的數字")
                continue
            if max_val is not None and val > max_val:
                print(f"  請輸入 {max_val} 以下的數字")
                continue
            return val
        except ValueError:
            print("  請輸入有效數字")

def pause():
    input("\n  按 Enter 繼續...")

# ══════════════════════════════════════════════════════════
# 模組一：查詢
# ══════════════════════════════════════════════════════════

def menu_query():
    conn = get_conn()
    while True:
        print_section("查詢模組")
        print("  1. 瀏覽資料表清單")
        print("  2. 查看資料表內容")
        print("  3. 依機構 / 月份條件篩選")
        print("  4. 自由 SQL 查詢")
        print("  5. 匯出查詢結果 (CSV)")
        print("  0. 返回主選單")
        choice = input("\n  請選擇 > ").strip()

        if choice == "1":
            query_list_tables(conn)
        elif choice == "2":
            query_browse_table(conn)
        elif choice == "3":
            query_filter(conn)
        elif choice == "4":
            query_free_sql(conn)
        elif choice == "5":
            query_export_csv(conn)
        elif choice == "0":
            break
    conn.close()


def query_list_tables(conn):
    print_section("資料表清單")
    tables = get_table_list(conn)
    print(f"  {'代碼':<10} {'說明':<30} {'筆數':>6}")
    print("  " + "-" * 50)
    for tbl in tables:
        cnt = conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
        desc = TABLE_DESC.get(tbl, "")
        print(f"  {tbl:<10} {desc:<30} {cnt:>6,}")
    pause()


def query_browse_table(conn):
    tables = get_table_list(conn)
    print_section("選擇資料表")
    for i, t in enumerate(tables, 1):
        print(f"  {i:2}. {t}  {TABLE_DESC.get(t, '')}")
    idx = input_int("\n  請選擇表號 > ", 1, len(tables)) - 1
    tbl = tables[idx]

    cols = get_columns(conn, tbl)
    print(f"\n  {tbl} 共 {len(cols)} 個欄位：")
    col_names = [c[0] for c in cols]
    for i, (name, typ) in enumerate(cols, 1):
        print(f"  {i:3}. {name:<35} {typ}")

    page_size = 20
    offset = 0
    total = conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]

    while True:
        rows = conn.execute(
            f'SELECT * FROM "{tbl}" LIMIT {page_size} OFFSET {offset}'
        ).fetchall()
        if not rows:
            print("  （無更多資料）")
            break

        print(f"\n  第 {offset+1}–{min(offset+page_size, total)} 筆 / 共 {total} 筆")
        if HAS_PANDAS:
            df = pd.DataFrame(rows, columns=col_names)
            display_df(df)
        else:
            header = "  " + " | ".join(str(c)[:12] for c in col_names[:8])
            print(header)
            for r in rows:
                print("  " + " | ".join(str(v)[:12] for v in r[:8]))

        print("\n  n=下一頁  p=上一頁  q=離開")
        cmd = input("  > ").strip().lower()
        if cmd == "n":
            if offset + page_size < total:
                offset += page_size
        elif cmd == "p":
            offset = max(0, offset - page_size)
        elif cmd == "q":
            break


def query_filter(conn):
    tables = get_table_list(conn)
    print_section("條件篩選查詢")
    for i, t in enumerate(tables, 1):
        print(f"  {i:2}. {t}  {TABLE_DESC.get(t, '')}")
    idx = input_int("\n  請選擇表號 > ", 1, len(tables)) - 1
    tbl = tables[idx]

    cols_info = get_columns(conn, tbl)
    col_names = [c[0] for c in cols_info]

    conditions = []
    params = []

    # 機構名稱篩選
    if "機構名稱" in col_names:
        orgs = conn.execute(f'SELECT DISTINCT 機構名稱 FROM "{tbl}" ORDER BY 機構名稱').fetchall()
        if len(orgs) > 1:
            print("\n  機構名稱：")
            for i, (org,) in enumerate(orgs, 1):
                print(f"    {i}. {org}")
            print("    0. 全部")
            oi = input_int("  請選擇 > ", 0, len(orgs), default=0)
            if oi > 0:
                conditions.append('機構名稱 = ?')
                params.append(orgs[oi-1][0])

    # 年份篩選
    if "yr" in col_names:
        yr_range = conn.execute(f'SELECT MIN(yr), MAX(yr) FROM "{tbl}"').fetchone()
        print(f"\n  年份範圍：{yr_range[0]} ~ {yr_range[1]}")
        yr_from = input(f"  起始年（留空=不限）> ").strip()
        yr_to   = input(f"  結束年（留空=不限）> ").strip()
        if yr_from:
            conditions.append("yr >= ?")
            params.append(int(yr_from))
        if yr_to:
            conditions.append("yr <= ?")
            params.append(int(yr_to))

    # 月份篩選
    if "mn" in col_names:
        mn_input = input("  月份（1-12，留空=不限）> ").strip()
        if mn_input:
            conditions.append("mn = ?")
            params.append(int(mn_input))

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f'SELECT * FROM "{tbl}" {where}'
    rows = conn.execute(sql, params).fetchall()
    print(f"\n  共 {len(rows)} 筆結果")

    if rows and HAS_PANDAS:
        df = pd.DataFrame(rows, columns=col_names)
        display_df(df)
        # 儲存供後續匯出
        conn._last_df = df
        conn._last_sql = sql

    pause()


def query_free_sql(conn):
    print_section("自由 SQL 查詢")
    print("  輸入 SQL 語句（輸入 'q' 結束）")
    print("  範例：SELECT yr, mn, 機構名稱, 發卡總數 FROM EC002W WHERE yr=115")
    print()
    while True:
        sql = input("  SQL> ").strip()
        if sql.lower() == "q":
            break
        if not sql:
            continue
        try:
            cur = conn.execute(sql)
            if cur.description:
                col_names = [d[0] for d in cur.description]
                rows = cur.fetchall()
                print(f"\n  共 {len(rows)} 筆")
                if HAS_PANDAS and rows:
                    df = pd.DataFrame(rows, columns=col_names)
                    display_df(df, max_rows=30)
            else:
                print(f"  執行完成，影響 {cur.rowcount} 筆")
        except sqlite3.Error as e:
            print(f"  [SQL 錯誤] {e}")
        print()


def query_export_csv(conn):
    print_section("匯出 CSV")
    sql = input("  輸入 SELECT 語句（留空使用上次查詢）> ").strip()
    if not sql:
        if hasattr(conn, '_last_sql'):
            sql = conn._last_sql
        else:
            print("  尚無查詢記錄，請先執行查詢。")
            pause()
            return
    try:
        cur = conn.execute(sql)
        col_names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        out_path = BASE_DIR / f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            writer.writerows(rows)
        print(f"  已匯出 {len(rows)} 筆 → {out_path.name}")
    except sqlite3.Error as e:
        print(f"  [錯誤] {e}")
    pause()


# ══════════════════════════════════════════════════════════
# 模組二：分析
# ══════════════════════════════════════════════════════════

def menu_analysis():
    conn = get_conn()
    while True:
        print_section("分析模組")
        print("  1. 資料概覽（各表月份範圍）")
        print("  2. 時間趨勢分析")
        print("  3. 各機構比較")
        print("  4. 數值欄位統計摘要")
        print("  5. 同比分析（當年 vs 上年同期）")
        print("  0. 返回主選單")
        choice = input("\n  請選擇 > ").strip()

        if choice == "1":
            analysis_overview(conn)
        elif choice == "2":
            analysis_trend(conn)
        elif choice == "3":
            analysis_compare_orgs(conn)
        elif choice == "4":
            analysis_summary_stats(conn)
        elif choice == "5":
            analysis_yoy(conn)
        elif choice == "0":
            break
    conn.close()


def analysis_overview(conn):
    print_section("資料概覽")
    tables = get_table_list(conn)
    print(f"  {'表代碼':<10} {'說明':<28} {'筆數':>5}  {'年份範圍':<15} {'機構數':>5}")
    print("  " + "-" * 70)
    for tbl in tables:
        cnt = conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
        desc = TABLE_DESC.get(tbl, "")[:26]
        cols = [c[0] for c, _ in [(c, c) for c in get_columns(conn, tbl)]]
        has_yr = "yr" in cols
        has_org = "機構名稱" in cols
        yr_str = ""
        org_cnt = 0
        if has_yr and cnt > 0:
            r = conn.execute(f'SELECT MIN(yr), MAX(yr) FROM "{tbl}"').fetchone()
            yr_str = f"{r[0]}~{r[1]}" if r[0] else ""
        if has_org and cnt > 0:
            org_cnt = conn.execute(f'SELECT COUNT(DISTINCT 機構名稱) FROM "{tbl}"').fetchone()[0]
        print(f"  {tbl:<10} {desc:<28} {cnt:>5,}  {yr_str:<15} {org_cnt:>5}")
    pause()


def analysis_trend(conn):
    tables = [t for t in get_table_list(conn)
              if "yr" in [c[0] for c in get_columns(conn, t)]]
    print_section("時間趨勢分析")
    for i, t in enumerate(tables, 1):
        print(f"  {i:2}. {t}  {TABLE_DESC.get(t, '')}")
    idx = input_int("\n  選擇資料表 > ", 1, len(tables)) - 1
    tbl = tables[idx]

    # 選擇數值欄位
    all_cols = get_columns(conn, tbl)
    zh_cols = [c[0] for c in all_cols
               if c[0][0] >= '\u4e00' and c[0][0] <= '\u9fff'
               and c[0] not in {"機構名稱","資料月份","維護部門名稱","主管姓名",
                                  "主管電話","承辦人姓名","承辦人電話","承辦人E_MAIL",
                                  "卡片名稱","交易類型","項目","申訴類別","客戶類別","核准業務別"}]
    if not zh_cols:
        print("  此表無適合分析的數值欄位")
        pause()
        return

    print("\n  可分析欄位：")
    for i, c in enumerate(zh_cols, 1):
        print(f"  {i:2}. {c}")
    ci = input_int("  選擇欄位 > ", 1, len(zh_cols)) - 1
    col = zh_cols[ci]

    # 機構篩選
    org_cond = ""
    org_param = []
    if "機構名稱" in [c[0] for c in all_cols]:
        orgs = conn.execute(f'SELECT DISTINCT 機構名稱 FROM "{tbl}" ORDER BY 機構名稱').fetchall()
        if len(orgs) > 1:
            print("\n  機構：")
            for i, (o,) in enumerate(orgs, 1):
                print(f"    {i}. {o}")
            print("    0. 全部合計")
            oi = input_int("  選擇 > ", 0, len(orgs), default=1)
            if oi > 0:
                org_cond = "WHERE 機構名稱 = ?"
                org_param = [orgs[oi-1][0]]

    sql = f'''
        SELECT yr, mn, CAST("{col}" AS REAL) as val
        FROM "{tbl}"
        {org_cond}
        ORDER BY yr, mn
    '''
    rows = conn.execute(sql, org_param).fetchall()
    if not rows:
        print("  無資料")
        pause()
        return

    print(f"\n  {tbl} — {col} 趨勢")
    print(f"  {'年/月':<10} {'數值':>20}  圖示")
    print("  " + "-" * 50)

    vals = [r[2] for r in rows if r[2] is not None]
    max_val = max(vals) if vals else 1
    bar_width = 30

    for yr, mn, val in rows:
        if val is None:
            continue
        bar_len = int((val / max_val) * bar_width) if max_val > 0 else 0
        bar = "█" * bar_len
        print(f"  {yr}/{mn:02d}      {format_number(val):>20}  {bar}")
    pause()


def analysis_compare_orgs(conn):
    tables = [t for t in get_table_list(conn)
              if "機構名稱" in [c[0] for c in get_columns(conn, t)]]
    print_section("各機構比較")
    for i, t in enumerate(tables, 1):
        print(f"  {i:2}. {t}  {TABLE_DESC.get(t, '')}")
    idx = input_int("\n  選擇資料表 > ", 1, len(tables)) - 1
    tbl = tables[idx]

    all_cols = get_columns(conn, tbl)
    col_names = [c[0] for c in all_cols]
    zh_cols = [c[0] for c in all_cols
               if c[0][0] >= '\u4e00' and c[0][0] <= '\u9fff'
               and c[0] not in {"機構名稱","資料月份","維護部門名稱","主管姓名",
                                  "主管電話","承辦人姓名","承辦人電話","承辦人E_MAIL",
                                  "卡片名稱","交易類型","項目","申訴類別","客戶類別","核准業務別"}]
    if not zh_cols:
        print("  此表無數值欄位")
        pause()
        return

    print("\n  數值欄位：")
    for i, c in enumerate(zh_cols, 1):
        print(f"  {i:2}. {c}")
    ci = input_int("  選擇欄位 > ", 1, len(zh_cols)) - 1
    col = zh_cols[ci]

    # 選擇月份
    if "yr" in col_names:
        r = conn.execute(f'SELECT MAX(yr), MAX(mn) FROM "{tbl}"').fetchone()
        print(f"\n  最新資料：{r[0]}/{r[1]:02d}")
        yr_in = input(f"  年份（預設={r[0]}）> ").strip() or str(r[0])
        mn_in = input(f"  月份（預設={r[1]}）> ").strip() or str(r[1])
        cond = "WHERE yr=? AND mn=?"
        params = [int(yr_in), int(mn_in)]
    else:
        cond, params = "", []

    sql = f'''
        SELECT 機構名稱, CAST("{col}" AS REAL) as val
        FROM "{tbl}"
        {cond}
        ORDER BY val DESC NULLS LAST
    '''
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        print("  無資料")
        pause()
        return

    vals = [r[1] for r in rows if r[1] is not None]
    max_val = max(vals) if vals else 1
    bar_width = 25
    print(f"\n  {tbl} — {col}（{yr_in}/{mn_in}）")
    print(f"  {'機構名稱':<20} {'數值':>18}  比例")
    print("  " + "-" * 65)
    total = sum(vals)
    for org, val in rows:
        if val is None:
            continue
        bar_len = int((val / max_val) * bar_width)
        pct = val / total * 100 if total > 0 else 0
        bar = "█" * bar_len
        print(f"  {str(org)[:20]:<20} {format_number(val):>18}  {bar} {pct:.1f}%")
    print(f"  {'合計':<20} {format_number(total):>18}")
    pause()


def analysis_summary_stats(conn):
    tables = get_table_list(conn)
    print_section("數值統計摘要")
    for i, t in enumerate(tables, 1):
        print(f"  {i:2}. {t}  {TABLE_DESC.get(t, '')}")
    idx = input_int("\n  選擇資料表 > ", 1, len(tables)) - 1
    tbl = tables[idx]

    all_cols = get_columns(conn, tbl)
    zh_cols = [c[0] for c in all_cols
               if c[0][0] >= '\u4e00' and c[0][0] <= '\u9fff'
               and c[0] not in {"機構名稱","資料月份","維護部門名稱","主管姓名",
                                  "主管電話","承辦人姓名","承辦人電話","承辦人E_MAIL",
                                  "卡片名稱","交易類型","項目","申訴類別","客戶類別","核准業務別"}]

    print(f"\n  {tbl} 統計摘要")
    print(f"  {'欄位':<28} {'最小':>15} {'最大':>15} {'平均':>15} {'筆數':>6}")
    print("  " + "-" * 85)
    for col in zh_cols[:15]:
        try:
            r = conn.execute(
                f'SELECT MIN(CAST("{col}" AS REAL)), MAX(CAST("{col}" AS REAL)),'
                f'       AVG(CAST("{col}" AS REAL)), COUNT("{col}")'
                f' FROM "{tbl}" WHERE "{col}" IS NOT NULL AND "{col}" != ""'
            ).fetchone()
            if r and r[0] is not None:
                print(f"  {col:<28} {format_number(r[0]):>15} {format_number(r[1]):>15}"
                      f" {r[2]:>15,.1f} {r[3]:>6,}")
        except Exception:
            pass
    pause()


def analysis_yoy(conn):
    """同比分析：當年 vs 上年同期"""
    tables = [t for t in get_table_list(conn)
              if "yr" in [c[0] for c in get_columns(conn, t)]]
    print_section("同比分析（YoY）")
    for i, t in enumerate(tables, 1):
        print(f"  {i:2}. {t}  {TABLE_DESC.get(t, '')}")
    idx = input_int("\n  選擇資料表 > ", 1, len(tables)) - 1
    tbl = tables[idx]

    all_cols = get_columns(conn, tbl)
    col_names = [c[0] for c in all_cols]
    zh_cols = [c[0] for c in all_cols
               if c[0][0] >= '\u4e00' and c[0][0] <= '\u9fff'
               and c[0] not in {"機構名稱","資料月份","維護部門名稱","主管姓名",
                                  "主管電話","承辦人姓名","承辦人電話","承辦人E_MAIL",
                                  "卡片名稱","交易類型","項目","申訴類別","客戶類別","核准業務別"}]
    if not zh_cols:
        print("  無數值欄位")
        pause()
        return

    print("\n  數值欄位：")
    for i, c in enumerate(zh_cols, 1):
        print(f"  {i:2}. {c}")
    ci = input_int("  選擇欄位 > ", 1, len(zh_cols)) - 1
    col = zh_cols[ci]

    r = conn.execute(f'SELECT MAX(yr) FROM "{tbl}"').fetchone()
    cur_yr = r[0]
    prev_yr = cur_yr - 1

    org_cond = ""
    org_param_cur = []
    org_param_prev = []
    if "機構名稱" in col_names:
        orgs = conn.execute(
            f'SELECT DISTINCT 機構名稱 FROM "{tbl}" ORDER BY 機構名稱'
        ).fetchall()
        if len(orgs) > 1:
            print("\n  機構：")
            for i, (o,) in enumerate(orgs, 1):
                print(f"    {i}. {o}")
            print("    0. 全部合計")
            oi = input_int("  選擇 > ", 0, len(orgs), default=0)
            if oi > 0:
                org_cond = "AND 機構名稱 = ?"
                org_param_cur = [orgs[oi-1][0]]
                org_param_prev = [orgs[oi-1][0]]

    def get_monthly(yr):
        sql = f'''
            SELECT mn, SUM(CAST("{col}" AS REAL))
            FROM "{tbl}"
            WHERE yr=? {org_cond}
            GROUP BY mn ORDER BY mn
        '''
        return {r[0]: r[1] for r in conn.execute(sql, [yr] + (org_param_cur if yr == cur_yr else org_param_prev)).fetchall()}

    cur_data  = get_monthly(cur_yr)
    prev_data = get_monthly(prev_yr)

    all_months = sorted(set(cur_data) | set(prev_data))
    print(f"\n  {tbl} — {col} 同比")
    print(f"  {'月份':<6} {str(cur_yr)+'年':>18} {str(prev_yr)+'年':>18} {'YoY%':>8}")
    print("  " + "-" * 55)
    for mn in all_months:
        cur_v  = cur_data.get(mn)
        prev_v = prev_data.get(mn)
        yoy_str = ""
        if cur_v is not None and prev_v is not None and prev_v != 0:
            yoy = (cur_v - prev_v) / abs(prev_v) * 100
            arrow = "▲" if yoy >= 0 else "▼"
            yoy_str = f"{arrow}{abs(yoy):.1f}%"
        print(f"  {mn:>2}月   {format_number(cur_v) if cur_v else '-':>18}"
              f" {format_number(prev_v) if prev_v else '-':>18} {yoy_str:>8}")
    pause()


# ══════════════════════════════════════════════════════════
# 模組三：匯入新資料
# ══════════════════════════════════════════════════════════

def menu_import():
    conn = get_conn()
    while True:
        print_section("匯入新資料")
        print("  1. 從 ebank_all_data*.json 匯入")
        print("  2. 從單一 ebank_XXXXX.json 匯入")
        print("  3. 從 CSV 檔匯入")
        print("  4. 查看匯入記錄")
        print("  0. 返回主選單")
        choice = input("\n  請選擇 > ").strip()

        if choice == "1":
            import_all_data_json(conn)
        elif choice == "2":
            import_single_json(conn)
        elif choice == "3":
            import_csv(conn)
        elif choice == "4":
            import_show_log(conn)
        elif choice == "0":
            break
    conn.close()


def ensure_import_log(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS "_import_log" (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            imported_at TEXT,
            source_file TEXT,
            table_code  TEXT,
            rows_added  INTEGER,
            rows_skipped INTEGER
        )
    ''')

def safe_name_import(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_\u4e00-\u9fff]", "_", str(name))
    if s and s[0].isdigit():
        s = "c_" + s
    return s

def resolve_safe_map(col_list):
    """將欄位名稱列表轉換為不衝突的 safe_name map（SQLite 不分大小寫）"""
    seen_safe = {}
    safe_map = {}
    for col in col_list:
        s = safe_name_import(col)
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
    return safe_map

def upsert_records(conn, tbl, records, source_file=""):
    """將 records 插入資料表，跳過已存在的完全相同資料"""
    if not records:
        return 0, 0

    # 確保表存在，若不存在則建立
    existing_cols = {}
    if table_exists(conn, tbl):
        for info in conn.execute(f'PRAGMA table_info("{tbl}")').fetchall():
            existing_cols[info[1]] = info[2]

    # 收集新資料的所有欄位
    new_col_set = {}
    col_order = []
    for r in records:
        for k, v in r.items():
            if k in SKIP_FIELDS:
                continue
            if k not in new_col_set:
                new_col_set[k] = "INTEGER" if isinstance(v, int) and not isinstance(v, bool) else \
                                  "REAL" if isinstance(v, float) else "TEXT"
                col_order.append(k)

    safe_map = resolve_safe_map(col_order)

    if not existing_cols:
        # 建立新表
        col_defs = ", ".join(f'"{safe_map[c]}" {new_col_set[c]}' for c in col_order)
        conn.execute(f'CREATE TABLE "{tbl}" ({col_defs})')
        existing_cols = {safe_map[c]: new_col_set[c] for c in col_order}

    # 若表已有，補充缺少的欄位
    for orig_col, safe_col in safe_map.items():
        if safe_col not in existing_cols:
            typ = new_col_set[orig_col]
            conn.execute(f'ALTER TABLE "{tbl}" ADD COLUMN "{safe_col}" {typ}')
            existing_cols[safe_col] = typ

    # 取得現有所有資料的 JSON hash set 用於去重
    existing_rows = conn.execute(f'SELECT * FROM "{tbl}"').fetchall()
    existing_hashes = set()
    ex_cols = [c[0] for c in get_columns(conn, tbl)]
    for row in existing_rows:
        d = {ex_cols[i]: row[i] for i in range(len(ex_cols))}
        existing_hashes.add(json.dumps(d, sort_keys=True, ensure_ascii=False))

    added = 0
    skipped = 0
    insert_cols_safe = [safe_map[c] for c in col_order]
    placeholders = ", ".join("?" for _ in insert_cols_safe)
    insert_sql = (f'INSERT INTO "{tbl}" '
                  f'({", ".join(chr(34)+c+chr(34) for c in insert_cols_safe)}) '
                  f'VALUES ({placeholders})')

    for r in records:
        clean = {safe_map[k]: (int(v) if isinstance(v, bool) else v)
                 for k, v in r.items() if k not in SKIP_FIELDS and k in safe_map}
        h = json.dumps(clean, sort_keys=True, ensure_ascii=False)
        if h in existing_hashes:
            skipped += 1
            continue
        row_vals = [clean.get(safe_map[c]) for c in col_order]
        conn.execute(insert_sql, row_vals)
        existing_hashes.add(h)
        added += 1

    return added, skipped


def import_all_data_json(conn):
    print_section("匯入 ebank_all_data*.json")
    # 搜尋可用檔案
    json_files = sorted(BASE_DIR.glob("ebank_all_data*.json"))
    json_files = [f for f in json_files if "拷貝" not in f.name]
    if not json_files:
        print("  找不到 ebank_all_data*.json 檔案")
        pause()
        return

    print("  可用檔案：")
    for i, f in enumerate(json_files, 1):
        print(f"  {i}. {f.name}")
    idx = input_int("  選擇檔案 > ", 1, len(json_files)) - 1
    fpath = json_files[idx]

    with open(fpath, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    ensure_import_log(conn)
    total_added = 0
    total_skipped = 0

    print(f"\n  處理 {fpath.name}...")
    for table_key, content in data.items():
        tbl = table_key.split("_")[0]
        records = content.get("details", [])
        added, skipped = upsert_records(conn, tbl, records, fpath.name)
        if added > 0 or skipped > 0:
            print(f"  {tbl:<10} 新增 {added:4d}  跳過 {skipped:4d}")
        conn.execute(
            'INSERT INTO "_import_log" (imported_at, source_file, table_code, rows_added, rows_skipped) VALUES (?,?,?,?,?)',
            (datetime.now().isoformat(), fpath.name, tbl, added, skipped)
        )
        total_added += added
        total_skipped += skipped

    conn.commit()
    print(f"\n  完成！新增 {total_added} 筆，跳過重複 {total_skipped} 筆")
    pause()


def import_single_json(conn):
    print_section("匯入單一 JSON")
    json_files = sorted(BASE_DIR.glob("ebank_*.json"))
    json_files = [f for f in json_files
                  if "拷貝" not in f.name
                  and "all_data" not in f.name
                  and not f.name.endswith("拷貝.json")]
    # 去除 (1) 等重複
    if not json_files:
        print("  找不到 JSON 檔案")
        pause()
        return

    print("  可用檔案：")
    for i, f in enumerate(json_files, 1):
        print(f"  {i:3}. {f.name}")
    raw = input("\n  輸入檔案序號或完整路徑 > ").strip()

    try:
        idx = int(raw) - 1
        fpath = json_files[idx]
    except (ValueError, IndexError):
        fpath = Path(raw)

    if not fpath.exists():
        print(f"  [錯誤] 找不到檔案：{fpath}")
        pause()
        return

    with open(fpath, "r", encoding="utf-8-sig") as f:
        data = json.load(f)

    ensure_import_log(conn)

    # 支援兩種格式：{itemName, details} 或 {tableKey: {details}}
    if "details" in data and "itemName" in data:
        tbl = data["itemName"].split("_")[0]
        records = data["details"]
        added, skipped = upsert_records(conn, tbl, records, fpath.name)
        conn.execute(
            'INSERT INTO "_import_log" (imported_at, source_file, table_code, rows_added, rows_skipped) VALUES (?,?,?,?,?)',
            (datetime.now().isoformat(), fpath.name, tbl, added, skipped)
        )
        conn.commit()
        print(f"\n  {tbl}: 新增 {added} 筆，跳過 {skipped} 筆")
    elif isinstance(data, dict):
        for key, content in data.items():
            if isinstance(content, dict) and "details" in content:
                tbl = key.split("_")[0]
                records = content["details"]
                added, skipped = upsert_records(conn, tbl, records, fpath.name)
                conn.execute(
                    'INSERT INTO "_import_log" (imported_at, source_file, table_code, rows_added, rows_skipped) VALUES (?,?,?,?,?)',
                    (datetime.now().isoformat(), fpath.name, tbl, added, skipped)
                )
                print(f"  {tbl}: 新增 {added} 筆，跳過 {skipped} 筆")
        conn.commit()
    else:
        print("  無法識別的 JSON 格式")
    pause()


def import_csv(conn):
    print_section("匯入 CSV")
    csv_files = sorted(BASE_DIR.glob("*.csv"))
    csv_files = [f for f in csv_files if "拷貝" not in f.name]
    if not csv_files:
        print("  找不到 CSV 檔案")
        pause()
        return

    print("  可用 CSV：")
    for i, f in enumerate(csv_files, 1):
        print(f"  {i:3}. {f.name}")
    raw = input("\n  輸入序號或路徑 > ").strip()
    try:
        idx = int(raw) - 1
        fpath = csv_files[idx]
    except (ValueError, IndexError):
        fpath = Path(raw)

    if not fpath.exists():
        print(f"  [錯誤] 找不到：{fpath}")
        pause()
        return

    # 猜測表名
    tbl_guess = fpath.stem.split("_")[0].upper()
    tbl = input(f"  目標資料表（預設={tbl_guess}）> ").strip() or tbl_guess

    with open(fpath, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        records = [dict(r) for r in reader]

    if not records:
        print("  CSV 無資料")
        pause()
        return

    print(f"  預覽前 3 筆（共 {len(records)} 筆）：")
    for r in records[:3]:
        print(f"    {dict(list(r.items())[:5])}")

    confirm = input(f"\n  確認匯入至 {tbl}？(y/n) > ").strip().lower()
    if confirm != "y":
        print("  取消")
        pause()
        return

    ensure_import_log(conn)
    added, skipped = upsert_records(conn, tbl, records, fpath.name)
    conn.execute(
        'INSERT INTO "_import_log" (imported_at, source_file, table_code, rows_added, rows_skipped) VALUES (?,?,?,?,?)',
        (datetime.now().isoformat(), fpath.name, tbl, added, skipped)
    )
    conn.commit()
    print(f"\n  完成！新增 {added} 筆，跳過 {skipped} 筆")
    pause()


def import_show_log(conn):
    ensure_import_log(conn)
    rows = conn.execute(
        'SELECT imported_at, source_file, table_code, rows_added, rows_skipped '
        'FROM "_import_log" ORDER BY id DESC LIMIT 30'
    ).fetchall()
    print_section("匯入記錄（最近 30 筆）")
    if not rows:
        print("  尚無匯入記錄")
    else:
        print(f"  {'時間':<22} {'來源檔案':<35} {'表':<8} {'新增':>6} {'跳過':>6}")
        print("  " + "-" * 82)
        for r in rows:
            print(f"  {r[0][:19]:<22} {r[1][:33]:<35} {r[2]:<8} {r[3]:>6} {r[4]:>6}")
    pause()


# ══════════════════════════════════════════════════════════
# 主選單
# ══════════════════════════════════════════════════════════

def main():
    while True:
        print_title("FSC 電子支付資料工具")
        print("  1. 查詢模組  ─ 瀏覽 / 篩選 / SQL / 匯出 CSV")
        print("  2. 分析模組  ─ 趨勢 / 機構比較 / 統計 / 同比")
        print("  3. 匯入模組  ─ 匯入 JSON / CSV，自動去重")
        print()
        print("  0. 離開")

        if not DB_PATH.exists():
            print(f"\n  [警告] 找不到資料庫 {DB_PATH.name}")
            print("  請先執行 build_database.py 建立資料庫。")

        choice = input("\n  請選擇 > ").strip()
        if choice == "1":
            menu_query()
        elif choice == "2":
            menu_analysis()
        elif choice == "3":
            menu_import()
        elif choice == "0":
            print("\n  再見！")
            break


if __name__ == "__main__":
    main()
