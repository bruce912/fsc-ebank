#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
資料庫遷移驗證腳本
1. 以 SQLite backup API 建立全新 fsc_ebank_new.db
2. 驗證每張表：列數、欄位名稱、所有資料列 hash 相符
3. 驗證通過後以 fsc_ebank_new.db 取代 fsc_ebank.db
   並保留 fsc_ebank_backup.db 作為備份
"""

import sqlite3
import hashlib
import json
import shutil
import sys
from pathlib import Path

BASE_DIR  = Path(__file__).parent
SRC_DB    = BASE_DIR / "fsc_ebank.db"
NEW_DB    = BASE_DIR / "fsc_ebank_new.db"
BACKUP_DB = BASE_DIR / "fsc_ebank_backup.db"

SKIP_TABLES = {"sqlite_sequence", "_import_log"}

# ── 工具：取得 table 列表 ────────────────────────────────────
def get_tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows if r[0] not in SKIP_TABLES]

# ── 工具：計算整張表的行雜湊（可偵測任何欄位值差異）──────────
def table_hash(conn, tbl):
    cols = [c[1] for c in conn.execute(f'PRAGMA table_info("{tbl}")').fetchall()]
    col_sql = ", ".join(f'"{c}"' for c in cols)
    rows = conn.execute(f'SELECT {col_sql} FROM "{tbl}" ORDER BY rowid').fetchall()
    h = hashlib.sha256()
    for row in rows:
        h.update(json.dumps(row, ensure_ascii=False, default=str).encode())
    return h.hexdigest(), len(rows), cols

# ── 步驟 1：備份原始資料庫 ──────────────────────────────────
def step_backup():
    print("📦 步驟 1：備份現有資料庫")
    shutil.copy2(SRC_DB, BACKUP_DB)
    size = BACKUP_DB.stat().st_size / 1024
    print(f"   ✅ {BACKUP_DB.name}  ({size:.1f} KB)\n")

# ── 步驟 2：使用 SQLite backup API 建立新資料庫 ─────────────
def step_copy():
    print("🔨 步驟 2：建立新資料庫 fsc_ebank_new.db")
    if NEW_DB.exists():
        NEW_DB.unlink()
    src = sqlite3.connect(SRC_DB)
    dst = sqlite3.connect(NEW_DB)
    src.backup(dst, pages=1000)   # 全庫複製
    src.close()
    dst.close()
    size = NEW_DB.stat().st_size / 1024
    print(f"   ✅ {NEW_DB.name}  ({size:.1f} KB)\n")

# ── 步驟 3：逐表驗證 ────────────────────────────────────────
def step_verify():
    print("🔍 步驟 3：逐表驗證（欄位數、列數、資料 hash）\n")
    src = sqlite3.connect(SRC_DB)
    dst = sqlite3.connect(NEW_DB)

    src_tables = get_tables(src)
    dst_tables = set(get_tables(dst))

    errors = []

    # 表格標頭
    print(f"{'表名':<22} {'列數(舊)':<10} {'列數(新)':<10} {'欄位數':<8} {'Hash':<12} 結果")
    print("─" * 75)

    total_rows_src = 0
    total_rows_dst = 0

    for tbl in src_tables:
        src_hash, src_cnt, src_cols = table_hash(src, tbl)
        total_rows_src += src_cnt

        if tbl not in dst_tables:
            print(f"{tbl:<22} {src_cnt:<10} {'—':<10} {'—':<8} {'—':<12} ❌ 新庫缺少此表")
            errors.append(f"表 {tbl} 在新庫中不存在")
            continue

        dst_hash, dst_cnt, dst_cols = table_hash(dst, tbl)
        total_rows_dst += dst_cnt

        # 欄位差異
        col_diff = set(src_cols) ^ set(dst_cols)
        if col_diff:
            errors.append(f"表 {tbl}: 欄位差異 {col_diff}")

        # Hash 比對
        hash_ok   = (src_hash == dst_hash)
        count_ok  = (src_cnt == dst_cnt)
        cols_ok   = (not col_diff)
        all_ok    = hash_ok and count_ok and cols_ok

        hash_str = src_hash[:8] + "…"
        mark     = "✅" if all_ok else "❌"
        print(f"{tbl:<22} {src_cnt:<10} {dst_cnt:<10} {len(src_cols):<8} {hash_str:<12} {mark}")

        if not count_ok:
            errors.append(f"表 {tbl}: 列數不符（舊 {src_cnt}，新 {dst_cnt}）")
        if not hash_ok and count_ok and cols_ok:
            errors.append(f"表 {tbl}: 資料 hash 不符（列數相同但內容有差異）")

    src.close()
    dst.close()

    print("─" * 75)
    print(f"{'合計':<22} {total_rows_src:<10} {total_rows_dst:<10}")
    return errors

# ── 步驟 4：取代舊資料庫 ─────────────────────────────────────
def step_replace():
    print("\n🔁 步驟 4：以新資料庫取代舊資料庫")
    SRC_DB.unlink()
    NEW_DB.rename(SRC_DB)
    print(f"   ✅ {NEW_DB.name} → {SRC_DB.name}")
    print(f"   📦 備份保留於 {BACKUP_DB.name}")

# ── 主流程 ──────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 75)
    print("  FSC 資料庫遷移與驗證")
    print("=" * 75 + "\n")

    if not SRC_DB.exists():
        print(f"❌ 找不到 {SRC_DB}，請確認路徑。")
        sys.exit(1)

    step_backup()
    step_copy()
    errors = step_verify()

    print()
    if errors:
        print(f"❌ 發現 {len(errors)} 個驗證問題：")
        for e in errors:
            print(f"   • {e}")
        print(f"\n⚠️  遷移中止，原資料庫未變動。備份：{BACKUP_DB.name}")
        if NEW_DB.exists():
            NEW_DB.unlink()
        sys.exit(1)
    else:
        step_replace()
        print("\n" + "=" * 75)
        print("  ✅ 遷移完成！所有資料均已驗證，無任何錯置。")
        print("=" * 75)
