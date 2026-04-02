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
    "月票交易統計": "行政院月票方案交易統計",
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

def is_data_col(name: str) -> bool:
    """系統數值欄位：_N__amt/_N__xact 等，及 c_XXXX 合計欄，以及 WB041W 的 cards/xact/amt"""
    if name in ('cards', 'xact', 'amt'):
        return True
    return bool(re.match(r'^_\d+', name) or re.match(r'^c_\d+', name))

# 顯示用：保留中文欄位 + yr/mn 作為日期錨點（內部用）
DISPLAY_ANCHOR = {"yr", "mn"}
# 比較/趨勢：排除這些非數值中文欄
TEXT_CHINESE_COLS = {"機構名稱","資料月份","卡片名稱","交易類型","項目","申訴類別",
                     "客戶類別","核准業務別","業務項目_合作對象","機構別",
                     "儲值卡類型","付款方式","使用服務種類","服務種類"}

# ── 各表在「資料瀏覽」中額外顯示的摘要系統欄 ──────────────────
TABLE_SUMMARY_COLS = {
    "EP005B": ["c_3000A", "_5__amt", "_6__amt"],
    "EP005W": ["_1900A","_1900B","_1900C","_8900A","_8900B","_8900C"],
    "EP006B": ["_3000A","_3000B","_2900A","_2900B"],
    "EP006W": ["c_1999X","c_1999Y"],
    "EP007W": ["_1999A","_1999B","_2999A","_2999B","_4999A","_4999B","_9000A","_9000B"],
    "EP007X": ["_1999A","_1999B","_1999C"],
    "EP008W": ["c_3000A","c_3000B","c_3000C","c_6000A","c_6000B","c_6000C"],
    "EP010X": ["_0__amt","_1__amt","_5__amt","_6__amt","_7__amt","_8__amt"],
    "EP014W": ["c_1900A","c_3000A","c_4900A","c_6000A","c_9000A"],
    "EP015W": ["_1900A","_1900B","_1900C"],
    "WB041W": ["cards","xact","amt",
               "_0__cards","_0__xact","_0__amt",
               "_1__cards","_1__xact","_1__amt",
               "_2__cards","_2__xact","_2__amt",
               "_3__cards","_3__xact","_3__amt",
               "_4__cards","_4__xact","_4__amt",
               "_5__cards","_5__xact","_5__amt",
               "_6__cards","_6__xact","_6__amt"],
}

# ── 完整欄位中文定義（所有表、所有欄位） ───────────────────────
def _make_labels():
    L = {}

    # ── EC002W 儲值卡發行 ─────────────────────────────────────
    L["EC002W"] = {
        "b":     "全部-發卡總數",           "b2":    "重複加值式-發卡總數",
        "c":     "全部-流通卡數",           "c2":    "重複加值式-流通卡數",
        "d":     "全部-當月發卡數",         "d2":    "重複加值式-當月發卡數",
        "e":     "全部-當月停卡數",         "e2":    "重複加值式-當月停卡數",
        "f":     "全部-當月贖回金額",       "f2":    "重複加值式-當月贖回金額",
        "g":     "全部-當月偽冒停卡張數",   "g2":    "重複加值式-當月偽冒停卡張數",
        "h":     "全部-當年累計偽冒停卡張數","h2":   "重複加值式-當年累計偽冒停卡張數",
        "i":     "全部-當月偽冒損失金額",   "i2":    "重複加值式-當月偽冒損失金額",
        "j":     "全部-當年累計偽冒損失金額","j2":   "重複加值式-當年累計偽冒損失金額",
        "k":     "全部-當月儲值金額",       "k2":    "重複加值式-當月儲值金額",
        "l":     "全部-當月儲值卡數",       "l2":    "重複加值式-當月儲值卡數",
        "m":     "全部-當月儲值次數",       "m2":    "重複加值式-當月儲值次數",
        "n":     "全部-當月消費金額",       "n2":    "重複加值式-當月消費金額",
        "n_tot": "全部-消費金額合計",       "n2_tot":"重複加值式-消費金額合計",
        "p":     "全部-當月消費筆數",       "p2":    "重複加值式-當月消費筆數",
        "p_tot": "全部-消費筆數合計",       "p2_tot":"重複加值式-消費筆數合計",
        "sn":    "卡片序號",
    }

    # ── EC011W 儲值卡類型（18種卡型 × 流通卡數/新增/交易筆數/金額/備註）
    _card_types = [
        "記名-聯名卡","記名-政府機關合作","記名-電信業者合作",
        "記名-教育部學校合作","記名-其他合作","記名-NFC不含聯名",
        "記名-台灣大哥大","記名-遠傳電信","記名-亞太電信","記名-其他電信",
        "記名-優待卡","記名-年滿65歲","記名-身心障礙",
        "附隨電支帳戶儲值卡","無記名","拋棄式儲值卡",
        "記名-其他16","記名-其他17",
    ]
    L["EC011W"] = {}
    for i, ct in enumerate(_card_types):
        L["EC011W"].update({
            f"_{i}__card":    f"{ct}-流通卡數",
            f"_{i}__card2":   f"{ct}-當月新增卡數",
            f"_{i}__txn_qty": f"{ct}-交易筆數",
            f"_{i}__txn_amt": f"{ct}-交易金額（元）",
            f"_{i}__remark":  f"{ct}-備註",
        })

    # ── EP005B 電支帳戶戶數及使用者人數 ──────────────────────
    L["EP005B"] = {
        "_0__amt": "第一類電支帳戶戶數",
        "_1__amt": "符合第二類認證程序戶數",
        "_2__amt": "符合第三類認證程序戶數",
        "_3__amt": "第二類電支帳戶戶數",
        "_4__amt": "第三類電支帳戶戶數",
        "c_3000A": "電支帳戶戶數總計",
        "c_1900A": "含境外業務帳戶小計",
        "c_2900A": "不含境外業務帳戶小計",
        "_5__amt": "電支帳戶使用者人數",
        "_6__amt": "可從事境外交易之使用者人數",
    }

    # ── EP005W 電支帳戶使用者別交易
    # 區塊1：本業使用者（不含境外合作）_0-_4900
    # 區塊2：所有使用者（含境外合作客戶）_27-_8900
    _M = ["使用者人數", "交易筆數", "交易金額（元）"]
    _本國 = ["本國籍-個人", "本國籍-非個人-政府機關", "本國籍-非個人-法人",
              "本國籍-非個人-行號", "本國籍-非個人-其他團體"]
    L5w = {}
    # 區塊1 本國籍 _0-_14
    for i, t in enumerate(_本國):
        for j, m in enumerate(_M):
            L5w[f"_{i*3+j}__amt"] = f"本業-{t}-{m}"
    L5w.update({
        "_1900A":"本業-本國籍小計(A)-使用者人數",
        "_1900B":"本業-本國籍小計(A)-交易筆數",
        "_1900C":"本業-本國籍小計(A)-交易金額（元）",
    })
    # 區塊1 外國籍大陸地區 _15-_20
    for i, t in enumerate(["外國籍-大陸地區-個人", "外國籍-大陸地區-非個人（限法人）"]):
        for j, m in enumerate(_M):
            L5w[f"_{15+i*3+j}__amt"] = f"本業-{t}-{m}"
    L5w.update({
        "_2900A":"本業-大陸地區小計(B)-使用者人數",
        "_2900B":"本業-大陸地區小計(B)-交易筆數",
        "_2900C":"本業-大陸地區小計(B)-交易金額（元）",
    })
    # 區塊1 外國籍大陸以外 _21-_26
    for i, t in enumerate(["外國籍-大陸以外-個人", "外國籍-大陸以外-非個人（限法人）"]):
        for j, m in enumerate(_M):
            L5w[f"_{21+i*3+j}__amt"] = f"本業-{t}-{m}"
    L5w.update({
        "_3900A":"本業-大陸以外小計(C)-使用者人數",
        "_3900B":"本業-大陸以外小計(C)-交易筆數",
        "_3900C":"本業-大陸以外小計(C)-交易金額（元）",
        "_4900A":"本業使用者總計(D)-使用者人數",
        "_4900B":"本業使用者總計(D)-交易筆數",
        "_4900C":"本業使用者總計(D)-交易金額（元）",
    })
    # 區塊2 含境外合作客戶
    for i, t in enumerate(_本國):
        for j, m in enumerate(_M):
            L5w[f"_{27+i*3+j}__amt"] = f"含境外-{t}-{m}"
    L5w.update({
        "_5900A":"含境外-本國籍小計(a)-使用者人數",
        "_5900B":"含境外-本國籍小計(a)-交易筆數",
        "_5900C":"含境外-本國籍小計(a)-交易金額（元）",
    })
    for i, t in enumerate(["外國籍-大陸地區-個人", "外國籍-大陸地區-非個人（限法人）"]):
        for j, m in enumerate(_M):
            L5w[f"_{42+i*3+j}__amt"] = f"含境外-{t}-{m}"
    L5w.update({
        "_6900A":"含境外-大陸地區小計(b)-使用者人數",
        "_6900B":"含境外-大陸地區小計(b)-交易筆數",
        "_6900C":"含境外-大陸地區小計(b)-交易金額（元）",
    })
    for i, t in enumerate(["外國籍-大陸以外-個人", "外國籍-大陸以外-非個人（限法人）"]):
        for j, m in enumerate(_M):
            L5w[f"_{48+i*3+j}__amt"] = f"含境外-{t}-{m}"
    L5w.update({
        "_7900A":"含境外-大陸以外小計(c)-使用者人數",
        "_7900B":"含境外-大陸以外小計(c)-交易筆數",
        "_7900C":"含境外-大陸以外小計(c)-交易金額（元）",
        "_8900A":"所有使用者總計(d)-使用者人數",
        "_8900B":"所有使用者總計(d)-交易筆數",
        "_8900C":"所有使用者總計(d)-交易金額（元）",
    })
    L["EP005W"] = L5w

    # ── EP006B 業務帳戶別交易（帳戶類別×業務別×筆數/金額）──
    _acct_types = ["第一類電支帳戶","第二類電支帳戶","第三類電支帳戶"]
    _biz_6b = ["代理收付款項_收受儲值_帳戶間移轉業務","與境外合作或協助相關業務"]
    L["EP006B"] = {
        "_0__amt": "含境外-第一類電支帳戶-筆數",
        "_1__amt": "含境外-第一類電支帳戶-金額",
        "_2__amt": "含境外-第二類電支帳戶-筆數",
        "_3__amt": "含境外-第二類電支帳戶-金額",
        "_4__amt": "含境外-第三類電支帳戶-筆數",
        "_5__amt": "含境外-第三類電支帳戶-金額",
        "_3000A":  "含境外-帳戶別交易筆數總計",
        "_3000B":  "含境外-帳戶別交易金額總計",
        "_1900A":  "含境外-本業帳戶筆數小計",
        "_1900B":  "含境外-本業帳戶金額小計",
        "_6__amt": "境外合作-第一類電支帳戶-筆數",
        "_7__amt": "境外合作-第一類電支帳戶-金額",
        "_8__amt": "境外合作-第二類電支帳戶-筆數",
        "_9__amt": "境外合作-第二類電支帳戶-金額",
        "_10__amt":"境外合作-第三類電支帳戶-筆數",
        "_11__amt":"境外合作-第三類電支帳戶-金額",
        "_4900A":  "含境外-境外合作帳戶筆數小計",
        "_4900B":  "含境外-境外合作帳戶金額小計",
        "_2900A":  "境外合作-帳戶別交易筆數總計",
        "_2900B":  "境外合作-帳戶別交易金額總計",
    }

    # ── EP006W 業務別交易（交易類型×支付工具×筆數/金額）────
    _tools_6w   = ["信用卡","約定連結存款帳戶","非約定連結存款帳戶","委外代收"]
    _tools_6w_sv= ["信用卡","約定連結存款帳戶","委外代收"]
    L["EP006W"] = {}
    for i, t in enumerate(_tools_6w):
        L["EP006W"][f"_{i*2}__amt"]   = f"代理收付-{t}-筆數"
        L["EP006W"][f"_{i*2+1}__amt"] = f"代理收付-{t}-金額（元）"
    L["EP006W"].update({
        "c_1100X":"代理收付-小計-筆數", "c_1100Y":"代理收付-小計-金額（元）"})
    for i, t in enumerate(_tools_6w_sv):
        L["EP006W"][f"_{8+i*2}__amt"]   = f"收受儲值-{t}-筆數"
        L["EP006W"][f"_{9+i*2}__amt"]   = f"收受儲值-{t}-金額（元）"
    L["EP006W"].update({
        "c_1200X":"收受儲值-小計-筆數", "c_1200Y":"收受儲值-小計-金額（元）",
        "_14__amt":"國內小額匯兌-電支帳戶餘額-筆數",
        "_15__amt":"國內小額匯兌-電支帳戶餘額-金額（元）",
        "_16__amt":"國內小額匯兌-帳戶間移轉-筆數",
        "c_1510X":"國內小額匯兌-小計-筆數", "c_1510Y":"國內小額匯兌-小計-金額（元）",
        "_17__amt":"跨機構小額匯兌-電支帳戶餘額-筆數",
        "_18__amt":"跨機構小額匯兌-電支帳戶餘額-金額（元）",
        "_19__amt":"跨機構小額匯兌-帳戶間移轉-筆數",
        "c_1520X":"跨機構小額匯兌-小計-筆數", "c_1520Y":"跨機構小額匯兌-小計-金額（元）",
        "_20__amt":"國外小額匯兌-帳戶間移轉匯出-筆數",
        "_21__amt":"國外小額匯兌-帳戶間移轉匯出-金額（元）",
        "_22__amt":"國外小額匯兌-匯入-筆數",
        "c_1610X":"國外小額匯兌匯出-小計-筆數", "c_1610Y":"國外小額匯兌匯出-小計-金額（元）",
        "_23__amt":"國外小額匯兌匯入-帳戶間移轉-筆數",
        "_24__amt":"國外小額匯兌匯入-帳戶間移轉-金額（元）",
        "_25__amt":"國外小額匯兌匯入-其他-筆數",
        "c_1620X":"國外小額匯兌匯入-小計-筆數", "c_1620Y":"國外小額匯兌匯入-小計-金額（元）",
        "_26__amt":"境外代理收付-其他-筆數",
        "c_1400A":"境外代理收付-小計-筆數",
        "c_1400B":"境外代理收付-小計-金額（元）",
        "c_1400C":"境外代理收付-小計-其他",
        "c_1999A":"業務合計-代理收付-筆數",
        "c_1999B":"業務合計-代理收付-金額（元）",
        "c_1999C":"業務合計-收受儲值-筆數",
        "c_1999D":"業務合計-收受儲值-金額（元）",
        "c_1999E":"業務合計-國內小額匯兌-筆數",
        "c_1999F":"業務合計-國內小額匯兌-金額（元）",
        "c_1999G":"業務合計-境外合作-筆數",
        "c_1999H":"業務合計-境外合作-金額（元）",
        "c_1999X":"業務交易總筆數", "c_1999Y":"業務交易總金額（元）",
    })

    # ── EP007W 電支帳戶支付工具別交易（3業務×7工具×筆數/金額）
    _tools_7w = ["信用卡","約定連結存款帳戶","非約定連結存款帳戶",
                 "委外代收","電支帳戶餘額","儲值卡","其他"]
    _tools_7w_sv = ["信用卡","約定連結存款帳戶","非約定連結存款帳戶",
                    "委外代收","代理收付餘額轉儲值","儲值卡","其他"]
    L["EP007W"] = {}
    for i, t in enumerate(_tools_7w):
        L["EP007W"][f"_{i*2}__amt"]    = f"代理收付-{t}-筆數"
        L["EP007W"][f"_{i*2+1}__amt"]  = f"代理收付-{t}-金額（元）"
    L["EP007W"].update({"_1999A":"代理收付-小計-筆數","_1999B":"代理收付-小計-金額（元）"})
    for i, t in enumerate(_tools_7w_sv):
        L["EP007W"][f"_{14+i*2}__amt"] = f"收受儲值-{t}-筆數"
        L["EP007W"][f"_{15+i*2}__amt"] = f"收受儲值-{t}-金額（元）"
    L["EP007W"].update({"_2999A":"收受儲值-小計-筆數","_2999B":"收受儲值-小計-金額（元）"})
    for i, t in enumerate(_tools_7w):
        L["EP007W"][f"_{28+i*2}__amt"] = f"小額匯兌-{t}-筆數"
        L["EP007W"][f"_{29+i*2}__amt"] = f"小額匯兌-{t}-金額（元）"
    L["EP007W"].update({
        "_3999A":"境外代理收付-小計-筆數","_3999B":"境外代理收付-小計-金額（元）",
        "_4999A":"小額匯兌-小計-筆數",   "_4999B":"小額匯兌-小計-金額（元）",
        "_9000A":"三大業務總筆數",        "_9000B":"三大業務總金額（元）",
    })
    # _42-_49 (境外代理收付明細)
    for i, t in enumerate(_tools_7w[:4]):
        L["EP007W"][f"_{42+i*2}__amt"] = f"境外代理收付-{t}-筆數"
        L["EP007W"][f"_{43+i*2}__amt"] = f"境外代理收付-{t}-金額（元）"

    # ── EP007X 儲值卡支付工具別交易（5工具×筆數/金額/卡數）──
    _tools_7x = ["信用卡","存款帳戶","委外代收（如便利商店）","電支帳戶餘額","其他"]
    L["EP007X"] = {}
    for i, t in enumerate(_tools_7x):
        L["EP007X"][f"_{i*3}__amt"]   = f"收受儲值-{t}-筆數"
        L["EP007X"][f"_{i*3+1}__amt"] = f"收受儲值-{t}-金額（元）"
        L["EP007X"][f"_{i*3+2}__amt"] = f"收受儲值-{t}-卡數"
    L["EP007X"].update({
        "_1999A":"收受儲值-小計-總筆數",
        "_1999B":"收受儲值-小計-總金額（元）",
        "_1999C":"收受儲值-小計-總卡數",
    })

    # ── EP008W 特約機構交易（2區塊×9類型×特約機構數/筆數/金額）
    _cat_8w = ["本國籍個人","本國籍政府機關","本國籍法人","本國籍行號","本國籍其他團體",
               "外籍大陸個人","外籍大陸非個人","外籍大陸以外個人","外籍大陸以外非個人"]
    _vals_8w = ["特約機構數","收款筆數","收款金額（元）"]
    L["EP008W"] = {}
    for i, cat in enumerate(_cat_8w):
        for j, val in enumerate(_vals_8w):
            L["EP008W"][f"_{i*3+j}__amt"]    = f"含境外-{cat}-{val}"
            L["EP008W"][f"_{27+i*3+j}__amt"] = f"不含境外-{cat}-{val}"
    L["EP008W"].update({
        "c_3000A":"含境外-總計-特約機構數",
        "c_3000B":"含境外-總計-收款筆數",
        "c_3000C":"含境外-總計-收款金額（元）",
        "c_6000A":"不含境外-總計-特約機構數",
        "c_6000B":"不含境外-總計-收款筆數",
        "c_6000C":"不含境外-總計-收款金額（元）",
    })

    # ── EP010W 實體通路支付服務（核准業務別×筆數/金額）────────
    _biz_10w = ["代理收付實質交易款項","收受儲值款項","電支帳戶間款項移轉"]
    L["EP010W"] = {}
    for i, b in enumerate(_biz_10w):
        L["EP010W"][f"_{i*2}__amt"]   = f"{b}-筆數"
        L["EP010W"][f"_{i*2+1}__amt"] = f"{b}-金額（元）"
    L["EP010W"].update({
        "_1900A":"業務小計-筆數", "_1900B":"業務小計-金額（元）",
    })

    # ── EP010X 代理收付通路別（非實體/實體×電支帳戶/儲值卡×筆數/金額+據點數）
    L["EP010X"] = {
        "_0__amt":"非實體通路-電支帳戶-筆數",
        "_1__amt":"非實體通路-電支帳戶-金額（元）",
        "_2__amt":"非實體通路-儲值卡-筆數",
        "_3__amt":"非實體通路-儲值卡-金額（元）",
        "_4__amt":"非實體通路-電支帳戶通路據點數",
        "_5__amt":"實體通路-電支帳戶-筆數",
        "_6__amt":"實體通路-電支帳戶-金額（元）",
        "_7__amt":"實體通路-儲值卡-筆數",
        "_8__amt":"實體通路-儲值卡-金額（元）",
        "_9__amt":"實體通路-電支帳戶通路據點數",
    }

    # ── EP014W 收受使用者支付款項餘額 ────────────────────────
    L["EP014W"] = {
        "_0__amt": "電支帳戶-代理收付款項餘額（不含跨境）",
        "_1__amt": "電支帳戶-跨境代理收付款項餘額",
        "c_1900A": "電支帳戶-代理收付小計A",
        "_2__amt": "電支帳戶-儲值款項餘額B",
        "c_3000A": "電支帳戶-支付款項餘額C＝A＋B",
        "_3__amt": "儲值卡-代理收付款項餘額（不含跨境）",
        "_4__amt": "儲值卡-跨境代理收付款項餘額",
        "c_4900A": "儲值卡-代理收付小計D",
        "_5__amt": "儲值卡-儲值款項餘額E",
        "c_6000A": "儲值卡-支付款項餘額F＝D＋E",
        "c_9000A": "支付款項餘額總計C＋F",
    }

    # ── EP015W 申訴案件（新增/結案×業務別×申訴類型）──────────
    _cmp_types = ["註冊開戶","儲值卡購買","手續費","使用問題",
                  "款項退還","款項提領","偽冒交易","其他"]
    _case_types = ["本月新增","本月結案"]
    _biz_15 = ["代理收付收受儲值帳戶間移轉業務","境外合作業務"]
    L["EP015W"] = {}
    # Block1 (前24個): 新增案件×業務別×申訴類型
    for i, ct in enumerate(_cmp_types):
        L["EP015W"][f"_{i*3}__amt"]   = f"新增-代理收付業務-{ct}"
        L["EP015W"][f"_{i*3+1}__amt"] = f"新增-第二類電支業務-{ct}"
        L["EP015W"][f"_{i*3+2}__amt"] = f"新增-第三類電支業務-{ct}"
    L["EP015W"].update({
        "_1900A":"新增-代理收付業務-小計",
        "_1900B":"新增-第二類電支業務-小計",
        "_1900C":"新增-第三類電支業務-小計",
    })
    # Block2 (結案 24個)
    for i, ct in enumerate(_cmp_types):
        L["EP015W"][f"_{24+i*3}__amt"] = f"結案-代理收付業務-{ct}"
        L["EP015W"][f"_{25+i*3}__amt"] = f"結案-第二類電支業務-{ct}"
        L["EP015W"][f"_{26+i*3}__amt"] = f"結案-第三類電支業務-{ct}"
    L["EP015W"].update({
        "_2900A":"結案-代理收付業務-小計",
        "_2900B":"結案-第二類電支業務-小計",
        "_2900C":"結案-第三類電支業務-小計",
        "_3000A":"全期-代理收付業務-案件總計",
        "_3000B":"全期-第二類電支業務-案件總計",
        "_3000C":"全期-第三類電支業務-案件總計",
    })

    # ── WB041W 行動支付業務 ───────────────────────────────────
    L["WB041W"] = {
        "cards":"發卡數", "xact":"交易筆數", "amt":"交易金額（元）",
        "bank_no":"機構代碼",
        "_0__cards":"NFC預先加載-台灣大哥大-發卡數",
        "_0__xact": "NFC預先加載-台灣大哥大-交易筆數",
        "_0__amt":  "NFC預先加載-台灣大哥大-金額",
        "_1__cards":"NFC空中下載-中華電信-發卡數",
        "_1__xact": "NFC空中下載-中華電信-交易筆數",
        "_1__amt":  "NFC空中下載-中華電信-金額",
        "_2__cards":"NFC空中下載-台灣大哥大-發卡數",
        "_2__xact": "NFC空中下載-台灣大哥大-交易筆數",
        "_2__amt":  "NFC空中下載-台灣大哥大-金額",
        "_3__cards":"NFC空中下載-遠傳電信-發卡數",
        "_3__xact": "NFC空中下載-遠傳電信-交易筆數",
        "_3__amt":  "NFC空中下載-遠傳電信-金額",
        "_4__cards":"NFC空中下載-亞太電信-發卡數",
        "_4__xact": "NFC空中下載-亞太電信-交易筆數",
        "_4__amt":  "NFC空中下載-亞太電信-金額",
        "_5__cards":"NFC聯名-中華電信×中信銀行-發卡數",
        "_5__xact": "NFC聯名-中華電信×中信銀行-交易筆數",
        "_5__amt":  "NFC聯名-中華電信×中信銀行-金額",
        "_6__cards":"NFC聯名-中華電信×聯邦銀行-發卡數",
        "_6__xact": "NFC聯名-中華電信×聯邦銀行-交易筆數",
        "_6__amt":  "NFC聯名-中華電信×聯邦銀行-金額",
    }
    return L

FULL_COL_LABELS = _make_labels()

# 向後相容舊名稱（Dashboard KPI 使用）
TABLE_COL_LABELS = FULL_COL_LABELS

# helper
def get_col_label(table: str, col: str) -> str:
    return FULL_COL_LABELS.get(table, {}).get(col, col)

# ── 各表層次結構定義（用於 Browse 多層次表頭） ──────────────────
def _make_structure():
    S = {}

    # 基本 info 欄
    _info = [
        {"col": "yr",     "label": "年份"},
        {"col": "mn",     "label": "月份"},
        {"col": "機構名稱","label": "機構名稱"},
    ]

    # EP007W
    _tools_7w = ["信用卡","約定連結存款","非約定連結存款","委外代收","電支帳戶餘額","儲值卡","其他"]
    _tools_7w_sv = ["信用卡","約定連結存款","非約定連結存款","委外代收","代理收付餘額轉儲值","儲值卡","其他"]
    def mk7w_group(name, base, tools, bg):
        cols = []
        for i, t in enumerate(tools):
            cols += [
                {"col": f"_{base+i*2}__amt",   "label": f"{t}×筆數"},
                {"col": f"_{base+i*2+1}__amt",  "label": f"{t}×金額"},
            ]
        sub_a = "_1999A" if base==0 else ("_2999A" if base==14 else "_4999A")
        sub_b = "_1999B" if base==0 else ("_2999B" if base==14 else "_4999B")
        cols += [{"col": sub_a, "label":"小計×筆數","highlight":True},
                 {"col": sub_b, "label":"小計×金額","highlight":True}]
        return {"name": name, "bg": bg, "cols": cols}

    S["EP007W"] = {
        "static": _info,
        "groups": [
            mk7w_group("代理收付實質交易款項",  0, _tools_7w,    "#dbeafe"),
            mk7w_group("收受儲值款項",          14, _tools_7w_sv, "#dcfce7"),
            mk7w_group("辦理國內外小額匯兌",    28, _tools_7w,    "#fef3c7"),
            {"name":"總計","bg":"#f1f5f9","cols":[
                {"col":"_9000A","label":"總筆數","highlight":True},
                {"col":"_9000B","label":"總金額（元）","highlight":True},
            ]},
        ]
    }

    # EP007X
    _tools_7x = ["信用卡","存款帳戶","委外代收","電支帳戶餘額","其他"]
    S["EP007X"] = {
        "static": _info,
        "groups": [
            {"name":"收受儲值款項","bg":"#dcfce7","cols":
                sum([[
                    {"col":f"_{i*3}__amt",   "label":f"{t}×筆數"},
                    {"col":f"_{i*3+1}__amt", "label":f"{t}×金額（元）"},
                    {"col":f"_{i*3+2}__amt", "label":f"{t}×卡數"},
                ] for i,t in enumerate(_tools_7x)],[]) +
                [{"col":"_1999A","label":"小計×筆數","highlight":True},
                 {"col":"_1999B","label":"小計×金額","highlight":True},
                 {"col":"_1999C","label":"小計×卡數","highlight":True}]
            },
        ]
    }

    # EP008W
    _cat_8w = ["本國籍個人","本國籍政府機關","本國籍法人","本國籍行號","本國籍其他團體",
               "外籍大陸個人","外籍大陸非個人","外籍大陸以外個人","外籍大陸以外非個人"]
    def mk8w_group(name, base, totA, totB, totC, bg):
        cols = []
        for i, cat in enumerate(_cat_8w):
            cols += [
                {"col":f"_{base+i*3}__amt",   "label":f"{cat}×特約機構數"},
                {"col":f"_{base+i*3+1}__amt", "label":f"{cat}×收款筆數"},
                {"col":f"_{base+i*3+2}__amt", "label":f"{cat}×收款金額"},
            ]
        cols += [{"col":totA,"label":"總計×特約機構數","highlight":True},
                 {"col":totB,"label":"總計×收款筆數","highlight":True},
                 {"col":totC,"label":"總計×收款金額（元）","highlight":True}]
        return {"name": name, "bg": bg, "cols": cols}
    S["EP008W"] = {
        "static": _info,
        "groups": [
            mk8w_group("所有特約機構（含境外合作客戶）",
                       0,"c_3000A","c_3000B","c_3000C","#dbeafe"),
            mk8w_group("本業特約機構（不含境外合作客戶）",
                       27,"c_6000A","c_6000B","c_6000C","#dcfce7"),
        ]
    }

    # EP010X
    S["EP010X"] = {
        "static": _info,
        "groups": [
            {"name":"非實體通路","bg":"#dbeafe","cols":[
                {"col":"_0__amt","label":"電支帳戶×筆數"},
                {"col":"_1__amt","label":"電支帳戶×金額（元）"},
                {"col":"_2__amt","label":"儲值卡×筆數"},
                {"col":"_3__amt","label":"儲值卡×金額（元）"},
                {"col":"_4__amt","label":"通路據點數","highlight":True},
            ]},
            {"name":"實體通路","bg":"#dcfce7","cols":[
                {"col":"_5__amt","label":"電支帳戶×筆數"},
                {"col":"_6__amt","label":"電支帳戶×金額（元）"},
                {"col":"_7__amt","label":"儲值卡×筆數"},
                {"col":"_8__amt","label":"儲值卡×金額（元）"},
                {"col":"_9__amt","label":"通路據點數","highlight":True},
            ]},
        ]
    }

    # EP014W
    S["EP014W"] = {
        "static": _info,
        "groups": [
            {"name":"電子支付帳戶","bg":"#dbeafe","cols":[
                {"col":"_0__amt",  "label":"代理收付款項餘額（不含跨境）"},
                {"col":"_1__amt",  "label":"跨境代理收付款項餘額"},
                {"col":"c_1900A",  "label":"代理收付小計A","highlight":True},
                {"col":"_2__amt",  "label":"儲值款項餘額B"},
                {"col":"c_3000A",  "label":"支付款項餘額C＝A＋B","highlight":True},
            ]},
            {"name":"儲值卡","bg":"#dcfce7","cols":[
                {"col":"_3__amt",  "label":"代理收付款項餘額（不含跨境）"},
                {"col":"_4__amt",  "label":"跨境代理收付款項餘額"},
                {"col":"c_4900A",  "label":"代理收付小計D","highlight":True},
                {"col":"_5__amt",  "label":"儲值款項餘額E"},
                {"col":"c_6000A",  "label":"支付款項餘額F＝D＋E","highlight":True},
            ]},
            {"name":"合計","bg":"#f1f5f9","cols":[
                {"col":"c_9000A",  "label":"支付款項餘額總計C＋F","highlight":True},
            ]},
        ]
    }

    # EP005B
    S["EP005B"] = {
        "static": _info,
        "groups": [
            {"name":"電子支付帳戶戶數","bg":"#dbeafe","cols":[
                {"col":"_0__amt","label":"第一類電支帳戶"},
                {"col":"_1__amt","label":"符合第二類認證"},
                {"col":"_2__amt","label":"符合第三類認證"},
                {"col":"_3__amt","label":"第二類電支帳戶"},
                {"col":"_4__amt","label":"第三類電支帳戶"},
                {"col":"c_3000A","label":"帳戶戶數總計","highlight":True},
            ]},
            {"name":"使用者人數","bg":"#dcfce7","cols":[
                {"col":"_5__amt","label":"使用者人數"},
                {"col":"_6__amt","label":"可從事境外交易人數"},
            ]},
        ]
    }

    # EP005W 電支帳戶使用者別交易
    def mk5w_block(prefix, base_main, base_dl, base_dlout, sub_a, sub_b, sub_c, tot, bg1, bg2, bg3, bg4):
        """建立一個區塊（本業 or 含境外）的六個群組"""
        _本國 = ["個人", "非個人-政府機關", "非個人-法人", "非個人-行號", "非個人-其他團體"]
        _M = [("人數","使用者人數"), ("筆數","交易筆數"), ("金額","交易金額（元）")]
        # 本國籍群組
        cols_tw = []
        for i, t in enumerate(_本國):
            for suf, lbl in _M:
                cols_tw.append({"col":f"_{base_main+i*3+_M.index((suf,lbl))}__amt","label":f"本國籍-{t}×{suf}"})
        cols_tw += [
            {"col":sub_a+"A","label":"本國籍小計×人數","highlight":True},
            {"col":sub_a+"B","label":"本國籍小計×筆數","highlight":True},
            {"col":sub_a+"C","label":"本國籍小計×金額","highlight":True},
        ]
        # 外國籍大陸地區群組
        cols_cn = []
        for i, t in enumerate(["大陸地區-個人","大陸地區-非個人（限法人）"]):
            for _, lbl in _M:
                cols_cn.append({"col":f"_{base_dl+i*3+[l for _,l in _M].index(lbl)}__amt","label":f"{t}×{lbl.replace('（元）','')}"})
        cols_cn += [
            {"col":sub_b+"A","label":"大陸地區小計×人數","highlight":True},
            {"col":sub_b+"B","label":"大陸地區小計×筆數","highlight":True},
            {"col":sub_b+"C","label":"大陸地區小計×金額","highlight":True},
        ]
        # 外國籍大陸以外群組
        cols_ot = []
        for i, t in enumerate(["大陸以外-個人","大陸以外-非個人（限法人）"]):
            for _, lbl in _M:
                cols_ot.append({"col":f"_{base_dlout+i*3+[l for _,l in _M].index(lbl)}__amt","label":f"{t}×{lbl.replace('（元）','')}"})
        cols_ot += [
            {"col":sub_c+"A","label":"大陸以外小計×人數","highlight":True},
            {"col":sub_c+"B","label":"大陸以外小計×筆數","highlight":True},
            {"col":sub_c+"C","label":"大陸以外小計×金額","highlight":True},
        ]
        # 總計群組
        cols_tot = [
            {"col":tot+"A","label":f"{prefix}總計×使用者人數","highlight":True},
            {"col":tot+"B","label":f"{prefix}總計×交易筆數","highlight":True},
            {"col":tot+"C","label":f"{prefix}總計×交易金額","highlight":True},
        ]
        return [
            {"name":f"{prefix}本國籍使用者","bg":bg1,"cols":cols_tw},
            {"name":f"{prefix}外國籍-大陸地區","bg":bg2,"cols":cols_cn},
            {"name":f"{prefix}外國籍-大陸以外","bg":bg3,"cols":cols_ot},
            {"name":f"{prefix}總計","bg":bg4,"cols":cols_tot},
        ]

    S["EP005W"] = {
        "static": _info,
        "groups": (
            mk5w_block("本業（不含境外合作）-", 0,  15, 21, "_1900", "_2900", "_3900", "_4900",
                       "#dbeafe","#dcfce7","#fef3c7","#f1f5f9") +
            mk5w_block("含境外合作客戶-",       27, 42, 48, "_5900", "_6900", "_7900", "_8900",
                       "#bfdbfe","#bbf7d0","#fde68a","#e2e8f0")
        )
    }

    # WB041W 行動支付業務
    _nfc_types = [
        ("NFC預先加載-台灣大哥大", 0),
        ("NFC空中下載-中華電信",   1),
        ("NFC空中下載-台灣大哥大", 2),
        ("NFC空中下載-遠傳電信",   3),
        ("NFC空中下載-亞太電信",   4),
        ("NFC聯名-中華電信×中信銀行", 5),
        ("NFC聯名-中華電信×聯邦銀行", 6),
    ]
    _nfc_bg = ["#dbeafe","#dcfce7","#dbeafe","#dcfce7","#dbeafe","#fef3c7","#fef3c7"]
    nfc_groups = []
    for (name, idx), bg in zip(_nfc_types, _nfc_bg):
        nfc_groups.append({"name": name, "bg": bg, "cols": [
            {"col": f"_{idx}__cards", "label": "發卡數"},
            {"col": f"_{idx}__xact",  "label": "交易筆數"},
            {"col": f"_{idx}__amt",   "label": "交易金額（元）"},
        ]})
    S["WB041W"] = {
        "static": [
            {"col": "yr",           "label": "年份"},
            {"col": "mn",           "label": "月份"},
            {"col": "機構名稱",     "label": "機構名稱"},
            {"col": "業務項目_合作對象", "label": "業務項目/合作對象"},
        ],
        "groups": [
            {"name": "合計", "bg": "#f1f5f9", "cols": [
                {"col": "cards", "label": "發卡數",       "highlight": True},
                {"col": "xact",  "label": "交易筆數",     "highlight": True},
                {"col": "amt",   "label": "交易金額（元）","highlight": True},
            ]},
        ] + nfc_groups
    }

    # EP006W
    _tools_6w  = ["信用卡","約定連結存款","非約定連結存款","委外代收"]
    _tools_sv  = ["信用卡","約定連結存款","委外代收"]
    def mk6w_grp(name, pairs, totX, totY, bg):
        cols = []
        for col_n, col_a, lbl in pairs:
            cols.append({"col":col_n,"label":f"{lbl}×筆數"})
            if col_a:
                cols.append({"col":col_a,"label":f"{lbl}×金額"})
        cols += [{"col":totX,"label":"小計×筆數","highlight":True},
                 {"col":totY,"label":"小計×金額","highlight":True}]
        return {"name":name,"bg":bg,"cols":cols}
    S["EP006W"] = {
        "static": _info + [{"col":"交易類型","label":"交易類型"}],
        "groups": [
            mk6w_grp("代理收付",
                     [("_0__amt","_1__amt","信用卡"),("_2__amt","_3__amt","約定存款"),
                      ("_4__amt","_5__amt","非約定存款"),("_6__amt","_7__amt","委外代收")],
                     "c_1100X","c_1100Y","#dbeafe"),
            mk6w_grp("收受儲值",
                     [("_8__amt","_9__amt","信用卡"),("_10__amt","_11__amt","約定存款"),
                      ("_12__amt","_13__amt","委外代收")],
                     "c_1200X","c_1200Y","#dcfce7"),
            {"name":"國內小額匯兌","bg":"#fef3c7","cols":[
                {"col":"_14__amt","label":"電支帳戶餘額×筆數"},
                {"col":"_15__amt","label":"電支帳戶餘額×金額"},
                {"col":"_16__amt","label":"帳戶間移轉×筆數"},
                {"col":"c_1510X","label":"小計×筆數","highlight":True},
                {"col":"c_1510Y","label":"小計×金額","highlight":True},
            ]},
            {"name":"跨機構小額匯兌","bg":"#fce7f3","cols":[
                {"col":"_17__amt","label":"電支帳戶餘額×筆數"},
                {"col":"_18__amt","label":"電支帳戶餘額×金額"},
                {"col":"_19__amt","label":"帳戶間移轉×筆數"},
                {"col":"c_1520X","label":"小計×筆數","highlight":True},
                {"col":"c_1520Y","label":"小計×金額","highlight":True},
            ]},
            {"name":"國外小額匯兌","bg":"#ede9fe","cols":[
                {"col":"_20__amt","label":"匯出×筆數"},{"col":"_21__amt","label":"匯出×金額"},
                {"col":"_22__amt","label":"匯入×筆數"},
                {"col":"c_1610X","label":"匯出小計×筆數","highlight":True},
                {"col":"c_1610Y","label":"匯出小計×金額","highlight":True},
                {"col":"_23__amt","label":"匯入帳戶移轉×筆數"},
                {"col":"_24__amt","label":"匯入帳戶移轉×金額"},
                {"col":"_25__amt","label":"匯入其他×筆數"},
                {"col":"c_1620X","label":"匯入小計×筆數","highlight":True},
                {"col":"c_1620Y","label":"匯入小計×金額","highlight":True},
            ]},
            {"name":"合計","bg":"#f1f5f9","cols":[
                {"col":"c_1999X","label":"總筆數","highlight":True},
                {"col":"c_1999Y","label":"總金額（元）","highlight":True},
            ]},
        ]
    }

    return S

TABLE_STRUCTURE = _make_structure()

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

# ── 儀表板 KPI 定義 ────────────────────────────────────────────
# col: 資料庫欄位名（以實際含有數值的欄位為準，避免使用標籤欄）
# filter: 額外 WHERE 條件（僅在同表有多類型列時使用）
DASHBOARD_KPI = [
    # ── 儲值卡發行 (EC002W) ────────────────────────────────────
    # 每機構每月一卡片名稱一列，各欄直接存數值
    {"table": "EC002W", "group": "儲值卡發行",
     "kpis": [
         {"col": "發卡總數",   "label": "累計發卡數", "unit": "張"},
         {"col": "流通卡數",   "label": "流通卡數",   "unit": "張"},
         {"col": "當月發卡數", "label": "當月發卡數", "unit": "張"},
         {"col": "當月停卡數", "label": "當月停卡數", "unit": "張"},
     ]},

    # ── 電子支付帳戶 (EP005B) ──────────────────────────────────
    # c_3000A = 帳戶戶數總計（數值欄）
    # _5__amt = 使用者人數，_6__amt = 可從事境外交易人數
    {"table": "EP005B", "group": "電支帳戶",
     "kpis": [
         {"col": "c_3000A", "label": "電支帳戶戶數總計", "unit": "戶"},
         {"col": "_5__amt", "label": "使用者人數",       "unit": "人"},
         {"col": "_6__amt", "label": "可從事境外交易人數", "unit": "人"},
     ]},

    # ── 業務別交易 (EP006W) ────────────────────────────────────
    # 每機構每月一列，交易類型='電子支付帳戶'
    # c_1999X = 各支付工具合計筆數，c_1999Y = 各支付工具合計金額
    {"table": "EP006W", "group": "業務別交易",
     "kpis": [
         {"col": "c_1999X", "label": "業務交易總筆數", "unit": "筆",
          "filter": "\"交易類型\" = '電子支付帳戶'"},
         {"col": "c_1999Y", "label": "業務交易總金額", "unit": "元",
          "filter": "\"交易類型\" = '電子支付帳戶'"},
     ]},

    # ── 帳戶別交易 (EP006B) ────────────────────────────────────
    # 類別='筆數' 的列中，_3000A = 帳戶別合計筆數，_3000B = 合計金額
    {"table": "EP006B", "group": "帳戶別交易",
     "kpis": [
         {"col": "_3000A", "label": "帳戶別交易筆數總計", "unit": "筆",
          "filter": "\"類別\" = '筆數'"},
         {"col": "_3000B", "label": "帳戶別交易金額總計", "unit": "元",
          "filter": "\"類別\" = '筆數'"},
     ]},

    # ── 支付工具別交易 (EP007W) ────────────────────────────────
    # _9000A = 三大業務（代理收付+收受儲值+小額匯兌）總筆數
    # _9000B = 三大業務總金額
    {"table": "EP007W", "group": "支付工具別交易",
     "kpis": [
         {"col": "_9000A", "label": "支付工具別交易總筆數", "unit": "筆"},
         {"col": "_9000B", "label": "支付工具別交易總金額", "unit": "元"},
         {"col": "_1999A", "label": "代理收付筆數小計",     "unit": "筆"},
         {"col": "_2999A", "label": "收受儲值筆數小計",     "unit": "筆"},
     ]},

    # ── 特約機構交易 (EP008W) ──────────────────────────────────
    # c_3000A/B/C = 含境外合作客戶之特約機構數/收款筆數/收款金額
    {"table": "EP008W", "group": "特約機構交易",
     "kpis": [
         {"col": "c_3000A", "label": "特約機構數（含境外）", "unit": "家"},
         {"col": "c_3000B", "label": "收款筆數（含境外）",   "unit": "筆"},
         {"col": "c_3000C", "label": "收款金額（含境外）",   "unit": "元"},
     ]},

    # ── 代理收付通路別 (EP010X) ───────────────────────────────
    # 每機構每月一列，_N__amt 直接存各通路數值
    # _1__amt = 非實體通路×電支帳戶金額，_6__amt = 實體通路×電支帳戶金額
    {"table": "EP010X", "group": "代理收付通路別",
     "kpis": [
         {"col": "_1__amt", "label": "非實體通路電支帳戶金額", "unit": "元"},
         {"col": "_6__amt", "label": "實體通路電支帳戶金額",   "unit": "元"},
         {"col": "_8__amt", "label": "實體通路儲值卡金額",     "unit": "元"},
     ]},

    # ── 收受支付款項餘額 (EP014W) ──────────────────────────────
    # c_9000A = 電支帳戶+儲值卡支付款項餘額總計（C+F）
    # c_3000A = 電支帳戶支付款項餘額C（代理收付小計A+儲值B）
    {"table": "EP014W", "group": "支付款項餘額",
     "kpis": [
         {"col": "c_9000A", "label": "支付款項餘額總計",   "unit": "元"},
         {"col": "c_3000A", "label": "電支帳戶支付款項餘額C", "unit": "元"},
         {"col": "c_6000A", "label": "儲值卡支付款項餘額F", "unit": "元"},
     ]},

    # ── 申訴案件 (EP015W) ─────────────────────────────────────
    # 申訴類別='本月新增申訴案件' 的 小計 欄存實際案件數
    {"table": "EP015W", "group": "申訴案件",
     "kpis": [
         {"col": "小計", "label": "本月新增申訴案件", "unit": "件",
          "filter": "\"申訴類別\" = '本月新增申訴案件'"},
     ]},

    # ── 行動支付業務 (WB041W) ─────────────────────────────────
    # cards/xact/amt 為各業務項目的彙總欄（INTEGER 直接存值）
    {"table": "WB041W", "group": "行動支付業務",
     "kpis": [
         {"col": "cards", "label": "發卡數",       "unit": "張"},
         {"col": "xact",  "label": "交易筆數",     "unit": "筆"},
         {"col": "amt",   "label": "交易金額",     "unit": "元"},
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
    """回傳可用於趨勢/比較分析的欄位清單。
    包含：
      1. 中文命名的數值欄
      2. _N__amt / c_XXXXX 等系統數值欄（附帶中文標籤）
    """
    conn = get_conn()
    all_cols = get_columns(conn, code)
    tbl_labels = TABLE_COL_LABELS.get(code, {})
    result = []
    for c in all_cols:
        if c in SKIP_COLS:
            continue
        is_ch = is_chinese_col(c)
        is_dc = is_data_col(c)
        if not is_ch and not is_dc:
            continue
        numeric = is_numeric_col(conn, code, c)
        # 系統數值欄：必須確認有實際數值才加入
        if is_dc and not numeric:
            continue
        # 中文欄：若是純標籤欄（文字型）則跳過（不過濾數值型中文欄）
        label = tbl_labels.get(c, c)
        result.append({"name": c, "label": label, "numeric": numeric})
    conn.close()
    return jsonify(result)


@app.route("/api/table/<code>/structure")
def api_structure(code):
    if code not in TABLE_STRUCTURE:
        return jsonify(None)
    # Filter cols to those that actually exist in the DB
    conn = get_conn()
    actual = set(get_columns(conn, code))
    conn.close()
    struct = TABLE_STRUCTURE[code]
    result = {
        "static": [c for c in struct.get("static", []) if c["col"] in actual],
        "groups": []
    }
    for g in struct.get("groups", []):
        filtered_cols = [c for c in g["cols"] if c["col"] in actual]
        if filtered_cols:
            result["groups"].append({**g, "cols": filtered_cols})
    return jsonify(result)


@app.route("/api/table/<code>/data")
def api_data(code):
    yr    = request.args.get("yr", type=int)
    mn    = request.args.get("mn", type=int)
    yr_to = request.args.get("yr_to", type=int)
    org   = request.args.get("org", "")
    scheme = request.args.get("scheme", "")
    page  = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)
    structured = request.args.get("structured", "0") == "1"

    conn = get_conn()
    cols = get_columns(conn, code)

    # Structured mode: use TABLE_STRUCTURE column order
    if structured and code in TABLE_STRUCTURE:
        struct = TABLE_STRUCTURE[code]
        struct_cols = [c["col"] for c in struct.get("static", [])]
        for g in struct.get("groups", []):
            for c in g["cols"]:
                struct_cols.append(c["col"])
        display_cols = [c for c in struct_cols if c in cols]
    else:
        # 顯示：中文欄 + yr/mn + 該表的摘要系統欄
        summary_cols = TABLE_SUMMARY_COLS.get(code, [])
        display_cols = [c for c in cols
                        if ((is_chinese_col(c) or c in DISPLAY_ANCHOR or c in summary_cols)
                            and c not in SKIP_COLS)]
        # 若篩完為空（全英文欄位表，如 WB056W），fallback 到全欄
        if not display_cols:
            display_cols = [c for c in cols if c not in SKIP_COLS]

    conds, params = [], []
    if yr:      conds.append("yr >= ?");      params.append(yr)
    if yr_to:   conds.append("yr <= ?");      params.append(yr_to)
    if mn:      conds.append("mn = ?");       params.append(mn)
    if org and "機構名稱" in cols:
        conds.append('機構名稱 = ?');           params.append(org)
    if scheme and "方案名稱" in cols:
        conds.append('方案名稱 = ?');           params.append(scheme)

    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    total = conn.execute(f'SELECT COUNT(*) FROM "{code}" {where}', params).fetchone()[0]

    col_sql = ", ".join(f'"{c}"' for c in display_cols)
    order_by = "ORDER BY yr DESC, mn DESC" if "yr" in cols else "ORDER BY rowid DESC"
    rows = conn.execute(
        f'SELECT {col_sql} FROM "{code}" {where} {order_by} '
        f'LIMIT {per_page} OFFSET {(page-1)*per_page}',
        params
    ).fetchall()

    # Distinct orgs
    orgs = []
    if "機構名稱" in cols:
        orgs = [r[0] for r in conn.execute(
            f'SELECT DISTINCT 機構名稱 FROM "{code}" ORDER BY 機構名稱'
        ).fetchall()]
    # Distinct schemes
    schemes = []
    if "方案名稱" in cols:
        schemes = [r[0] for r in conn.execute(
            f'SELECT DISTINCT 方案名稱 FROM "{code}" WHERE 方案名稱 IS NOT NULL ORDER BY 方案名稱'
        ).fetchall()]

    # 為系統欄位附上標籤（前端用於表頭顯示）
    tbl_labels = TABLE_COL_LABELS.get(code, {})
    _sys_labels = {"yr":"年份","mn":"月份","ym":"年月","sn":"序號","bank_no":"機構代碼"}
    col_labels = [tbl_labels.get(c, _sys_labels.get(c, c)) for c in display_cols]

    conn.close()
    return jsonify({
        "columns":    display_cols,
        "col_labels": col_labels,
        "rows":       [list(r) for r in rows],
        "total":      total,
        "page":       page,
        "per_page":   per_page,
        "orgs":       orgs,
        "schemes":    schemes
    })


@app.route("/api/table/<code>/compare")
def api_compare(code):
    yr1 = request.args.get("yr1", type=int)
    mn1 = request.args.get("mn1", type=int)
    yr2 = request.args.get("yr2", type=int)
    mn2 = request.args.get("mn2", type=int)
    org    = request.args.get("org", "")
    scheme = request.args.get("scheme", "")

    if not all([yr1, mn1, yr2, mn2]):
        return jsonify({"error": "缺少參數"}), 400

    conn = get_conn()
    cols = get_columns(conn, code)
    skip = SKIP_COLS | {"yr", "mn", "ym", "sn", "cardname", "editable"}
    # 取中文數值欄 + 系統數值欄（_N__amt / c_XXXXX / cards/xact/amt）
    numeric_cols = [c for c in cols
                    if c not in skip
                    and (is_chinese_col(c) or is_data_col(c))
                    and c not in TEXT_CHINESE_COLS
                    and is_numeric_col(conn, code, c)]

    conds_extra, params_extra = [], []
    if org and "機構名稱" in cols:
        conds_extra.append("機構名稱 = ?");  params_extra.append(org)
    if scheme and "方案名稱" in cols:
        conds_extra.append("方案名稱 = ?");  params_extra.append(scheme)
    cond_extra = ("AND " + " AND ".join(conds_extra)) if conds_extra else ""

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

    tbl_labels = TABLE_COL_LABELS.get(code, {})
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
            "col":   col,
            "label": tbl_labels.get(col, col),
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
    col    = request.args.get("col", "")
    org    = request.args.get("org", "")
    scheme = request.args.get("scheme", "")

    conn = get_conn()
    cols = get_columns(conn, code)
    if col not in cols or "yr" not in cols:
        conn.close()
        return jsonify({"error": "欄位不存在"}), 400

    conds_extra, params_extra = [], []
    if org and "機構名稱" in cols:
        conds_extra.append("機構名稱 = ?");  params_extra.append(org)
    if scheme and "方案名稱" in cols:
        conds_extra.append("方案名稱 = ?");  params_extra.append(scheme)
    cond_extra = ("AND " + " AND ".join(conds_extra)) if conds_extra else ""

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
    yr     = request.args.get("yr", type=int)
    mn     = request.args.get("mn", type=int)
    yr_to  = request.args.get("yr_to", type=int)
    org    = request.args.get("org", "")
    scheme = request.args.get("scheme", "")

    conn = get_conn()
    cols = get_columns(conn, code)
    display_cols = [c for c in cols if c not in SKIP_COLS]

    conds, params = [], []
    if yr:      conds.append("yr >= ?");  params.append(yr)
    if yr_to:   conds.append("yr <= ?");  params.append(yr_to)
    if mn:      conds.append("mn = ?");   params.append(mn)
    if org and "機構名稱" in cols:
        conds.append('機構名稱 = ?');       params.append(org)
    if scheme and "方案名稱" in cols:
        conds.append('方案名稱 = ?');       params.append(scheme)

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


# ══ 全業者統計 API ═══════════════════════════════════════════

@app.route("/api/industry/periods")
def api_industry_periods():
    """回傳所有年月清單"""
    conn = get_conn()
    rows = conn.execute('SELECT DISTINCT yr, mn FROM "全業者統計" ORDER BY yr, mn').fetchall()
    conn.close()
    return jsonify([{"yr": r[0], "mn": r[1], "ym": f"{r[0]}/{r[1]:02d}"} for r in rows])

@app.route("/api/industry/institutions")
def api_industry_institutions():
    """回傳所有機構名稱"""
    conn = get_conn()
    rows = conn.execute('SELECT DISTINCT 機構名稱 FROM "全業者統計" ORDER BY 機構名稱').fetchall()
    conn.close()
    return jsonify([r[0] for r in rows])

@app.route("/api/industry/trend")
def api_industry_trend():
    """
    回傳歷史趨勢：各年月的加總數字
    ?metric=使用者人數|代理收付金額_千元|移轉匯兌金額_千元|收受儲值金額_千元|各類餘額合計_千元
    ?inst=機構名稱 (optional, empty=全部)
    ?欄位說明=帳戶間款項移轉|國內外小額匯兌 (optional)
    """
    metric = request.args.get("metric", "使用者人數")
    inst   = request.args.get("inst", "")
    desc   = request.args.get("欄位說明", "")

    allowed = {"使用者人數","代理收付金額_千元","移轉匯兌金額_千元","收受儲值金額_千元","儲值餘額_千元","代理收付餘額_千元","各類餘額合計_千元"}
    if metric not in allowed:
        return jsonify({"error": "invalid metric"}), 400

    conn = get_conn()
    where_parts = []
    params = []
    if inst:
        where_parts.append('機構名稱 = ?')
        params.append(inst)
    if desc:
        where_parts.append('"欄位說明" = ?')
        params.append(desc)
    where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    rows = conn.execute(
        f'SELECT ym, yr, mn, SUM(CAST("{metric}" AS REAL)) '
        f'FROM "全業者統計" {where_sql} '
        f'GROUP BY yr, mn ORDER BY yr, mn',
        params
    ).fetchall()
    conn.close()
    return jsonify([{"ym": r[0], "yr": r[1], "mn": r[2], "value": r[3]} for r in rows])

@app.route("/api/industry/latest")
def api_industry_latest():
    """最新一期各機構數據"""
    conn = get_conn()
    latest = conn.execute('SELECT yr, mn FROM "全業者統計" ORDER BY yr DESC, mn DESC LIMIT 1').fetchone()
    if not latest:
        conn.close()
        return jsonify([])
    yr, mn = latest
    rows = conn.execute(
        'SELECT 機構名稱, 使用者人數, 代理收付金額_千元, 移轉匯兌金額_千元, '
        '欄位說明, 收受儲值金額_千元, 儲值餘額_千元, 代理收付餘額_千元, 各類餘額合計_千元 '
        'FROM "全業者統計" WHERE yr=? AND mn=? ORDER BY 各類餘額合計_千元 DESC',
        [yr, mn]
    ).fetchall()
    conn.close()
    cols = ["機構名稱","使用者人數","代理收付金額_千元","移轉匯兌金額_千元","欄位說明",
            "收受儲值金額_千元","儲值餘額_千元","代理收付餘額_千元","各類餘額合計_千元"]
    return jsonify({
        "ym": f"{yr}/{mn:02d}",
        "data": [dict(zip(cols, r)) for r in rows]
    })

@app.route("/api/industry/by_inst")
def api_industry_by_inst():
    """單一機構歷史數據（含欄位說明拆分）"""
    inst = request.args.get("inst", "")
    if not inst:
        return jsonify({"error": "inst required"}), 400
    conn = get_conn()
    rows = conn.execute(
        'SELECT ym, yr, mn, 使用者人數, 代理收付金額_千元, 移轉匯兌金額_千元, '
        '欄位說明, 收受儲值金額_千元, 儲值餘額_千元, 代理收付餘額_千元, 各類餘額合計_千元 '
        'FROM "全業者統計" WHERE 機構名稱=? ORDER BY yr, mn, 欄位說明',
        [inst]
    ).fetchall()
    conn.close()
    cols = ["ym","yr","mn","使用者人數","代理收付金額_千元","移轉匯兌金額_千元",
            "欄位說明","收受儲值金額_千元","儲值餘額_千元","代理收付餘額_千元","各類餘額合計_千元"]
    return jsonify([dict(zip(cols, r)) for r in rows])


# ══════════════════════════════════════════════════════════
# 月票交易統計 API
# ══════════════════════════════════════════════════════════

@app.route("/api/monthly_pass/periods")
def mp_periods():
    conn = get_conn()
    if not _table_exists(conn, "月票交易統計"):
        conn.close()
        return jsonify([])
    rows = conn.execute(
        'SELECT DISTINCT yr, mn, ym FROM "月票交易統計" ORDER BY yr, mn'
    ).fetchall()
    conn.close()
    return jsonify([{"yr": r[0], "mn": r[1], "ym": r[2]} for r in rows])

@app.route("/api/monthly_pass/schemes")
def mp_schemes():
    """回傳所有方案（代碼＋名稱去重）"""
    conn = get_conn()
    if not _table_exists(conn, "月票交易統計"):
        conn.close()
        return jsonify([])
    rows = conn.execute(
        'SELECT DISTINCT 方案代碼, 方案名稱 FROM "月票交易統計" ORDER BY 方案代碼'
    ).fetchall()
    conn.close()
    return jsonify([{"code": r[0], "name": r[1]} for r in rows])

@app.route("/api/monthly_pass/by_system")
def mp_by_system():
    """各月份 × 體系別（SVC/QR）加總"""
    conn = get_conn()
    if not _table_exists(conn, "月票交易統計"):
        conn.close()
        return jsonify([])
    rows = conn.execute('''
        SELECT ym, yr, mn, 體系別,
               SUM(交易筆數) AS 交易筆數,
               SUM(交易金額) AS 交易金額
        FROM "月票交易統計"
        GROUP BY yr, mn, 體系別
        ORDER BY yr, mn, 體系別
    ''').fetchall()
    conn.close()
    return jsonify([{
        "ym": r[0], "yr": r[1], "mn": r[2],
        "體系別": r[3], "交易筆數": r[4], "交易金額": r[5]
    } for r in rows])

@app.route("/api/monthly_pass/by_scheme")
def mp_by_scheme():
    """各月份 × 方案加總（可篩月份）"""
    ym = request.args.get("ym", "")
    conn = get_conn()
    if not _table_exists(conn, "月票交易統計"):
        conn.close()
        return jsonify([])
    where = 'WHERE ym=?' if ym else ''
    params = [ym] if ym else []
    rows = conn.execute(f'''
        SELECT ym, 方案代碼, 方案名稱, 體系別,
               SUM(交易筆數) AS 交易筆數,
               SUM(交易金額) AS 交易金額
        FROM "月票交易統計"
        {where}
        GROUP BY ym, 方案代碼, 方案名稱, 體系別
        ORDER BY ym, 方案代碼, 體系別
    ''', params).fetchall()
    conn.close()
    return jsonify([{
        "ym": r[0], "方案代碼": r[1], "方案名稱": r[2],
        "體系別": r[3], "交易筆數": r[4], "交易金額": r[5]
    } for r in rows])

@app.route("/api/monthly_pass/trend")
def mp_trend():
    """指定方案代碼的歷史趨勢"""
    scheme = request.args.get("scheme", "")
    sys_   = request.args.get("sys", "")   # SVC / QR / 空=全部
    conn = get_conn()
    if not _table_exists(conn, "月票交易統計"):
        conn.close()
        return jsonify([])
    conds, params = [], []
    if scheme:
        conds.append('方案代碼=?'); params.append(scheme)
    if sys_:
        conds.append('體系別=?'); params.append(sys_)
    where = ('WHERE ' + ' AND '.join(conds)) if conds else ''
    rows = conn.execute(f'''
        SELECT ym, yr, mn,
               SUM(交易筆數) AS 交易筆數,
               SUM(交易金額) AS 交易金額
        FROM "月票交易統計" {where}
        GROUP BY yr, mn ORDER BY yr, mn
    ''', params).fetchall()
    conn.close()
    return jsonify([{
        "ym": r[0], "yr": r[1], "mn": r[2],
        "交易筆數": r[3], "交易金額": r[4]
    } for r in rows])

@app.route("/api/monthly_pass/latest_summary")
def mp_latest_summary():
    """最新月份：總計 + 體系別分計 + 各方案排行"""
    conn = get_conn()
    if not _table_exists(conn, "月票交易統計"):
        conn.close()
        return jsonify({})
    latest = conn.execute(
        'SELECT yr, mn, ym FROM "月票交易統計" ORDER BY yr DESC, mn DESC LIMIT 1'
    ).fetchone()
    if not latest:
        conn.close()
        return jsonify({})
    yr, mn, ym = latest

    # 體系別加總
    sys_rows = conn.execute('''
        SELECT 體系別, SUM(交易筆數), SUM(交易金額)
        FROM "月票交易統計" WHERE yr=? AND mn=?
        GROUP BY 體系別 ORDER BY 體系別
    ''', [yr, mn]).fetchall()

    # 各方案排行（合計 SVC+QR）
    scheme_rows = conn.execute('''
        SELECT 方案代碼, 方案名稱,
               SUM(交易筆數) AS 筆數, SUM(交易金額) AS 金額
        FROM "月票交易統計" WHERE yr=? AND mn=?
        GROUP BY 方案代碼, 方案名稱
        ORDER BY 金額 DESC
    ''', [yr, mn]).fetchall()

    conn.close()
    return jsonify({
        "ym": ym, "yr": yr, "mn": mn,
        "by_system": [{"體系別": r[0], "交易筆數": r[1], "交易金額": r[2]} for r in sys_rows],
        "by_scheme": [{"方案代碼": r[0], "方案名稱": r[1], "交易筆數": r[2], "交易金額": r[3]} for r in scheme_rows],
    })

@app.route("/api/monthly_pass/scheme_monthly")
def mp_scheme_monthly():
    """各月份 × 方案 彙整（SVC+QR 加總）"""
    conn = get_conn()
    if not _table_exists(conn, "月票交易統計"):
        conn.close()
        return jsonify([])
    rows = conn.execute('''
        SELECT yr, mn, ym, 方案代碼, 方案名稱,
               SUM(交易筆數) AS 交易筆數, SUM(交易金額) AS 交易金額
        FROM "月票交易統計"
        GROUP BY yr, mn, ym, 方案代碼, 方案名稱
        ORDER BY yr, mn, 方案代碼
    ''').fetchall()
    conn.close()
    return jsonify([{"yr":r[0],"mn":r[1],"ym":r[2],"方案代碼":r[3],"方案名稱":r[4],"交易筆數":r[5],"交易金額":r[6]} for r in rows])

# ══════════════════════════════════════════════════════════
# Page
# ══════════════════════════════════════════════════════════
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/docs")
def api_docs():
    """回傳資料庫欄位說明 Markdown 文字"""
    from flask import Response
    md_path = BASE_DIR / "資料庫欄位說明.md"
    if not md_path.exists():
        return Response("# 找不到說明文件", mimetype="text/plain; charset=utf-8")
    return Response(md_path.read_text(encoding="utf-8"),
                    mimetype="text/plain; charset=utf-8")


if __name__ == "__main__":
    print("FSC 電子支付資料系統")
    print(f"資料庫: {DB_PATH}")
    port = int(os.environ.get("PORT", 5000))
    host = "0.0.0.0" if os.environ.get("RENDER") else "127.0.0.1"
    debug = not os.environ.get("RENDER")
    print(f"啟動中... http://{host}:{port}")
    app.run(debug=debug, host=host, port=port)
