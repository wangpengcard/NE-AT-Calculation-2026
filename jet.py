#!/usr/bin/env python3
"""
东北天气数据缺失补齐脚本（单次运行版）
- 扫描 444.csv 中的空值行
- 循环调用 API 直到所有缺失数据补齐
- 一次性写回 444.csv
API: OpenWeatherMap One Call API 3.0 - Day Summary
"""
import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error

# ── 配置 ──
INPUT_CSV = "444.csv"
RETRY_LIMIT = 3
SLEEP_BETWEEN = 1.2

API_KEY = os.environ.get("OPENWEATHER_API_KEY", "")
if not API_KEY:
    print("ERROR: OPENWEATHER_API_KEY 未设置")
    sys.exit(1)

DATA_COLUMNS = [
    "temp_min", "temp_max", "temp_afternoon",
    "temp_night", "temp_evening", "temp_morning",
    "precipitation_total"
]


def fetch_daily(lat, lon, date_str):
    url = (
        f"https://api.openweathermap.org/data/3.0/onecall/day_summary"
        f"?lat={lat}&lon={lon}&date={date_str}"
        f"&units=metric&appid={API_KEY}"
    )
    req = urllib.request.Request(url)
    for attempt in range(RETRY_LIMIT):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode() if e.fp else ""
            print(f"    HTTP {e.code} (attempt {attempt+1}): {body[:120]}")
            if e.code == 429:
                time.sleep(10)
            elif e.code >= 500:
                time.sleep(3)
            else:
                break
        except Exception as e:
            print(f"    异常 (attempt {attempt+1}): {e}")
            time.sleep(2)
    return None


def extract_values(data):
    temp = data.get("temperature", {})
    precip = data.get("precipitation", {})
    return {
        "temp_min": temp.get("min", ""),
        "temp_max": temp.get("max", ""),
        "temp_afternoon": temp.get("afternoon", ""),
        "temp_night": temp.get("night", ""),
        "temp_evening": temp.get("evening", ""),
        "temp_morning": temp.get("morning", ""),
        "precipitation_total": precip.get("total", ""),
    }


def is_row_missing(row):
    for col in DATA_COLUMNS:
        val = row.get(col, "").strip()
        if val == "" or val == "None":
            return True
    return False


def main():
    if not os.path.exists(INPUT_CSV):
        print(f"ERROR: 找不到 {INPUT_CSV}")
        sys.exit(1)

    # 读取 CSV
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"总记录数: {len(rows)}")

    # 找缺失行
    missing = []
    for i, row in enumerate(rows):
        if is_row_missing(row):
            missing.append(i)

    print(f"缺失记录: {len(missing)} 条")

    if not missing:
        print("✅ 没有缺失数据！")
        return

    # 循环补齐，带重试
    fixed = 0
    still_missing = []

    for idx in missing:
        row = rows[idx]
        name = row.get("name", "")
        lat = row.get("lat", "")
        lon = row.get("lon", "")
        date_str = row.get("date", "")

        if not date_str or not lat or not lon:
            print(f"  跳过 #{idx}: 缺少基础信息 ({name}, {date_str})")
            still_missing.append(idx)
            continue

        print(f"  补齐 #{idx}: {name} ({date_str})")
        data = fetch_daily(lat, lon, date_str)

        if data:
            values = extract_values(data)
            for col in DATA_COLUMNS:
                rows[idx][col] = str(values[col])
            fixed += 1
            print(f"    ✅")
        else:
            still_missing.append(idx)
            print(f"    ❌")

        time.sleep(SLEEP_BETWEEN)

    # 重试失败的（最多再跑 2 轮）
    for retry_round in range(2):
        if not still_missing:
            break
        print(f"\n🔄 重试第 {retry_round+1} 轮: {len(still_missing)} 条")
        next_missing = []
        for idx in still_missing:
            row = rows[idx]
            lat = row.get("lat", "")
            lon = row.get("lon", "")
            date_str = row.get("date", "")
            name = row.get("name", "")

            print(f"  重试 #{idx}: {name} ({date_str})")
            data = fetch_daily(lat, lon, date_str)

            if data:
                values = extract_values(data)
                for col in DATA_COLUMNS:
                    rows[idx][col] = str(values[col])
                fixed += 1
                print(f"    ✅")
            else:
                next_missing.append(idx)
                print(f"    ❌")

            time.sleep(SLEEP_BETWEEN)
        still_missing = next_missing

    # 一次性写回
    with open(INPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # 汇总
    print(f"\n{'='*50}")
    print(f"补齐: {fixed} 条")
    print(f"仍缺失: {len(still_missing)} 条")
    if not still_missing:
        print("🎉 所有数据已补齐！")


if __name__ == "__main__":
    main()
