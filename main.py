#!/usr/bin/env python3
"""
2026年东北天气数据采集脚本
- 追赶阶段：每天 219 地址 × 最多 4 天，补历史数据
- 日常阶段：每天 219 地址 × 1 天，采前一天数据
- 失败记录在追赶阶段跳过，日常阶段优先重试
API: OpenWeatherMap One Call API 3.0 - Day Summary
"""
import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

# ── 配置 ──
TOWNS_CSV = "townsNE.csv"
OUTPUT_CSV = "2026.csv"
PROGRESS_FILE = "progress.json"
MAX_CATCHUP_DAYS = 4       # 追赶阶段每天最多补几天
DAILY_BUDGET = 219         # 日常阶段每天最多调用次数
DATE_START = "2026-04-01"
DATE_END = "2026-09-30"
TZ_BEIJING = timezone(timedelta(hours=8))

API_KEY = os.environ.get("OPENWEATHER_API_KEY", "")
if not API_KEY:
    print("ERROR: OPENWEATHER_API_KEY 未设置")
    sys.exit(1)


def date_range(start_str, end_str):
    """生成 start~end 的日期字符串列表（含首尾）"""
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


def load_towns():
    """加载地点列表"""
    towns = []
    if not os.path.exists(TOWNS_CSV):
        print(f"ERROR: 找不到 {TOWNS_CSV}")
        sys.exit(1)
    with open(TOWNS_CSV, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            tid = row[0].strip()
            name = row[1].strip()
            coord_str = row[2].strip().strip('"').replace("，", ",")
            parts = coord_str.split(",")
            if len(parts) != 2:
                continue
            lat, lon = parts[0].strip(), parts[1].strip()
            towns.append({"id": tid, "name": name, "lat": lat, "lon": lon})
    return towns


def load_progress():
    """加载进度文件"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"last_date": None, "end_date": DATE_END, "failed": []}


def save_progress(last_date, end_date, failed):
    """保存进度文件"""
    with open(PROGRESS_FILE, "w") as f:
        json.dump({
            "last_date": last_date,
            "end_date": end_date,
            "failed": failed
        }, f, ensure_ascii=False, indent=2)


def ensure_output_header():
    """初始化 CSV 表头"""
    if not os.path.exists(OUTPUT_CSV) or os.path.getsize(OUTPUT_CSV) == 0:
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "id", "name", "lat", "lon", "date",
                "temp_min", "temp_max", "temp_afternoon",
                "temp_night", "temp_evening", "temp_morning",
                "precipitation_total"
            ])


def fetch_daily(lat, lon, date_str):
    """调用 API 获取单日天气汇总"""
    url = (
        f"https://api.openweathermap.org/data/3.0/onecall/day_summary"
        f"?lat={lat}&lon={lon}&date={date_str}"
        f"&units=metric&appid={API_KEY}"
    )
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        print(f"    HTTP {e.code}: {body[:120]}")
        return None
    except Exception as e:
        print(f"    异常: {e}")
        return None


def extract_row(town, date_str, data):
    """从 API 响应中提取 CSV 格式的数据行"""
    temp = data.get("temperature", {})
    precip = data.get("precipitation", {})
    return [
        town["id"], town["name"], town["lat"], town["lon"], date_str,
        temp.get("min", ""), temp.get("max", ""),
        temp.get("afternoon", ""), temp.get("night", ""),
        temp.get("evening", ""), temp.get("morning", ""),
        precip.get("total", ""),
    ]


def main():
    now_bj = datetime.now(TZ_BEIJING)
    yesterday_str = (now_bj - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"运行时间: {now_bj.strftime('%Y-%m-%d %H:%M')} 北京时间")
    print(f"昨天日期: {yesterday_str}")

    # 加载数据
    towns = load_towns()
    print(f"总地点数: {len(towns)}")

    progress = load_progress()
    last_date = progress.get("last_date")
    end_date = progress.get("end_date", DATE_END)
    failed = progress.get("failed", [])

    # 已完成，直接退出
    if last_date and last_date >= end_date:
        print(f"✅ 采集已完成（最后日期: {last_date}），无需操作。")
        return

    # 确定本次要采集的日期范围
    if last_date is None:
        # 首次运行，从起始日期开始
        range_start = DATE_START
    else:
        # 从上次最后日期的下一天开始
        last_dt = datetime.strptime(last_date, "%Y-%m-%d")
        range_start = (last_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    # 结束日期：不超过昨天
    range_end = min(yesterday_str, end_date)

    if range_start > range_end:
        print(f"没有需要采集的新日期（range_start={range_start}, range_end={range_end}）")
        # 但可能还有失败记录要处理，继续往下走
    else:
        print(f"新日期范围: {range_start} ~ {range_end}")

    # 生成要采集的日期列表
    if range_start <= range_end:
        all_dates = date_range(range_start, range_end)
    else:
        all_dates = []

    # 限制追赶天数
    dates_to_fetch = all_dates[:MAX_CATCHUP_DAYS]
    skipped_dates = all_dates[MAX_CATCHUP_DAYS:]

    if skipped_dates:
        print(f"本次采集 {len(dates_to_fetch)} 天（{dates_to_fetch[0]} ~ {dates_to_fetch[-1]}），"
              f"跳过 {len(skipped_dates)} 天留给后续运行")

    ensure_output_header()

    total_api_calls = 0
    new_rows = []
    phase1_failed = []   # 阶段1新产生的失败
    phase2_failed = []   # 阶段2重试仍然失败的

    # ── 阶段 1：采集新日期 ──
    for date_str in dates_to_fetch:
        print(f"\n📅 采集日期: {date_str}（{len(towns)} 个地点）")
        date_ok = 0
        date_fail = 0

        for town in towns:
            data = fetch_daily(town["lat"], town["lon"], date_str)
            total_api_calls += 1

            if data:
                new_rows.append(extract_row(town, date_str, data))
                date_ok += 1
            else:
                # 失败：写空行 + 记录到失败列表
                new_rows.append([
                    town["id"], town["name"], town["lat"], town["lon"], date_str,
                    "", "", "", "", "", "", ""
                ])
                phase1_failed.append({
                    "town_id": town["id"],
                    "name": town["name"],
                    "lat": town["lat"],
                    "lon": town["lon"],
                    "date": date_str
                })
                date_fail += 1

            time.sleep(1)

        print(f"  结果: 成功 {date_ok}, 失败 {date_fail}")

        # 更新 last_date（即使部分失败也推进）
        save_progress(date_str, end_date, failed + phase1_failed)

    # ── 阶段 2：重试历史失败记录 ──
    # 计算剩余预算（阶段 1 用了多少）
    budget_used = len(dates_to_fetch) * len(towns)
    remaining_budget = DAILY_BUDGET - budget_used

    if failed and remaining_budget > 0:
        retry_list = failed[:remaining_budget]
        print(f"\n🔄 重试历史失败记录: {len(retry_list)} 条（剩余预算 {remaining_budget}）")

        for item in retry_list:
            # 检查是否在采集日期范围内
            if item["date"] < DATE_START or item["date"] > end_date:
                continue

            data = fetch_daily(item["lat"], item["lon"], item["date"])
            total_api_calls += 1

            if data:
                town_ref = {"id": item["town_id"], "name": item["name"],
                            "lat": item["lat"], "lon": item["lon"]}
                new_rows.append(extract_row(town_ref, item["date"], data))
                # 从 failed 中移除（成功了）
                failed = [f for f in failed if not (
                    f["town_id"] == item["town_id"] and f["date"] == item["date"]
                )]
            else:
                # 仍然失败，记录下来
                phase2_failed.append(item)

            time.sleep(1)

    # ── 写入 CSV ──
    if new_rows:
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(new_rows)

    # ── 最终保存进度 ──
    # failed: 旧列表中未被重试的 + 重试仍然失败的 + 阶段1新失败的
    final_last_date = dates_to_fetch[-1] if dates_to_fetch else last_date
    final_failed = failed + phase2_failed + phase1_failed
    # 去重
    seen = set()
    deduped_failed = []
    for f in final_failed:
        key = (f["town_id"], f["date"])
        if key not in seen:
            seen.add(key)
            deduped_failed.append(f)

    save_progress(final_last_date or last_date, end_date, deduped_failed)

    # ── 汇总 ──
    print(f"\n{'='*50}")
    print(f"本次 API 调用: {total_api_calls} 次")
    print(f"写入记录: {len(new_rows)} 条")
    print(f"最后采集日期: {final_last_date or last_date}")
    print(f"剩余失败记录: {len(deduped_failed)} 条")

    if deduped_failed:
        # 按日期统计失败数
        fail_by_date = {}
        for f in deduped_failed:
            fail_by_date[f["date"]] = fail_by_date.get(f["date"], 0) + 1
        for d in sorted(fail_by_date.keys()):
            print(f"  {d}: {fail_by_date[d]} 条失败")

    if final_last_date and final_last_date >= end_date and not deduped_failed:
        print("🎉 所有数据采集完毕！")
    else:
        remaining = date_range(
            (datetime.strptime(final_last_date or DATE_START, "%Y-%m-%d") +
             timedelta(days=1)).strftime("%Y-%m-%d"),
            end_date
        ) if (final_last_date or DATE_START) <= end_date else []
        if remaining:
            days_needed = (len(remaining) + MAX_CATCHUP_DAYS - 1) // MAX_CATCHUP_DAYS
            print(f"剩余 {len(remaining)} 天新数据，追赶阶段约还需 {days_needed} 天")


if __name__ == "__main__":
    main()
