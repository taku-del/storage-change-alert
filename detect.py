"""
ストレージ使用量の変化検知スクリプト

Tableau Cloud の usage_statistics から直近データを取得し、
前週比で大きな変化があった顧客を検出 → Slack投稿。

検出カテゴリ:
  1. 高使用率帯で急増: 既に80%以上 かつ 使用率+5pt以上 かつ 変化量20GB以上
  2. 解約リスク: 元々50%以上から 使用率-10pt以上 かつ 変化量20GB以上

環境変数:
  TABLEAU_PAT_SECRET  — Tableau Cloud PAT
  SLACK_BOT_TOKEN     — Slack Bot Token (chat:write スコープ)
"""

import json
import os
import sys
import zipfile
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import requests
import tableauserverclient as TSC
import pantab

# ── Tableau 設定 ──
SERVER_URL = "https://prod-apnortheast-a.online.tableau.com"
SITE_NAME = "directcloud"
TOKEN_NAME = "claude-api"
TOKEN_SECRET = os.environ.get("TABLEAU_PAT_SECRET", "")

# ── Slack 設定 ──
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL_SURGE = "C09MLT5LGRW"    # 高使用率帯で急増
SLACK_CHANNEL_CHURN = "C0AKQD8GVSR"    # 解約リスク

# ── 検出閾値 ──
CHANGE_GB_MIN = 20
SURGE_PCT_THRESHOLD = 5
SURGE_RATE_MIN = 80
DROP_PCT_THRESHOLD = -10
DROP_RATE_MIN = 50
LOOKBACK_DAYS = 7
SLACK_DISPLAY_MAX = 10


def download_extract():
    """Tableau Cloud から usage_statistics + Account をダウンロード"""
    if not TOKEN_SECRET:
        print("ERROR: TABLEAU_PAT_SECRET が未設定", file=sys.stderr)
        sys.exit(1)

    auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_SECRET, site_id=SITE_NAME)
    server = TSC.Server(SERVER_URL, use_server_version=True)

    with server.auth.sign_in(auth):
        datasources, _ = server.datasources.get()
        target_ds = next(
            (ds for ds in datasources if ds.name == "usage_statistics (tableau)"),
            None,
        )
        if not target_ds:
            print("ERROR: usage_statistics (tableau) が見つかりません", file=sys.stderr)
            sys.exit(1)

        print(f"[Tableau] ダウンロード: {target_ds.name}")

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = server.datasources.download(
                target_ds.id, filepath=tmpdir, include_extract=True
            )
            with zipfile.ZipFile(filepath, "r") as z:
                hyper_files = [f for f in z.namelist() if f.endswith(".hyper")]
                z.extract(hyper_files[0], tmpdir)
                hyper_path = os.path.join(tmpdir, hyper_files[0])

            tables = pantab.frames_from_hyper(hyper_path)

    return tables[("Extract", "usage_statistics")], tables[("Extract", "Account")]


def build_company_map(account: pd.DataFrame) -> dict:
    cols = {
        "ID__c": "company_id",
        "Name": "account_name",
        "contractplan__c": "contract_plan",
        "Agency__c": "agency",
    }
    available = {k: v for k, v in cols.items() if k in account.columns}
    acct = account[list(available.keys())].copy()
    acct.columns = list(available.values())

    if "agency" in acct.columns:
        acct["agency"] = (
            acct["agency"]
            .fillna("")
            .str.replace("【代理店】", "", regex=False)
            .str.strip()
        )

    acct = acct.drop_duplicates(subset="company_id", keep="first")
    return acct.set_index("company_id").to_dict("index")


def detect_changes(usage: pd.DataFrame, company_map: dict) -> dict:
    usage = usage.copy()
    usage["stat_date"] = pd.to_datetime(usage["stat_date"])

    latest_date = usage["stat_date"].max()
    compare_date_target = latest_date - timedelta(days=LOOKBACK_DAYS)

    available_dates = sorted(usage["stat_date"].unique())
    compare_date = min(available_dates, key=lambda d: abs(d - compare_date_target))

    print(f"[検出] 最新: {latest_date.date()} / 比較: {compare_date.date()}")

    latest_data = usage[usage["stat_date"] == latest_date].set_index("company_id")
    compare_data = usage[usage["stat_date"] == compare_date].set_index("company_id")
    common_ids = latest_data.index.intersection(compare_data.index)

    alerts = {"surge": [], "churn_risk": []}

    for cid in common_ids:
        now = latest_data.loc[cid]
        prev = compare_data.loc[cid]

        su_now = float(now.get("storage_used_gb", 0) or 0)
        su_prev = float(prev.get("storage_used_gb", 0) or 0)
        vol = float(now.get("volume_size_gb", 0) or 0)

        if vol <= 0:
            continue

        rate_now = round(su_now / vol * 100, 1)
        rate_prev = round(su_prev / vol * 100, 1)
        change_gb = round(su_now - su_prev, 2)
        change_pct = round(rate_now - rate_prev, 1)

        info = company_map.get(str(cid), {})
        entry = {
            "company_id": str(cid),
            "name": info.get("account_name", str(cid)),
            "plan": info.get("contract_plan", ""),
            "agency": info.get("agency", ""),
            "storage_used_gb": round(su_now, 2),
            "volume_size_gb": round(vol, 2),
            "rate_now": rate_now,
            "rate_prev": rate_prev,
            "change_gb": change_gb,
            "change_pct": change_pct,
        }

        if (rate_prev >= SURGE_RATE_MIN
                and change_pct >= SURGE_PCT_THRESHOLD
                and change_gb >= CHANGE_GB_MIN):
            alerts["surge"].append(entry)

        if (rate_prev >= DROP_RATE_MIN
                and change_pct <= DROP_PCT_THRESHOLD
                and change_gb <= -CHANGE_GB_MIN):
            alerts["churn_risk"].append(entry)

    alerts["surge"].sort(key=lambda x: x["change_pct"], reverse=True)
    alerts["churn_risk"].sort(key=lambda x: x["change_pct"])

    return {
        "latest_date": latest_date.strftime("%Y-%m-%d"),
        "compare_date": compare_date.strftime("%Y-%m-%d"),
        "alerts": alerts,
        "total_companies": len(common_ids),
        "generated_at": datetime.now().isoformat(),
    }


def format_surge_message(result: dict) -> str:
    alerts = result["alerts"]["surge"]
    latest = result["latest_date"]
    compare = result["compare_date"]
    count = len(alerts)

    if count == 0:
        return (
            f"<!channel>\n"
            f":large_green_circle: *高使用率帯 急増検知* ({latest})\n"
            f"比較期間: {compare} → {latest}\n"
            f"該当なし"
        )

    lines = [
        "<!channel>",
        f":chart_with_upwards_trend: *高使用率帯で急増* ({latest})",
        f"比較期間: {compare} → {latest} | 検出: {count} 件",
        f"条件: 使用率80%以上 かつ +5pt以上 かつ +20GB以上",
        "",
    ]
    for a in alerts[:SLACK_DISPLAY_MAX]:
        lines.append(
            f"  • *{a['name']}* — +{a['change_gb']}GB / "
            f"+{a['change_pct']}pt ({a['rate_prev']}% → {a['rate_now']}%)  "
            f"[{a['storage_used_gb']}/{a['volume_size_gb']}GB]"
        )
    if count > SLACK_DISPLAY_MAX:
        lines.append(f"  … 他 {count - SLACK_DISPLAY_MAX} 件")
    return "\n".join(lines)


def format_churn_message(result: dict) -> str:
    alerts = result["alerts"]["churn_risk"]
    latest = result["latest_date"]
    compare = result["compare_date"]
    count = len(alerts)

    if count == 0:
        return (
            f"<!channel>\n"
            f":large_green_circle: *解約リスク検知* ({latest})\n"
            f"比較期間: {compare} → {latest}\n"
            f"該当なし"
        )

    lines = [
        "<!channel>",
        f":chart_with_downwards_trend: *解約リスク（ストレージ急減）* ({latest})",
        f"比較期間: {compare} → {latest} | 検出: {count} 件",
        f"条件: 使用率50%以上から -10pt以上 かつ -20GB以上",
        "",
    ]
    for a in alerts[:SLACK_DISPLAY_MAX]:
        lines.append(
            f"  • *{a['name']}* — {a['change_gb']}GB / "
            f"{a['change_pct']}pt ({a['rate_prev']}% → {a['rate_now']}%)  "
            f"[{a['storage_used_gb']}/{a['volume_size_gb']}GB]"
        )
    if count > SLACK_DISPLAY_MAX:
        lines.append(f"  … 他 {count - SLACK_DISPLAY_MAX} 件")
    return "\n".join(lines)


def post_to_slack(channel: str, text: str):
    """Slack Bot Token で投稿"""
    if not SLACK_BOT_TOKEN:
        print(f"[Slack] SLACK_BOT_TOKEN 未設定 — スキップ ({channel})")
        print(text)
        return

    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        json={"channel": channel, "text": text},
    )
    data = resp.json()
    if data.get("ok"):
        print(f"[Slack] 投稿成功: {channel}")
    else:
        print(f"[Slack] 投稿失敗: {channel} — {data.get('error')}", file=sys.stderr)


def main():
    usage, account = download_extract()
    company_map = build_company_map(account)
    result = detect_changes(usage, company_map)

    surge_count = len(result["alerts"]["surge"])
    churn_count = len(result["alerts"]["churn_risk"])
    print(f"\n高使用率帯で急増: {surge_count} 件")
    print(f"解約リスク:       {churn_count} 件")

    # チャンネル別に投稿
    surge_msg = format_surge_message(result)
    churn_msg = format_churn_message(result)

    post_to_slack(SLACK_CHANNEL_SURGE, surge_msg)
    post_to_slack(SLACK_CHANNEL_CHURN, churn_msg)


if __name__ == "__main__":
    main()
