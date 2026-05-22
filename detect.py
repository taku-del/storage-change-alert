"""
ストレージ使用量の変化検知 + 更新リスク検知スクリプト

Tableau Cloud の CS_利用統計_統合 から直近データを取得し、
前週比で大きな変化があった顧客、および更新間近で接触空白のある顧客を検出。

結果は results/latest.json に保存し、GitHub Actions がコミット。
Slack投稿は Claude Remote Trigger が MCP 経由で行う。

検出カテゴリ:
  1. 高使用率帯で急増: 既に80%以上 かつ 使用率+5pt以上 かつ 変化量20GB以上
  2. 解約リスク（急減）: 元々50%以上から 使用率-10pt以上 かつ 変化量20GB以上
  3. 更新リスク（接触空白）: 更新3ヶ月以内 × 能動的接触90日以上前 × エンタープライズ/プレミアム

環境変数:
  TABLEAU_PAT_SECRET  — Tableau Cloud PAT
"""

import json
import os
import sys
import zipfile
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import tableauserverclient as TSC
import pantab

# ── Tableau 設定 ──
SERVER_URL = "https://prod-apnortheast-a.online.tableau.com"
SITE_NAME = "directcloud"
TOKEN_NAME = "claude-api"
TOKEN_SECRET = os.environ.get("TABLEAU_PAT_SECRET", "")

RESULTS_DIR = Path(__file__).parent / "results"

# ── 検出閾値 ──
CHANGE_GB_MIN = 20
SURGE_PCT_THRESHOLD = 5
SURGE_RATE_MIN = 80
DROP_PCT_THRESHOLD = -10
DROP_RATE_MIN = 50
LOOKBACK_DAYS = 7
SLACK_DISPLAY_MAX = 10

# ── 更新リスク閾値 ──
RENEWAL_MONTHS_AHEAD = 3
CONTACT_SILENT_DAYS = 90
RENEWAL_DISPLAY_MAX = 10

SF_BASE_URL = "https://directcloud.my.salesforce.com"
TABLEAU_DASHBOARD_URL = "https://prod-apnortheast-a.online.tableau.com/#/site/directcloud/workbooks/4681647"

# エンタープライズ/プレミアム（通知対象）
UPPER_PLANS = {
    'プレミアムプラン', 'エンタープライズプラン',
    'プレミアムプラン(2024)', 'エンタープライズプラン(2024)',
    'プレミアム(2025)', 'エンタープライズ(2025)',
}

# プラン優先度
PLAN_RANK = {}
for _p in UPPER_PLANS:
    if 'エンタープライズ' in _p:
        PLAN_RANK[_p] = 2
    else:
        PLAN_RANK[_p] = 1

# 能動的接触とみなすTaskのSubjectキーワード
ACTIVE_CONTACT_KEYWORDS = [
    '電話', '打合せ', '打ち合わせ', '商談', '訪問',
    'ミーティング', 'MTG', '状況確認', '追い電話', '案件フォロー',
    'ToDo-電話',
]

DS_NAME = "CS_利用統計_統合"


def download_extract():
    """Tableau Cloud から CS_利用統計_統合 をダウンロード"""
    if not TOKEN_SECRET:
        print("ERROR: TABLEAU_PAT_SECRET が未設定", file=sys.stderr)
        sys.exit(1)

    auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_SECRET, site_id=SITE_NAME)
    server = TSC.Server(SERVER_URL, use_server_version=True)

    with server.auth.sign_in(auth):
        datasources, _ = server.datasources.get()
        target_ds = next(
            (ds for ds in datasources if ds.name == DS_NAME),
            None,
        )
        if not target_ds:
            print(f"ERROR: {DS_NAME} が見つかりません", file=sys.stderr)
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

    return tables


def build_company_map(account: pd.DataFrame) -> dict:
    cols = {
        "ID__c": "company_id",
        "Id": "sf_account_id",
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
        sf_id = info.get("sf_account_id", "")
        entry = {
            "company_id": str(cid),
            "sf_account_id": sf_id,
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


def _sf_link(name: str, sf_id: str) -> str:
    """企業名をSalesforceリンク付きで返す"""
    if sf_id:
        return f"<{SF_BASE_URL}/{sf_id}|{name}>"
    return name


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
        name_link = _sf_link(a["name"], a.get("sf_account_id", ""))
        lines.append(
            f"  • *{name_link}* — +{a['change_gb']}GB / "
            f"+{a['change_pct']}pt ({a['rate_prev']}% → {a['rate_now']}%)  "
            f"[{a['storage_used_gb']}/{a['volume_size_gb']}GB]"
        )
    if count > SLACK_DISPLAY_MAX:
        lines.append(f"  … 他 {count - SLACK_DISPLAY_MAX} 件")
    return "\n".join(lines)


def detect_renewal_risk(
    account: pd.DataFrame,
    task: pd.DataFrame,
    contract: pd.DataFrame,
    contract_line: pd.DataFrame,
    company_map: dict,
) -> list:
    """更新3ヶ月以内 × 能動的接触90日以上前 × エンタープライズ/プレミアム を検出"""
    today = datetime.now().date()
    cutoff_date = today + timedelta(days=RENEWAL_MONTHS_AHEAD * 30)
    silent_since = today - timedelta(days=CONTACT_SILENT_DAYS)

    # 能動的接触のTaskのみフィルタ
    task = task.copy()
    task["Subject"] = task["Subject"].fillna("")
    kw_pattern = "|".join(ACTIVE_CONTACT_KEYWORDS)
    task = task[task["Subject"].str.contains(kw_pattern, case=False, regex=True)]
    task["ActivityDate"] = pd.to_datetime(task["ActivityDate"], errors="coerce")
    last_activity = (
        task.groupby("AccountId")["ActivityDate"]
        .max()
        .reset_index()
        .rename(columns={"ActivityDate": "last_activity"})
    )

    # アクティブ契約の次回更新日
    active = contract[
        (contract["IsActive__c"] == True) & (contract["Churn__c"] == False)
    ].copy()
    active["Contract_Planned_End_Month__c"] = pd.to_datetime(
        active["Contract_Planned_End_Month__c"], errors="coerce"
    )
    next_renewal = (
        active.groupby("Account__c")["Contract_Planned_End_Month__c"]
        .max()
        .reset_index()
        .rename(columns={"Contract_Planned_End_Month__c": "next_end"})
    )

    # 月額合計（アクティブ明細）
    active_cli = contract_line[contract_line["IsActive__c"] == True].copy()
    cc_acct = contract[["Id", "Account__c"]].drop_duplicates()
    cli_merged = active_cli.merge(
        cc_acct, left_on="CustomContract__c", right_on="Id", suffixes=("", "_cc")
    )
    monthly = (
        cli_merged.groupby("Account__c")["UnitPrice__c"]
        .sum()
        .reset_index()
        .rename(columns={"UnitPrice__c": "monthly_amount"})
    )

    # Account に結合
    acct = account[account["Type"] == "顧客"][
        ["Id", "Name", "ID__c", "contractplan__c"]
    ].copy()
    merged = acct.merge(next_renewal, left_on="Id", right_on="Account__c", how="inner")
    merged = merged.merge(last_activity, left_on="Id", right_on="AccountId", how="left")
    merged = merged.merge(monthly, left_on="Id", right_on="Account__c", how="left")

    # フィルタ: 更新3ヶ月以内
    merged = merged[
        (merged["next_end"].dt.date >= today)
        & (merged["next_end"].dt.date <= cutoff_date)
    ]

    # フィルタ: エンタープライズ/プレミアムのみ
    merged = merged[merged["contractplan__c"].isin(UPPER_PLANS)]

    # フィルタ: 能動的接触90日以上前 or 記録なし
    merged = merged[
        (merged["last_activity"].isna())
        | (merged["last_activity"].dt.date < silent_since)
    ]

    # 日数計算・ソート（プラン上位 → 金額大 → 更新日近い）
    merged["days_since"] = merged["last_activity"].apply(
        lambda x: (today - x.date()).days if pd.notna(x) else 9999
    )
    merged["plan_rank"] = merged["contractplan__c"].map(PLAN_RANK).fillna(0)
    merged["monthly_amount"] = merged["monthly_amount"].fillna(0)
    merged = merged.sort_values(
        ["plan_rank", "monthly_amount", "next_end"], ascending=[False, False, True]
    )

    results = []
    for _, row in merged.iterrows():
        cid = str(row.get("ID__c", ""))
        info = company_map.get(cid, {})
        sf_id = info.get("sf_account_id", str(row.get("Id", "")))
        last_act = row["last_activity"]

        results.append({
            "name": str(row["Name"]),
            "sf_account_id": sf_id,
            "plan": str(row.get("contractplan__c", "")),
            "next_end": row["next_end"].strftime("%Y-%m-%d"),
            "last_activity": last_act.strftime("%Y-%m-%d") if pd.notna(last_act) else None,
            "days_since_contact": int(row["days_since"]),
            "monthly_amount": int(row["monthly_amount"]),
            "agency": info.get("agency", ""),
        })

    print(f"[検出] 更新リスク: {len(results)} 件")
    return results


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
        name_link = _sf_link(a["name"], a.get("sf_account_id", ""))
        lines.append(
            f"  • *{name_link}* — {a['change_gb']}GB / "
            f"{a['change_pct']}pt ({a['rate_prev']}% → {a['rate_now']}%)  "
            f"[{a['storage_used_gb']}/{a['volume_size_gb']}GB]"
        )
    if count > SLACK_DISPLAY_MAX:
        lines.append(f"  … 他 {count - SLACK_DISPLAY_MAX} 件")
    lines.append("")
    lines.append(f":bar_chart: <{TABLEAU_DASHBOARD_URL}|Tableauダッシュボードで詳細確認>")
    return "\n".join(lines)


def format_renewal_risk_message(renewal_risks: list) -> str:
    count = len(renewal_risks)
    today = datetime.now().strftime("%Y-%m-%d")

    if count == 0:
        return (
            f":large_green_circle: *更新リスク（接触空白）* ({today})\n"
            f"該当なし"
        )

    lines = [
        "<!channel>",
        f":warning: *更新リスク（フォロー空白）* ({today})",
        f"更新3ヶ月以内 × 直近{CONTACT_SILENT_DAYS}日間 電話・商談・打合せ・訪問等の活動なし × エンタープライズ/プレミアム | 検出: {count} 件",
        "",
    ]
    for a in renewal_risks[:RENEWAL_DISPLAY_MAX]:
        name_link = _sf_link(a["name"], a.get("sf_account_id", ""))
        days = a["days_since_contact"]
        days_str = f"{days}日前" if days < 9999 else "記録なし"
        plan_short = (
            a["plan"]
            .replace("プラン", "")
            .replace("(2024)", "")
            .replace("(2025)", "")
        )
        amt = f"¥{a['monthly_amount']:,}" if a.get("monthly_amount") else "不明"
        lines.append(
            f"  • *{name_link}* — {plan_short} / {amt}/月 / "
            f"更新: {a['next_end']} / 最終接触: {days_str}"
        )
    if count > RENEWAL_DISPLAY_MAX:
        lines.append(f"  … 他 {count - RENEWAL_DISPLAY_MAX} 件")
    return "\n".join(lines)


def main():
    tables = download_extract()
    usage = tables[("Extract", "usage_statistics")]
    account = tables[("Extract", "Account")]
    task = tables[("Extract", "Task")]
    contract = tables[("Extract", "CustomContract__c")]
    contract_line = tables[("Extract", "ContractLineItem__c")]

    company_map = build_company_map(account)
    result = detect_changes(usage, company_map)

    surge_count = len(result["alerts"]["surge"])
    churn_count = len(result["alerts"]["churn_risk"])
    print(f"\n高使用率帯で急増: {surge_count} 件")
    print(f"解約リスク（急減）: {churn_count} 件")

    # 更新リスク検出
    renewal_risks = detect_renewal_risk(
        account, task, contract, contract_line, company_map
    )
    result["alerts"]["renewal_risk"] = renewal_risks

    # Slack用メッセージを生成
    result["slack_messages"] = {
        "surge": format_surge_message(result),
        "churn_risk": format_churn_message(result),
        "renewal_risk": format_renewal_risk_message(renewal_risks),
    }

    # 結果保存
    RESULTS_DIR.mkdir(exist_ok=True)
    out_path = RESULTS_DIR / "latest.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"[保存] {out_path}")


if __name__ == "__main__":
    main()
