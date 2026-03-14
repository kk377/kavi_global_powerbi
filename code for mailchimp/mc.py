import os
import time
from datetime import datetime, date, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple

from dotenv import load_dotenv
import requests
import pandas as pd
from flask import Flask, request, session, redirect, url_for
from requests_oauthlib import OAuth2Session

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-only-change-me")

# ---- Use env vars (do NOT hardcode secrets) ----
CLIENT_ID = os.getenv("MAILCHIMP_CLIENT_ID")
CLIENT_SECRET = os.getenv("MAILCHIMP_CLIENT_SECRET")
if not CLIENT_ID or not CLIENT_SECRET:
    print("WARNING: Please set MAILCHIMP_CLIENT_ID and MAILCHIMP_CLIENT_SECRET env vars.")

AUTH_URL = "https://login.mailchimp.com/oauth2/authorize"
TOKEN_URL = "https://login.mailchimp.com/oauth2/token"
METADATA_URL = "https://login.mailchimp.com/oauth2/metadata"

REDIRECT_URI = os.getenv("MAILCHIMP_REDIRECT_URI", "http://127.0.0.1:8000/callback")
SCOPE: List[str] = []

EXPORT_DIR = os.getenv("MAILCHIMP_EXPORT_DIR", "export_mailchimp")
os.makedirs(EXPORT_DIR, exist_ok=True)

# Fixed since date (override if needed)
SINCE_SEND_TIME = os.getenv("MAILCHIMP_SINCE_SEND_TIME", "2025-01-01T00:00:00Z")


# -----------------------
# Helpers
# -----------------------
def make_oauth(state: Optional[str] = None) -> OAuth2Session:
    return OAuth2Session(CLIENT_ID, redirect_uri=REDIRECT_URI, scope=SCOPE, state=state)


def auth_headers(access_token: str) -> Dict[str, str]:
    return {"Authorization": f"OAuth {access_token}"}


def get_dc_and_api_root(access_token: str) -> Tuple[str, str]:
    r = requests.get(METADATA_URL, headers=auth_headers(access_token), timeout=30)
    r.raise_for_status()
    dc = r.json()["dc"]
    return dc, f"https://{dc}.api.mailchimp.com/3.0"


def mc_get(api_root: str, access_token: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{api_root}{path}"
    r = requests.get(url, headers=auth_headers(access_token), params=params or {}, timeout=60)
    r.raise_for_status()
    return r.json()


def paginate_offset(
    api_root: str,
    access_token: str,
    path: str,
    item_key: str,
    base_params: Optional[Dict[str, Any]] = None,
    count: int = 1000,
    max_pages: int = 10000,
    sleep_s: float = 0.2,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    offset = 0
    params = dict(base_params or {})
    params["count"] = count

    for _ in range(max_pages):
        params["offset"] = offset
        data = mc_get(api_root, access_token, path, params=params)
        items = data.get(item_key, [])
        out.extend(items)
        if len(items) < count:
            break
        offset += count
        time.sleep(sleep_s)

    return out


def parse_iso_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def safe_get(d: Dict[str, Any], path: List[str], default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def get_since_time() -> str:
    return SINCE_SEND_TIME


def generate_dim_date(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Build dim_date:
      - date_id: YYYYMMDD
      - week_start_date/week_end_date: Monday-Sunday
      - week_of_year: %U (Sunday-based week num) as an integer
      - iso_week: ISO week number
      - holidays: left blank (is_holiday=False)
    """
    rows: List[Dict[str, Any]] = []
    current = start_date

    while current <= end_date:
        date_id = int(current.strftime("%Y%m%d"))
        quarter = (current.month - 1) // 3 + 1

        iso_week = current.isocalendar()[1]
        week_of_year = int(current.strftime("%U"))

        week_start = current - timedelta(days=current.weekday())  # Monday
        week_end = week_start + timedelta(days=6)                 # Sunday

        rows.append({
            "date_id": date_id,
            "date": current.isoformat(),
            "year": current.year,
            "quarter": quarter,
            "month": current.month,
            "month_name": current.strftime("%B"),
            "week_of_year": week_of_year,
            "iso_week": iso_week,
            "week_start_date": week_start.isoformat(),
            "week_end_date": week_end.isoformat(),
            "day_of_month": current.day,
            "day_of_week": current.weekday() + 1,  # Mon=1 .. Sun=7
            "day_name": current.strftime("%A"),
            "is_weekend": (current.weekday() >= 5),
            "is_holiday": False,
            "holiday_name": None,
        })

        current += timedelta(days=1)

    return pd.DataFrame(rows)


# -----------------------
# Routes
# -----------------------
@app.route("/")
def index():
    oauth = make_oauth()
    auth_url, state = oauth.authorization_url(AUTH_URL)
    session["oauth_state"] = state
    return f"""
    <h2>Mailchimp OAuth</h2>
    <p><a href="{auth_url}">Connect Mailchimp</a></p>
    <p>Redirect URI: <code>{REDIRECT_URI}</code></p>
    <p>Export since: <code>{get_since_time()}</code></p>
    """


@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return f"Missing code. Full query: {request.query_string.decode()}", 400

    oauth = make_oauth(state=session.get("oauth_state"))
    token = oauth.fetch_token(
        TOKEN_URL,
        client_secret=CLIENT_SECRET,
        code=code,
        include_client_id=True,
    )

    access_token = token["access_token"]
    dc, api_root = get_dc_and_api_root(access_token)

    # Helpful debug: who did we log in as?
    acct = None
    email = None
    try:
        root = mc_get(api_root, access_token, "/")
        acct = root.get("account_name")
        email = root.get("email")
    except Exception:
        pass

    session["mailchimp_account_name"] = acct
    session["mailchimp_email"] = email

    session["mailchimp_token"] = token
    session["mailchimp_dc"] = dc
    session["mailchimp_api_root"] = api_root

    return f"""
    <h3>Connected ✅</h3>
    <p><b>Account</b>: {acct or "(unknown)"}<br/>
       <b>Email</b>: {email or "(unknown)"}</p>
    <p>Now export since 2025-01-01:</p>
    <ul>
      <li><a href="/export_since2025">/export_since2025</a></li>
    </ul>
    """


@app.route("/export_since2025")
def export_since2025():
    token = session.get("mailchimp_token")
    api_root = session.get("mailchimp_api_root")
    if not token or not api_root:
        return redirect(url_for("index"))

    access_token = token["access_token"]
    since_send_time = get_since_time()
    since_dt = parse_iso_datetime(since_send_time)

    run_tag = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = os.path.join(EXPORT_DIR, run_tag)
    os.makedirs(out_dir, exist_ok=True)

    print("\n=== EXPORT START ===")
    print("since_send_time:", since_send_time)
    print("out_dir:", os.path.abspath(out_dir))

    try:
        # 0) sanity
        try:
            ping = mc_get(api_root, access_token, "/ping")
            print("ping:", ping)
            root = mc_get(api_root, access_token, "/")
            print("account_name:", root.get("account_name"))
            print("email:", root.get("email"))
        except Exception as e:
            print("sanity_check_error:", str(e))

        # A) dim_user.csv
        dim_user = pd.DataFrame([{
            "user_id": 1,
            "user_name": session.get("mailchimp_account_name") or "",
            "user_email": session.get("mailchimp_email") or "",
        }])
        dim_user_path = os.path.join(out_dir, "dim_user.csv")
        dim_user.to_csv(dim_user_path, index=False)
        print("wrote:", dim_user_path)

        # 1) lists -> dim_mailchimp_audience.csv
        lists = paginate_offset(api_root, access_token, "/lists", "lists", count=1000)
        print("lists:", len(lists))

        listid_to_surrogate = {lst.get("id"): i for i, lst in enumerate(lists, start=1) if lst.get("id")}

        dim_audience = pd.DataFrame([{
            "audience_id": listid_to_surrogate.get(lst.get("id")),
            "mailchimp_list_id": lst.get("id"),
            "audience_name": lst.get("name"),
            "created_at": lst.get("date_created"),
        } for lst in lists if lst.get("id")])

        dim_audience_path = os.path.join(out_dir, "dim_mailchimp_audience.csv")
        dim_audience.to_csv(dim_audience_path, index=False)
        print("wrote:", dim_audience_path)

        # 2) list growth-history -> fact_mailchimp_audience_monthly.csv
        fact_audience_rows = []
        for lst in lists:
            list_id = lst.get("id")
            if not list_id:
                continue
            gh = mc_get(api_root, access_token, f"/lists/{list_id}/growth-history")
            history = gh.get("history", [])
            print(f"growth-history list {list_id}: {len(history)} months")

            for h in history:
                month_str = h.get("month")  # "YYYY-MM"
                if not month_str or len(month_str) != 7:
                    continue
                ms = datetime.strptime(month_str + "-01", "%Y-%m-%d").date()

                new_subs = int(h.get("subscribed", 0) or 0)
                unsubs = int(h.get("unsubscribed", 0) or 0)
                cleaned = int(h.get("cleaned", 0) or 0)
                netchange = new_subs - unsubs - cleaned

                fact_audience_rows.append({
                    "month_start_date": ms.isoformat(),
                    "user_id": 1,
                    "audience_id": listid_to_surrogate.get(list_id),
                    "subscribers_total": h.get("existing"),
                    "subscribers_netchange": netchange,
                    "new_subscribers": new_subs,
                    "unsubscribes": unsubs,
                    "cleaned": cleaned,
                })
            time.sleep(0.15)

        fact_aud = pd.DataFrame(fact_audience_rows)
        if not fact_aud.empty:
            fact_aud.insert(0, "audience_monthly_id", range(1, len(fact_aud) + 1))

        fact_aud_path = os.path.join(out_dir, "fact_mailchimp_audience_monthly.csv")
        fact_aud.to_csv(fact_aud_path, index=False)
        print("audience_monthly_rows:", len(fact_aud))
        print("wrote:", fact_aud_path)

        # 3) campaigns since 2025-01-01:
        # Pull recent campaigns first, then filter locally (reliable).
        all_campaigns = paginate_offset(
            api_root,
            access_token,
            "/campaigns",
            "campaigns",
            base_params={"sort_field": "send_time", "sort_dir": "DESC"},
            count=500,
            sleep_s=0.2,
        )
        print("campaigns_pulled_total:", len(all_campaigns))

        campaigns: List[Dict[str, Any]] = []
        for c in all_campaigns:
            if c.get("status") != "sent":
                continue
            st = parse_iso_datetime(c.get("send_time"))
            if not st:
                continue
            if since_dt and st >= since_dt:
                campaigns.append(c)

        print("campaigns_after_filter_since2025:", len(campaigns))
        for i, c in enumerate(campaigns[:5]):
            print(i, c.get("id"), c.get("status"), c.get("send_time"), (c.get("settings") or {}).get("title"))

        campid_to_surrogate = {c.get("id"): i for i, c in enumerate(campaigns, start=1) if c.get("id")}

        dim_campaign = pd.DataFrame([{
            "campaign_id": campid_to_surrogate.get(c.get("id")),
            "mailchimp_campaign_id": c.get("id"),
            "campaign_name": (c.get("settings", {}) or {}).get("title") or c.get("campaign_title"),
            "campaign_type": c.get("type"),
            "status": c.get("status"),
            "send_time": c.get("send_time"),
            "subject_line": (c.get("settings", {}) or {}).get("subject_line"),
            "from_name": (c.get("settings", {}) or {}).get("from_name"),
            "reply_to": (c.get("settings", {}) or {}).get("reply_to"),
            "archive_url": c.get("archive_url"),
        } for c in campaigns if c.get("id")])

        dim_campaign_path = os.path.join(out_dir, "dim_mailchimp_campaign.csv")
        dim_campaign.to_csv(dim_campaign_path, index=False)
        print("wrote:", dim_campaign_path)

        # 4) reports + monthly agg -> fact_mailchimp_campaign_monthly.csv
        reports: List[Dict[str, Any]] = []
        report_errors = 0
        for c in campaigns:
            cid = c.get("id")
            if not cid:
                continue
            try:
                reports.append(mc_get(api_root, access_token, f"/reports/{cid}"))
            except Exception as e:
                report_errors += 1
                print("report_error:", cid, str(e))
            time.sleep(0.15)

        print("reports_ok:", len(reports), "reports_err:", report_errors)

        raw_rows = []
        for c in campaigns:
            cid = c.get("id")
            send_dt = parse_iso_datetime(c.get("send_time"))
            if not cid or not send_dt:
                continue

            mstart = month_start(send_dt.date()).isoformat()

            recipients = c.get("recipients", {}) or {}
            list_id = recipients.get("list_id")
            audience_id = listid_to_surrogate.get(list_id)

            r = next((x for x in reports if x.get("id") == cid), None)
            if not r:
                continue

            raw_rows.append({
                "month_start_date": mstart,
                "user_id": 1,
                "audience_id": audience_id,
                "emails_sent": int(r.get("emails_sent", 0) or 0),
                "opens_total": int(safe_get(r, ["opens", "opens_total"], 0) or 0),
                "unique_opens": int(safe_get(r, ["opens", "unique_opens"], 0) or 0),
                "clicks_total": int(safe_get(r, ["clicks", "clicks_total"], 0) or 0),
                "unique_clicks": int(safe_get(r, ["clicks", "unique_clicks"], 0) or 0),
                "bounces_hard": int(safe_get(r, ["bounces", "hard_bounces"], 0) or 0),
                "bounces_soft": int(safe_get(r, ["bounces", "soft_bounces"], 0) or 0),
                "unsubscribes": int(r.get("unsubscribed", 0) or 0),
                "abuse_reports": int(r.get("abuse_reports", 0) or 0),
                "open_rate": safe_get(r, ["opens", "open_rate"], None),
                "click_rate": safe_get(r, ["clicks", "click_rate"], None),
                "click_to_open_rate": safe_get(r, ["clicks", "click_to_open_rate"], None),
            })

        df_raw = pd.DataFrame(raw_rows)

        if df_raw.empty:
            df_month = pd.DataFrame(columns=[
                "campaign_monthly_id", "month_start_date", "user_id", "audience_id",
                "emails_sent", "opens_total", "unique_opens", "clicks_total", "unique_clicks",
                "bounces_hard", "bounces_soft", "unsubscribes", "abuse_reports",
                "open_rate", "click_rate", "click_to_open_rate"
            ])
        else:
            for col in ["open_rate", "click_rate", "click_to_open_rate"]:
                df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

            group_cols = ["month_start_date", "user_id", "audience_id"]
            sum_cols = [
                "emails_sent", "opens_total", "unique_opens", "clicks_total", "unique_clicks",
                "bounces_hard", "bounces_soft", "unsubscribes", "abuse_reports"
            ]
            df_sum = df_raw.groupby(group_cols, dropna=False)[sum_cols].sum().reset_index()

            def wavg(g, col):
                w = g["emails_sent"]
                v = g[col]
                m = w.notna() & v.notna() & (w > 0)
                return float((v[m] * w[m]).sum() / w[m].sum()) if m.any() else None

            rate_rows = []
            for keys, g in df_raw.groupby(group_cols, dropna=False):
                rate_rows.append({
                    "month_start_date": keys[0],
                    "user_id": keys[1],
                    "audience_id": keys[2],
                    "open_rate": wavg(g, "open_rate"),
                    "click_rate": wavg(g, "click_rate"),
                    "click_to_open_rate": wavg(g, "click_to_open_rate"),
                })
            df_rate = pd.DataFrame(rate_rows)

            df_month = df_sum.merge(df_rate, on=group_cols, how="left")
            df_month.insert(0, "campaign_monthly_id", range(1, len(df_month) + 1))

        fact_campaign_path = os.path.join(out_dir, "fact_mailchimp_campaign_monthly.csv")
        df_month.to_csv(fact_campaign_path, index=False)
        print("campaign_monthly_rows:", len(df_month))
        print("wrote:", fact_campaign_path)

        # 5) rank -> fact_mailchimp_campaign_rank_monthly.csv
        rank_metrics = [
            "emails_sent", "unique_opens", "unique_clicks", "unsubscribes",
            "open_rate", "click_rate", "click_to_open_rate"
        ]

        rank_rows = []
        rid = 1
        if not df_month.empty:
            for (m, u), g_month in df_month.groupby(["month_start_date", "user_id"]):
                for metric in rank_metrics:
                    gg = g_month[["audience_id", metric]].copy()
                    gg = gg.rename(columns={metric: "metric_value"}).dropna(subset=["metric_value"])
                    if gg.empty:
                        continue
                    gg["rank_desc"] = gg["metric_value"].rank(method="dense", ascending=False).astype(int)
                    gg["rank_asc"] = gg["metric_value"].rank(method="dense", ascending=True).astype(int)
                    gg = gg[(gg["rank_desc"] <= 10) | (gg["rank_asc"] <= 10)].copy()

                    for _, row in gg.iterrows():
                        rank_rows.append({
                            "campaign_rank_monthly_id": rid,
                            "month_start_date": m,
                            "user_id": u,
                            "audience_id": int(row["audience_id"]) if pd.notna(row["audience_id"]) else None,
                            "metric_name": metric,
                            "metric_value": float(row["metric_value"]),
                            "rank_desc": int(row["rank_desc"]),
                            "rank_asc": int(row["rank_asc"]),
                        })
                        rid += 1

        df_rank = pd.DataFrame(rank_rows)
        rank_path = os.path.join(out_dir, "fact_mailchimp_campaign_rank_monthly.csv")
        df_rank.to_csv(rank_path, index=False)
        print("rank_rows:", len(df_rank))
        print("wrote:", rank_path)

        # B) dim_date.csv (2025-01-01 -> today UTC)
        start_date = date(2025, 1, 1)
        end_date = datetime.utcnow().date()
        dim_date = generate_dim_date(start_date, end_date)

        dim_date_path = os.path.join(out_dir, "dim_date.csv")
        dim_date.to_csv(dim_date_path, index=False)
        print("dim_date_rows:", len(dim_date))
        print("wrote:", dim_date_path)

        print("=== EXPORT END ===\n")

        return {
            "status": "ok",
            "since_send_time_used": since_send_time,
            "output_dir": os.path.abspath(out_dir),
            "files": [
                "dim_user.csv",
                "dim_date.csv",
                "dim_mailchimp_audience.csv",
                "dim_mailchimp_campaign.csv",
                "fact_mailchimp_audience_monthly.csv",
                "fact_mailchimp_campaign_monthly.csv",
                "fact_mailchimp_campaign_rank_monthly.csv",
            ],
            "counts": {
                "lists": int(len(lists)),
                "campaigns_pulled_total": int(len(all_campaigns)),
                "campaigns_after_filter_since2025": int(len(campaigns)),
                "audience_monthly_rows": int(len(fact_aud)),
                "campaign_monthly_rows": int(len(df_month)),
                "rank_rows": int(len(df_rank)),
                "dim_date_rows": int(len(dim_date)),
            }
        }

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(tb)
        return {"status": "error", "error": str(e), "traceback": tb}, 500


if __name__ == "__main__":
    # Run: python app.py
    # Open: http://127.0.0.1:8000/
    app.run(host="127.0.0.1", port=8000, debug=True)
