# Kavi Global Power BI Dashboard

A social media analytics pipeline and Power BI dashboard for tracking performance across YouTube, LinkedIn, Mailchimp, Dripify, and Facebook. Data is extracted from platform APIs, loaded into an Azure SQL data warehouse, and visualized in Power BI.

## Project Structure

```
kavi_global_powerbi/
├── youtube/                          # YouTube ETL pipeline
│   ├── youtube_ETL.py                # Full historical backfill (all videos, all time)
│   ├── youtube_daily_refresh.py      # Incremental daily refresh
│   ├── youtube_public_api_test.py    # Quick public API pull (no OAuth needed)
│   ├── youtube_sql_connection.py     # Loads CSV snapshots into Azure SQL staging
│   ├── youtube_token_connection.py   # OAuth setup step 1: generate authorization URL
│   ├── youtube_token_collection.py   # OAuth setup step 2: exchange code for refresh token
│   └── .env.example                  # Template for required environment variables
│
├── code for mailchimp/               # Mailchimp API integration
│   ├── mc.py                         # Mailchimp data extraction script
│   ├── connect_DA.ipynb              # Exploratory notebook
│   └── requirements.txt              # Python dependencies
│
├── schema/                           # Azure SQL schema definitions
│   ├── Youtube.sql                   # YouTube data warehouse schema
│   ├── LinkedIn.sql                  # LinkedIn analytics schema
│   ├── mailchimp.sql                 # Mailchimp schema
│   ├── facebook.sql                  # Facebook schema
│   └── Dripify.sql                   # Dripify automation schema
│
├── Social_media_API_01 (1) (1).pbix  # Power BI data model
├── KPI Doc.docx                      # KPI definitions and documentation
├── Selected fields for apis.xlsx     # API field mappings reference
├── api search.xlsx                   # API endpoint research
├── Final_Presentation.pptx           # Final capstone presentation
├── KaviGlobal_Practicum_Progress.pptx
├── .env.example                      # Root-level env variable template
└── .gitignore
```

## YouTube ETL Pipeline

The YouTube pipeline pulls data from the YouTube Data API and YouTube Analytics API into an Azure SQL data warehouse (`dw_youtube` + `dw_shared` schemas).

### How it works

**One-time setup** — run these once to get your OAuth refresh token:
1. `youtube_token_connection.py` — opens a Google authorization URL in your browser
2. `youtube_token_collection.py` — exchanges the returned auth code for a refresh token; copy the `refresh_token` value into your `.env`

**Historical backfill** — run once to load all historical data:
```bash
python youtube/youtube_ETL.py
```
This pulls every video and all available daily analytics, chunked by month with checkpointing so it can be safely interrupted and resumed.

**Daily refresh** — run on a schedule to keep the warehouse current:
```bash
python youtube/youtube_daily_refresh.py
```

**Quick public API snapshot** — no OAuth required, uses an API key only:
```bash
python youtube/youtube_public_api_test.py
```

### Tables populated

| Schema | Table | Description |
|---|---|---|
| `dw_shared` | `dim_date` | Date dimension (year, quarter, month, week, holiday flags) |
| `dw_youtube` | `dim_channel` | Channel metadata |
| `dw_youtube` | `dim_video` | Video metadata (title, duration, tags, publish date) |
| `dw_youtube` | `dim_content_length` | Content length buckets (short / medium / long) |
| `dw_youtube` | `fact_youtube_video_daily` | Daily per-video metrics (views, watch time, impressions, engagement) |
| `dw_youtube` | `fact_youtube_channel_daily` | Daily channel-level metrics (views, subscribers gained/lost) |

### Environment variables

Copy `youtube/.env.example` to `youtube/.env` and fill in your values:

```
YOUTUBE_CLIENT_ID=...
YOUTUBE_CLIENT_SECRET=...
YOUTUBE_REFRESH_TOKEN=...
YOUTUBE_API_KEY=...
AZURE_SQL_SERVER=...
AZURE_SQL_DB=...
AZURE_SQL_USER=...
AZURE_SQL_PWD=...
```

Never commit your `.env` file — it's in `.gitignore`.

### Dependencies

```bash
pip install google-api-python-client google-auth pandas sqlalchemy pyodbc python-dotenv
```

## Database Setup

Schema files are in the `schema/` directory. Run them against your Azure SQL instance to create the required tables before running any ETL scripts.

## Power BI Dashboard

Open `Social_media_API_01 (1) (1).pbix` in Power BI Desktop and point the data source connections at your Azure SQL instance. Refresh the model to pull in the latest data from the warehouse.

---

**Project:** Kavi Global Social Media Analytics
**Last Updated:** March 2026
