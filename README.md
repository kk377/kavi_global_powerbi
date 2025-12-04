# Kavi Global Power BI Dashboard

A comprehensive Power BI analytics solution for tracking and analyzing social media performance across multiple platforms including LinkedIn, YouTube, and Dripify, and so on automation metrics.

## 📊 Overview

This project will provide a unified analytics dashboard for monitoring social media engagement, content performance, and outreach automation metrics. The solution integrates data from multiple social media APIs and will present them in a cohesive Power BI dashboard.

## 🗂️ Project Structure

```
kavi_global_powerbi/
├── schema/                    # Database schema definitions
│   ├── Dripify.sql           # Dripify automation schema
│   ├── LinkedIn.sql          # LinkedIn analytics schema
│   └── Youtube.sql           # YouTube analytics schema
├── Social_media_API_01 (1) (1).pbix  # Main Power BI data model file
├── api search.xlsx           # API search and documentation
├── Selected fields for apis.xlsx  # API field mappings
├── KPI Doc.docx              # Key Performance Indicators documentation
└── [Capstone] Final Presentation.pptx  # Project presentation
```

## 🚀 Getting Started

### Prerequisites
- Power BI Desktop (latest version recommended)
- Access to social media APIs:
  - LinkedIn API
  - YouTube Data API 
  - Dripify API
  - Twitter API
  - Facebook API
  - Apollo API
  - Google Analytics API
   
- Database system (PostgreSQL) for data warehouse

### Setup Instructions

1. **Database Setup**
   - Create a new database for the data warehouse
   - Execute the SQL schema files in the `schema/` directory:
     ```sql
     -- Run in order:
     -- 1. Create dim_date table (common across all schemas)
     -- 2. Execute Dripify.sql
     -- 3. Execute LinkedIn.sql
     -- 4. Execute Youtube.sql
     ```

2. **API Integration**
   - Review `api search.xlsx` for API endpoint documentation
   - Check `Selected fields for apis.xlsx` for field mappings
   - Configure API connections 

3. **Power BI Dashboard**
   - Open `Social_media_API_01 (1) (1).pbix` in Power BI Desktop
   - Load data source into Power BI 
   - Refresh the data model
   - Review and customize visualizations as needed (future work)


**Last Updated**: 12/4/2025
**Project**: Kavi Global Social Media Analytics
**Version**: 1.0

