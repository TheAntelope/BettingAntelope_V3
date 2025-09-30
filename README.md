# BettingAntelope_V3
notebooks running in sagemaker - scripts running in lambda functions - supabase database - front end which I will deply with vercel vo.

## Services

- `services/scrape-depth-charts` – AWS Lambda + EventBridge rule that scrapes ESPN NFL depth chart data every four hours and upserts the results into the `DepthCharts` table in Supabase.
