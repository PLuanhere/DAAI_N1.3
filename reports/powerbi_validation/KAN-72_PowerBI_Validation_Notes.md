# KAN-72 — Power BI Dashboard Validation Evidence

## Scope and provenance

- Project: DAAI_N1.3 / Customer Atlas — Problem 3.
- SSAS connection indicated in screenshots: Live Connection.
- Images are **actual Power BI screenshots** provided by the project team on 2026-10-23, not generated examples.
- Evidence folder tracks *interactive checks*. README dashboard preview images are a separate artifact and are not replaced.

## Dashboard 01 — Customer Overview

- `KAN-72_PBI_Overview_All.png`: Year = All. Customer Count 121,930; Buyer Count 86,333; Repeat Rate 73.59%; Order Count 544,066; Net Sales about 13.19bn; AOV 24,236.18 (display values).
- `KAN-72_PBI_Overview_2018_FullPeriodTrend.png`: Year = 2018. Historical customer KPIs unchanged; Order Count 58,539; Net Sales about 1.49bn; AOV 25,465.74. Annual Net Sales Trend remains full period by design.
- Observation: Sales KPIs respond to Year as expected. Historical customer KPIs and historical-segment revenue are intentionally all-time. Year 2018 order count and value were separately reconciled in SSAS MDX.

## Dashboard 04 — When & What They Buy

- `KAN-72_PBI_TimeCategory_2018_FixedInsight.png`: Year = 2018; Customer Type = All; Category = All. Peak month 2018-06 (9,205 orders), peak weekday Tuesday (9,423 orders), top category Streetwear (82.01%).
- `KAN-72_PBI_TimeCategory_2019_FixedInsight.png`: Year = 2019; Customer Type = All; Category = All. Peak month 2019-06 (4,000 orders), peak weekday Wednesday (5,473 orders), top category Streetwear (80.99%).
- Observation: KPI cards, time/category charts, and customer-type × category matrix respond to Year. Static Business Insight has been replaced by reporting-scope guidance that does not assert year-specific numbers.

## Verification status and limitations

| Check | Evidence status |
| --- | --- |
| Dashboard 01: Year All vs 2018 | Observed in screenshots |
| Dashboard 04: Year 2018 vs 2019 | Observed in screenshots |
| Dashboard 04: Customer Type Repeat vs One-time | Not yet evidenced here |
| Dashboard 04: Category filter behavior | Not yet evidenced here |
| Dashboard 00, 02, 03: interactive validation | Not yet evidenced here |
| Power BI source package and SSAS deployment reproducibility | Requires separate source/environment checks |

Numbers are transcribed from visual screenshots and should not be presented as a full independent automated test. Do not claim all five dashboards passed functional testing based on these four screenshots alone.
