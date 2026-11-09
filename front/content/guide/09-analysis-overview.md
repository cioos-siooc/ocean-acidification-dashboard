# Analysis overview

[All pages](README.md) · Previous: [Profiler casts](08-profiler-casts.md) · Next: [Extreme events & compound stress](10-extremes-compound.md)

The **Analysis** workspace opens the full model record at one point, every day since 2007, and gives you tools to
study it: seasons, records, extreme events, trends, anomalies and correlations. This page covers opening it and
its **Overview** tab.

## Open Analysis

1. Choose a **Field** and **Depth**, and click a point on the map.
2. Under **Analysis** in the left panel, click **Model**.

**Sensor** does the same for a selected sensor's record. Analysis opens full screen. Close it with the close button
or **Esc**.

## Tabs

| Tab | What it does | Page |
|---|---|---|
| **Overview** | Every year on one calendar | this page |
| **Extreme Events** | Finds unusually warm or cold spells | [Extreme events](10-extremes-compound.md) |
| **Compound Stress** | Counts days when two conditions hold at once | [Compound stress](10-extremes-compound.md#compound-stress) |
| **Trend** | Is it rising or falling over the years? | [Trends](11-trend-anomaly-correlation.md) |
| **Climatology Anomaly** | Each year against a normal year | [Anomalies](11-trend-anomaly-correlation.md#climatology-anomaly) |
| **Correlation** | Which variables move together | [Correlation](11-trend-anomaly-correlation.md#correlation) |

The settings in the left sidebar, such as **Season** and **Statistic**, carry over between tabs.

## Overview: every year on one calendar

The Overview chart puts every year since 2007 on one January-to-December axis.

- **Grey band:** the range of past years on each date.
- **White line:** their mean, the typical year.
- **Orange line:** the current year.
- **Legend:** click a year to add its line, and click it again to remove it.

Hover over the chart to read each line's value on a date.

## Season

**Season** keeps only part of the year: **All**, **DJF** (December – February), **MAM** (March – May), **JJA** (June
– August) or **SON** (September – November). The chart zooms to that season, and its y-axis rescales to fit, so check
the axis numbers when you compare seasons.

## Statistic

Each day in the record is built from 24 hourly values. **Statistic** chooses which one represents the day:

- **Mean:** the day's average.
- **Min:** the day's lowest hour.
- **Max:** the day's highest hour.

Use Min or Max to look at extremes that a daily average smooths out.

## All-time Records

**All-time Records** (top right) give the highest and lowest values in the record, with their dates, for the Season
and Statistic chosen.

## Download

**Download** saves the daily series and the band as CSV.

## Worked example: carbon through the seasons off Comox

Dissolved Inorganic Carbon (DIC) is the carbon dioxide dissolved in seawater. Plankton take it up as they grow.

1. Set **Field** to *Dissolved Inorganic Carbon*, click the northern Strait of Georgia off Comox (49.60° N,
   124.60° W), and open **Analysis → Model**.
2. In winter, surface water holds about 1970 mmol/m³. By midsummer it falls to about 1735, as plankton growth takes
   up the carbon.
3. The orange line is 2026: most of its summer stayed above the mean.
4. Choose **JJA** and click **2012** in the legend. 2012 was the lowest summer on record: **All-time Records** show
   1388 on 23 July 2012.
5. Choose **Min**. The record low is now 1305: the lowest hour, also on 23 July 2012.
6. Back on **Mean**, choose **DJF**. Note the axis: the winter chart spans only 1750 to 2100. Winters look alike from
   year to year, and nine years in ten stay between about 1920 and 2020. The lowest winter day, 1772, is far above
   the summer low of 1388.
