# Extreme events & compound stress

[All pages](README.md) · Previous: [Analysis overview](09-analysis-overview.md) · Next: [Trends, anomalies & correlation](11-trend-anomaly-correlation.md)

Two tabs of the **Analysis** workspace look for unusual conditions in the daily record since 2007. **Extreme Events**
finds spells when one variable is unusually high or low. **Compound Stress** counts days when two conditions hold at
once. To open Analysis, click a point on the map, then **Analysis → Model** in the left panel (see
[Analysis overview](09-analysis-overview.md)).

## Extreme Events

### Settings

| Setting | Meaning |
|---|---|
| **Threshold Mode** | How a day is judged unusual. **Percentile** compares each day with past years on the same dates: a day counts when it is beyond 90% of them. **Fixed value** uses one number for every date. |
| **Direction** | **Above** finds high spells, such as warm spells. **Below** finds low ones, such as cold spells. |
| **Baseline window** (Percentile) | What "the same dates" means: by default ±5 days around each date, in every year. A wider window gives a smoother threshold. |
| **Fixed threshold** (Fixed value) | The value to cross, in the Field's units. |
| **Min duration** | Spells shorter than this many days are ignored. |
| **Max merge gap** | Spells this many days apart or less count as one. |

Percentile mode finds what is unusual *for the time of year*, such as a warm spell in winter. Fixed value finds what
crosses a level that matters on any date, such as a temperature a species can't tolerate.

### Reading the results

- **Chart:** the daily series. The dashed line is the threshold for each date, and the shaded areas are the spells
  found. Drag the slider handles under the chart to zoom into a period.
- **Events table:** every spell, strongest first, with its start, end, length in days and peak value.
  **Peak Δ** is how far the peak was beyond the usual for that date.
- **Per-year summary:** the number of spells and spell days in each year. Use it to see whether events are becoming
  more frequent.

Changing a setting updates the chart, the table and the summary at once. Try raising **Min duration** to keep only
long spells, or raising **Max merge gap** to join spells split by a short break.

## Compound Stress

Organisms often cope with one stress at a time but not two together. **Compound Stress** counts days when two
conditions hold at once.

### Settings

- **Primary variable:** the Field you are analysing. Choose **Above** or **Below**, then the value.
- **Compare against:** a second variable, with its own direction and value.

For example, *Temperature above 18 °C and Omega Aragonite below 1* asks how often water is hot and corrosive at once.

### Reading the results

- **Chart:** the days when both conditions hold are shaded.
- **Table, one row per year:** **Days** is the number of days when both hold. **Streak** is the longest run of such
  days in a row.

## Download

- Extreme Events: **Download** saves the events list and the per-year summary as CSV.
- Compound Stress: **Download** saves the daily series and the per-year counts as CSV.

## Worked example: the 2021 heat dome in Saanich Inlet

1. Keep **Field** on *Temperature*, click the surface of Saanich Inlet (48.65° N, 123.50° W) and open
   **Analysis → Model**.
2. Open **Extreme Events**, with **Percentile**, **Above**, a ±5 day Baseline window, Min duration 5 and Max merge
   gap 2.
3. Drag the slider handles to zoom into 2021. The top spell in the table is the 2021 heat dome: 25 June – 13 July,
   19 days, peak 23.6 °C. Its **Peak Δ** is 6.3 °C: that much warmer than a typical late June.
4. Switch to **Fixed value** and set the **Fixed threshold** to **21 °C**. The **Per-year summary** shows four spells
   above 21 °C in 2021, 23 days in all. No other year had more than one.
5. Raise **Min duration** to **7**: only spells of a week or more remain, and 2021 keeps one (27 June – 4 July).
   Put it back to 5.
6. Raise **Max merge gap** to **5**: 2021's four spells join into two (27 June – 13 July and 1 – 15 August), because
   dips below 21 °C of up to 5 days no longer split them.
7. Open **Compound Stress**. Set the primary condition to **Above 18**, **Compare against** *Omega Aragonite*,
   **Below 1**. The result is zero days every year: warm surface water here was never corrosive.
8. Change the primary condition to **Below 10**. Corrosive surface water comes with the cold, every winter from about
   November to February: 89 days in 2021, the longest run 51 days. Here, heat and corrosive water strike at different
   times of year.
