# Trends, anomalies & correlation

[All pages](README.md) · Previous: [Extreme events & compound stress](10-extremes-compound.md) · Next: [Share & download](12-share-download.md)

Three tabs of the **Analysis** workspace answer longer-term questions about the daily record since 2007: is it
changing (**Trend**), how does one year compare with a normal year (**Climatology Anomaly**), and which variables
move together (**Correlation**). To open Analysis, click a point on the map, then **Analysis → Model** in the left
panel (see [Analysis overview](09-analysis-overview.md)).

## Season applies to every tab

**Season** in the sidebar (**All**, **DJF**, **MAM**, **JJA**, **SON**) limits each tab to one part of the year. A
season can show a change that the whole year hides: a decline in autumn can be cancelled out by no change in other
seasons.

## Trend

**Trend** asks whether the Field is rising or falling over the years.

- **Chart:** dots are the mean of each year, and the dashed line is the fitted trend.
- **Theil-Sen slope:** how much the trend line changes per year, in the Field's units. It is a robust slope: a few
  unusual years don't pull it much.
- **Mann-Kendall significance:** tests whether the trend is real or could be chance. A **p** below 0.05 counts as
  a real trend, and the badge says **increasing**, **decreasing** or **no trend**.
- **Detail:** the test's numbers. **S** counts pairs of years where the later year is higher, minus pairs where it is
  lower. **Z** scales S to give p. **n** is the number of years.

> The current year counts only the days so far. In a seasonal trend, a partial season can sit high or low, so check
> the last dot.

## Climatology Anomaly

**Climatology Anomaly** shows each year against a normal year.

- **Chart:** one line per year. 0 is the average for that calendar day. Above 0 is higher than usual, and below 0 is
  lower.
- **Baseline window:** the average for a day pools ±5 days around it, from every year. A wider window gives a
  smoother baseline.
- **Legend:** click a year to pick it out from the others.

Hover to read a year's anomaly on any date.

## Correlation

**Correlation** checks which variables rise and fall together at this point.

- **Variables (2-4):** the variables to compare, the Field included: two to four in all.
- **Matrix:** each cell is the correlation **r** between two variables, from −1 to +1. Red means they rise together
  (towards +1), and blue means one rises as the other falls (towards −1). Near 0 there is little link.
- **Click a cell** to see its scatter: one dot per day since 2007, with a dashed best straight-line fit.

Correlation shows that two variables move together, not that one causes the other. Both may follow a third, such as
the season or the tide.

## Download

- **Trend:** **Download** saves the annual means and the trend as CSV.
- **Climatology Anomaly:** **Download** saves the daily anomalies and the day-of-year climatology as CSV.
- **Correlation:** **Download** saves the matrix and the daily series as CSV, for the Season shown.

## Worked example: shell-building conditions off Victoria

Omega Calcite (Ω) tells whether seawater can dissolve calcite shells. Below 1, it is corrosive to them.

1. Set **Field** to *Omega Calcite*, click the surface off Victoria (48.40° N, 123.20° W) and open
   **Analysis → Model**.
2. **Trend**, Season **All:** almost no change, a slope of −0.0011 a year with p = 0.92, so **no trend**.
3. Choose **SON**: autumn is **decreasing**, −0.0059 a year (about −0.06 a decade), p = 0.002. 2025 and 2026 are the
   lowest autumns since 2007, although 2026 counts September, October and early November only so far.
4. Back on **All**, open **Climatology Anomaly** and click **2026** in the legend. 2026 ran below normal on 281 of its
   308 days so far. In mid-July it fell 0.54 below normal, the lowest of any day since 2007.
5. Open **Correlation** and add *pH (Total Scale)*, *Dissolved Inorganic Carbon* and *Salinity*:
   - pH: r = 0.93. Omega Calcite rises and falls with pH.
   - Dissolved Inorganic Carbon: r = −0.83. More carbon means lower Omega.
   - Salinity and carbon: r = 0.96. Salty water from the deep carries more carbon.
6. Click the Omega Calcite – carbon cell to see its scatter.
7. Choose **DJF**: in winter, carbon and salinity matter less (carbon r = −0.63, salinity −0.40), while pH is still
   0.95. In **MAM** the links are strongest: pH 0.98, carbon −0.83.
