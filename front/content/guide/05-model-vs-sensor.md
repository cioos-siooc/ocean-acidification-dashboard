# Model vs. sensor

[All pages](README.md) · Previous: [Find a sensor](04-find-a-sensor.md) · Next: [Time–depth view](06-time-depth.md)

How well does the model match the real ocean? The **Comparison** workspace puts the model next to a sensor's
measurements, over the sensor's whole record, with statistics and several views.

## Open Comparison

1. Select a sensor on the map or in the sensor list (see [Find a sensor](04-find-a-sensor.md)).
2. Click **Comparison** in the bottom panel, or the chart icon on the sensor's card.

Comparison opens full screen for the current Field. It uses the model at the level nearest the sensor's depth, and
compares only times when both have a value: the badge shows how many matched days there are. Close it with the close
button or **Esc**.

## Timeseries

**Timeseries** shows the model and the sensor over the whole record, one point per day. Orange is the model and
green is the sensor.

### Summary Stats

| Statistic | Meaning |
|---|---|
| **Bias** | The average of model minus sensor. Positive means the model reads high. |
| **RMSE** | Root-mean-square error: the typical size of the difference, in the Field's units. |
| **Pearson R** | How closely the two rise and fall together, from −1 to 1. 1 is a perfect match. |

## Season

**Season** limits the comparison to part of the year: **All**, **DJF** (winter), **MAM** (spring), **JJA** (summer) or
**SON** (autumn). The statistics are recalculated for that season only. The model may be better in some seasons than
others.

## Time resolution: 1D and 1H

- **1D** compares daily means over the whole record.
- **1H** compares the last 14 days, hour by hour. Hourly data takes a moment to fetch.

## Scatter

**Scatter** plots one dot per day, with the sensor across and the model up. Dots on the dashed 1:1 line agree. A
cloud above the line means the model reads high, and a cloud below it means the model reads low.

## Residuals

**Residuals** plots model minus sensor, day by day. Zero is perfect agreement. Look for spikes, which often come with
sudden changes the model times slightly differently, and for drifts that suggest a slow difference.

## Seasonal Cycle

**Seasonal Cycle** shows the average year: for each month, the mean of the model and of the sensor. It shows whether
the model gets the timing and size of the seasons right.

## Download

**Download** saves the matched model and sensor values as CSV.

## Worked example: the deep Strait of Georgia

1. Keep **Field** on *Temperature* and select the **Strait of Georgia VENUS Instrument Platform** (299 m).
2. Open **Comparison**. The **Timeseries** shows thirteen years of daily temperature at 299 m, over 4,200 matched
   days.
3. Most summers, colder water renews the deep basin, and both model and sensor drop by about 1 °C.
4. **Summary Stats:** bias +0.04 °C, RMSE 0.24 °C, Pearson R 0.84. The model differs from the sensor by about a
   quarter of a degree on average.
5. In **1H**, over the last two weeks the two stay within about 0.1 °C.
6. **Residuals** are usually within ±0.25 °C. The spikes come with sudden changes.
7. **Seasonal Cycle:** at 299 m the water is coldest in May and warmest in October, and model and sensor both peak in
   October. Deep water lags the surface by months.
