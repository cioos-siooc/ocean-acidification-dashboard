# Profiler casts

[All pages](README.md) · Previous: [Cross-section](07-cross-section.md) · Next: [Analysis overview](09-analysis-overview.md)

Most sensors measure at one fixed depth. A **profiler** moves up and down the water column and measures as it goes,
so each trip, or *cast*, records a full profile. OceanECO shows profiler data over time and depth, and compares it
with the model.

## Sensor depth

1. Select a profiling sensor on the map or in the sensor list (see [Find a sensor](04-find-a-sensor.md)).
2. Open the **Sensor depth** tab in the bottom panel. It is greyed out until a profiling sensor is selected.

The view is a heatmap like [Model depth](06-time-depth.md), with time across and depth down, but filled with the
sensor's measurements.

## 1H and 1D

- **1H** shows each cast as a thin column. The gaps are the hours between casts. This is the raw record, best for
  looking at single casts.
- **1D** averages each day's casts into one column per day. This is easier to read over a whole year.

Days with no casts stay empty.

## Colours

The heatmap uses the Field's colours. Set a range in **Color Settings** that fits the water at this station, since
the default range covers the whole Salish Sea.

## Depth sections in Comparison

1. Click **Comparison** in the bottom panel (see [Model vs. sensor](05-model-vs-sensor.md)).
2. Open the **Depth sections** tab.

The model is shown above the sensor's casts, on the same time and depth axes and the same colour scale, so you can
see whether the model has the same layers at the same times.

**Click a cell** in the model panel to compare the two at that depth. The statistics at the top (number of matched
values, bias, r and RMSE) then cover that depth level only, over every day with a cast.

## Download

- In **Sensor depth**, **Download** saves the sensor's depth grid as CSV.
- In **Depth sections**, **Download** saves both grids, model and sensor.

## Worked example: summer layers in Hood Canal

ORCA1 at Twanoh, in Hood Canal, casts from 1 to 30 m every six hours.

1. Keep **Field** on *Temperature*, search for *Twanoh* and select **ORCA1 Twanoh**.
2. Open **Sensor depth**. Go to early September (12 September 2026) in **1H**: each cast is a thin column.
3. Switch to **1D** and set the colour range to **9.5 – 18 °C**.
4. Read the year (November 2025 – November 2026):
   - In winter, the water is mostly 10 – 11.5 °C from top to bottom. Cold spells chill the top few metres to 8 –
     10 °C.
   - In summer, a warm layer forms in the top few metres, about 16 – 17 °C and up to 21 °C, over about 11 °C below
     20 m.
5. Open **Comparison** → **Depth sections**. Both model and sensor show the warm summer layer.
6. Click a model cell at about 11 m. The statistics now compare model and casts at that depth.
