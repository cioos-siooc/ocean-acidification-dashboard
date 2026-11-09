# Find a sensor

[All pages](README.md) · Previous: [Map tools](03-map-tools.md) · Next: [Model vs. sensor](05-model-vs-sensor.md)

Besides the model, OceanECO shows measurements from real instruments: buoys, seabed platforms and profilers run by
Ocean Networks Canada, ORCA and other groups. This page shows how to find a sensor and put its data on the chart.

## Sensor Layer and Model Layer

- **Sensor Layer** in the toolbar shows or hides the sensor stations on the map. A **green** station has recent data,
  and an **orange** one has older data only.
- **Model Layer** hides or shows the model colours. Hide them to see the sensors on a plain map.

## The sensor list

The sensor list in the left panel shows every station. Two controls narrow it down:

- **Filter sensors** (the filter button) keeps only sensors from one **Organization**, or only sensors that measure
  one **Variable**. The map shows the same stations as the list.
- **Search sensors** finds stations by name.

## Select a sensor

Click a station on the map, or its card in the list.

- The map and the chart move to the **nearest model level** to the sensor's depth, and a message says which level
  that is. For a seabed sensor at 299 m, for example, the nearest model level is 306.8 m.
- If the sensor measures the current Field, its data joins the chart.

## A sensor on the chart

With a sensor selected, the chart shows two series:

- **Green:** the sensor's measurements.
- **Pink:** the model at the nearest level.

The blue Climatology band still shows the model's usual range for each date. Switch to **1D** for a year of daily
means, which is easiest to read for a sensor (see [Move through time](02-move-through-time.md)).

To compare the two in detail, open **Comparison** (see [Model vs. sensor](05-model-vs-sensor.md)).

## Sensor details

**Sensor details** (the info button on the sensor's card) shows the station's location, depth, the dates it has data
for, and links to the original data.

## Download

With a sensor selected, **Download** saves both the model and the sensor values as CSV.

## Worked example: deep oxygen in the Strait of Georgia

The VENUS platform of Ocean Networks Canada sits on the floor of the Strait of Georgia at 299 m.

1. Set **Field** to *Dissolved Oxygen*.
2. Click **Filter sensors**, choose **Variable** and pick *Dissolved Oxygen*: only oxygen sensors stay.
3. Type *VENUS* in **Search sensors**. Two VENUS platforms appear.
4. Zoom in and click the **Strait of Georgia VENUS Instrument Platform** on the map. The map moves to the 306.8 m
   model level. Press **Auto Color**.
5. Switch the chart to **1D**. From November to early April, oxygen at 299 m stays near 100 mmol/m³, about a third
   of the surface value.
6. In mid-April, oxygen-rich water floods the deep Strait. This is a deep-water renewal, and the sensor and the model
   jump on the same day.
7. Open **Sensor details** to see where the data comes from.
