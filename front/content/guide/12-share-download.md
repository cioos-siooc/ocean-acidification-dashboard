# Share & download

[All pages](README.md) · Previous: [Trends, anomalies & correlation](11-trend-anomaly-correlation.md)

**Share** sends someone exactly the view you see. **Download** keeps the numbers behind it.

## Share

**Share** in the top bar makes a link to your current view.

### What the link keeps

- The **Field**, **depth** and **time** on the map.
- The **colour range** and palette.
- The **map position** and zoom, with **Map Labels** and **Bathymetry Contours**.
- The **point you clicked**, and the chart's time window.
- The **open panel** and its settings, a **selected sensor**, or a **drawn cross-section line**.
- Your **sensor filters**.

**Copy link** puts it on the clipboard, to paste into an email, a chat or a report.

The link is a snapshot of that moment. If you change the view afterwards, make a new link.

### Opening a link

Whoever opens the link sees your view, with nothing to set up again: the same Field, depth, time, colours, map
position, labels, point and chart. Without a link, the dashboard opens on its default view: today's temperature.

### Share in Analysis and Comparison

The full-screen workspaces have their own **Share** icon, since they cover the one in the top bar. Their links also
keep the tab and its settings, such as **Season** and **Statistic** in Analysis, so the workspace reopens exactly as
shared.

## Download

**Download** saves the data behind a chart or a section as a CSV file. Each view saves what it shows:

| View | The file holds |
|---|---|
| Chart (Timeseries) | The model, a selected sensor and the Climatology, at the chart's resolution |
| Model depth / Sensor depth | The depth grid |
| Cross-Section | The section along the line |
| Comparison | The matched model and sensor values (Depth sections: both grids) |
| Analysis tabs | What the tab shows: for example the events list, per-year counts or the correlation matrix |

### The file name

The name says what the file holds: the variable, the point, the depth and the dates.

### Inside the file

Open it in a spreadsheet or a text editor.

1. **Lines starting with `#`** say where the numbers come from: the source, the variable, the depth, the exact point
   and the time range.
2. **The header row** has one column per series on the chart, for example time, model, and the Climatology mean, min
   and max.
3. **The second row** gives each column's units.
4. **Then one row per time step.** Times are in Pacific time, with their offset from UTC. Climatology has one value a
   day, so in an hourly file most hours leave it blank.

Values are at full precision, not rounded as on screen. Most data tools can skip the `#` lines as comments and read
the units row separately.

### The legend decides the columns

The file follows the chart's legend. Hide a series, such as Climatology, by clicking it in the legend, and it is left
out of the download. Hide what you don't need before you download.

## Worked example: sharing the Fraser River plume

1. Set up the plume view from [Map tools](03-map-tools.md): **Salinity**, 0.5 m, 4 June 2026 at 16:30, zoomed on
   the Fraser delta with **Map Labels** on and a point clicked at 49.20° N, 123.35° W.
2. Press **Share**, then **Copy link**.
3. Open the link in a new browser window. The view comes back: Salinity at 0.5 m on 4 June, 16:30, with the same
   colour range, map position, labels, point and two-week chart.
4. Press **Download**. The file has every hour of the two weeks, with the model and the Climatology mean, min and
   max.
5. Click **Climatology** in the legend to hide it, and download again: the file now holds only the model.
6. Open **Analysis → Model**, choose **JJA** and **Min**: the freshest water of each summer day. Press Analysis's
   **Share** icon. The link reopens Analysis on the same tab, Season and Statistic.
