# Time–depth view

[All pages](README.md) · Previous: [Model vs. sensor](05-model-vs-sensor.md) · Next: [Cross-section](07-cross-section.md)

The chart shows one depth over time, and the vertical profile shows every depth at one time. **Model depth** shows
both at once: the whole water column at your point, over time.

## Open Model depth

1. Click a point on the map.
2. Open the **Model depth** tab in the bottom panel.

The view is a heatmap with time across, depth down and the surface at the top. Each cell is coloured by the Field's
value at that time and depth. The depth axis is stretched near the surface, where the water changes most, and ends at
the seabed.

## Time resolution

The chart's time buttons apply here too:

- **1D:** one column per day over a year. Best for seasons.
- **1H:** one column per hour over two weeks. Shows tides and the daily cycle.

Use the dates to move the window (see [Move through time](02-move-through-time.md)).

## Colours

The heatmap uses the same colours as the map, so **Color Settings** and **Auto Color** change both. A
*diverging* palette, such as *balance*, with its middle at a meaningful value makes the heatmap easy to read: for
example a range of 0.5 – 1.5 for Omega puts the colour change at Ω = 1.

## Click a cell

Click a cell to go to that time and depth:

- The **vertical profile** shows that time's profile.
- The **map** moves to the same time and depth.

Hover over a cell to read its time, depth and value.

## Download

**Download** saves the depth grid as CSV: one value per time and depth.

## Worked example: corrosive water in the Strait of Georgia

Omega Aragonite (Ω) tells whether seawater can dissolve the aragonite shells of oysters, mussels and some
plankton. Below Ω = 1 the water is corrosive to shells.

1. Set **Field** to *Omega Aragonite*, and click the central Strait of Georgia at 49.20° N, 123.50° W.
2. Open **Model depth** and choose **1D**.
3. In **Color Settings**, choose the palette *balance*, press **Reverse**, and set the range to **0.5 – 1.5**. Red is
   now corrosive water (Ω below 1), and blue is water above Ω = 1.
4. Read the year (November 2025 – November 2026):
   - Below about 30 m, the water is corrosive all year.
   - From November to February, corrosive water reaches the surface.
   - From April to September, the top layer is mostly above Ω = 1, with monthly mean surface Ω of 1.5 to 1.9, as
     plankton growth takes up carbon dioxide.
5. Click a cell in mid-January. The profile shows Ω below 1 from top to bottom, and the map shows corrosive water
   across the Strait.
6. Switch to **1H** and click a cell. Over two weeks, the Ω = 1 level moves between the surface and about 20 m, hour
   by hour.
