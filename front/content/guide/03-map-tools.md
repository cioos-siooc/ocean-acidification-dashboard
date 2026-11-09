# Map tools

[All pages](README.md) · Previous: [Move through time](02-move-through-time.md) · Next: [Find a sensor](04-find-a-sensor.md)

The toolbar on the left edge of the map has tools for finding places, reading values and controlling the view. Hover
over the toolbar to see each button's name, and click a button again to turn it off.

## Map Labels

**Map Labels** adds the names of places and bodies of water to the map. Use it to find your way around before you
click a point.

## Cursor Coordinates

**Cursor Coordinates** shows the latitude and longitude under the mouse. Use it to find a spot from its
coordinates: move the mouse until the box shows them, then click.

## Cursor Value

**Cursor Value** shows the value of the Field under the mouse, at the depth and time the map shows. You don't need
to click, so you can sweep the mouse across a feature and read how it changes. Turn on Cursor Coordinates as well to
see value and position together.

## Vertical Profile

The vertical profile at the top right shows the Field from the surface to the seabed at the point you clicked, at the
map's time. **Vertical Profile** in the toolbar hides it, to see more of the map, or shows it again.

## Playback speed

**Play** steps the map forward one hour, day or month at a time, depending on whether the chart is in 1H, 1D or 1M.
The **speed** button beside it sets the pace:

| Speed | Steps per second |
|---|---|
| 0.5x | one every 2 s |
| 1x | one |
| 2x | two |

In 1H, Play shows tides and daily cycles hour by hour.

## Other toolbar buttons

- **Model Layer** and **Sensor Layer** show or hide the model colours and the sensor stations (see
  [Find a sensor](04-find-a-sensor.md)).
- **Bathymetry Contours** draws seafloor depth contours (see [Cross-section](07-cross-section.md)).
- **Auto Color** and **Color Settings** set the colours (see [Getting started](01-getting-started.md#colours)).

## Worked example: the Fraser River plume

Each June the Fraser River runs high with snowmelt and spreads a layer of fresh water over the Strait of Georgia.

1. Set **Field** to *Salinity* and zoom in on the Fraser delta.
2. Turn on **Map Labels** to find the river mouth.
3. Turn on **Cursor Coordinates** and click the water at 49.20° N, 123.35° W, just off the delta.
4. Click the dates, go to **5 June 2026**, then click the chart at **4 June, 16:30** to put the map there. Press
   **Auto Color**: dark blue shows fresh river water spreading into the Strait.
5. Turn on **Cursor Value** and move the mouse from the river mouth out into the Strait. At the mouth, salinity is
   about 1 g/kg, almost fresh. Past the edge of the plume it jumps to about 26 g/kg. Deeper down, Strait water is
   about 31 g/kg.
6. Look at the profile: the fresh water is a thin layer. Salinity rises from about 8 to 26 g/kg in the top 6 m.
7. Press **Play** in 1H and raise the speed to **2x** to watch the plume move with the tides.
