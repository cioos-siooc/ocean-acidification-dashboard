# Getting started

[All pages](README.md) · Next: [Move through time](02-move-through-time.md)

This page covers the controls you use everywhere in OceanECO: choosing a variable, the map and its toolbar, colours,
clicking a point, the chart, the vertical profile, depth, downloads and the server status.

## Choose what the map shows: Field

**Field** at the top of the left panel picks the variable on the map, for example Temperature, Salinity,
pH (Total Scale), Dissolved Oxygen or Omega Aragonite. Click it to open the list, or type part of a name to find
one quickly.

The map, the chart and the vertical profile all switch to the new Field.

## Move around the map

- **Scroll** to zoom in and out.
- **Drag** to move around.

## The toolbar

The toolbar runs down the left edge of the map. Hover over it to see each button's name. Click a button to turn it on
and click again to turn it off. The buttons come in three groups:

| Group | Buttons | What they do |
|---|---|---|
| **Layers** | Model Layer, Sensor Layer, Bathymetry Contours, Map Labels | Show or hide the model colours, the sensor stations, seafloor depth contours and place names |
| **Tools** | Cursor Coordinates, Cursor Value, Vertical Profile | Read the position or the value under the mouse; show or hide the vertical profile |
| **Colours** | Auto Color, Color Settings | Fit or set the colours |

The [Map tools](03-map-tools.md), [Find a sensor](04-find-a-sensor.md) and [Cross-section](07-cross-section.md) pages
cover the layer buttons in detail.

## Colours

### Color Settings

**Color Settings** opens a panel with these settings:

- **Palette:** the colours used for the values. Type a name in **Search palettes…** to find one, such as
  *rainbow* or *balance*, and click it.
- **Reverse:** flips the palette end to end, so high values take the colours low values had.
- **Invert:** uses the palette's inverted colours.
- **Range:** the lowest and highest values the colours stretch across. Type the two numbers, or drag the handles.
  Values beyond either end take the end colour. A range that doesn't fit the water hides its detail: if you
  drag the top end too low, everything above it turns the same colour.
- **Reset to Defaults:** restores this Field's usual range and palette.

Color Settings also apply to the time–depth views and cross-sections.

### Auto Color

**Auto Color** fits the range to the water currently on screen, at the time shown, so the full range of colours
covers the values in view. Use it whenever the map looks flat or washed out, which often happens after zooming in.
The range then stays fixed as you step or play through time, so colour changes on the map are real changes in the
water.

> **Tip:** zoom in first, then press Auto Color.

## Read values without clicking: Cursor Value and Cursor Coordinates

- **Cursor Value** shows the value under the mouse in a small box beside the pointer, at the depth and time the map
  shows.
- **Cursor Coordinates** adds the latitude and longitude.

Turn them on together to read value and position at once, and click the buttons again to turn them off.

## Click a point

Click anywhere on the water to pick a point. The model gives values for that exact spot, and the chart and the
vertical profile fill in for it. If you click a sensor icon instead, the sensor is selected (see
[Find a sensor](04-find-a-sensor.md)).

## The chart

The chart at the bottom shows the chosen Field at your point over time. By default it shows the last two weeks,
hourly.

- **The bar above the chart** says what it shows: the Field, the model depth, the time range and the point you
  clicked.
- **Pink line:** the model, with one dot per hour (or per day or month; see
  [Move through time](02-move-through-time.md)).
- **Blue band (Climatology):** the lowest to highest value on these dates in every year since 2007. The dashed line
  inside it is the average. **Max**, **Mean** and **Min** label them at the right-hand end. When the model line
  leaves the band, the water is unusual for the time of year.
- **MAP** marks the time the map shows, and **NOW** marks the present.
- **Light and dark stripes** mark day and night.
- **Legend:** names each series. Click a name to hide or show it. Downloads leave out what you hide.
- **Picture icon** (top right of the chart): saves the chart as an image.
- **Hover** over the chart to read the time and the value.
- **Click** the chart to move the map to that time.

## The vertical profile

The profile at the top right shows the Field through the whole water column at your point, from the surface down to
the seabed, at the time shown on the map. Hover over it to read values at each depth. Use the **Vertical Profile**
button in the toolbar to hide or show it.

## Depth

**Depth** in the left panel sets the model level the map and the chart show.

- The **arrows** step one level shallower or deeper.
- **Click the depth** to pick one from the list. Type to search it: for example, type *19* to find 19.5 m.

The map and the chart both follow the new depth. The model's levels are closer together near the surface and
further apart at depth.

## Download

**Download** beside the chart saves its data as a CSV file: every value the chart shows, at full precision. See
[Share & download](12-share-download.md) for what the file contains.

## Server status

The top bar shows whether the data server can be reached.

- **Online:** the server is up. Hover over it to see when it was last checked.
- **Red, with a banner:** the server can't be reached. Maps and charts wait until it is back, and the dashboard
  checks again by itself.

## Worked example: pH in the Strait of Georgia

1. Set **Field** to *pH (Total Scale)*.
2. Zoom in on the open Strait of Georgia, around 49.5° N, 124.1° W.
3. Press **Auto Color**: the full range of colours now covers the pH in view.
4. Click the water. The chart shows two weeks of hourly pH, and the profile shows pH falling from the surface to the
   seabed.
5. Step deeper with the Depth arrows, or pick about 20 m from the list: the map and the chart follow.
