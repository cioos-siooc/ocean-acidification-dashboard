# Analytics Section — Feature Summary

## Simple Mode (Main Panel)

### 1. All Years Overlaid (default chart view)
- Plots each year (2007–2026) as its own line on a shared calendar x-axis, color-coded old→new
- Lets you see if a recent year stands out against the full historical spread
- **Audience**: researchers wanting a quick visual anomaly check — "was 2024 unusual for this location?"

### 2. Annual Summary (default chart view - to be removed, maybe)
- Per-year boxplot (min/Q1/median/Q3/max) + mean scatter + linear trend line
- Shows both inter-annual variability and the long-run direction
- **Audience**: anyone communicating trends — educators, policy briefings, public-facing reports

### 3. Season Filter (All / MAM / JJA / SON / DJF)
- Client-side filter applied to already-fetched data, so switching is instant
- Isolates seasonal signals (e.g., "is aragonite saturation declining in summer specifically?")
- **Audience**: researchers who care about season-specific dynamics

### 4. Statistic Selector (Min / Mean / Max)
- Controls which daily aggregate is plotted; intentionally requires a manual "Run Analysis" button press (unlike point/variable/depth which auto-fetch)
- **Audience**: researchers asking worst-case (Min) vs. average vs. peak-stress (Max) questions

### 5. Threshold / Per-Year Stats Panel
- User sets a threshold value + direction (above/below); table shows count of days and longest consecutive streak per year that exceed it
- Heat-mapped coloring and row hover highlights that year's line in the overlay chart
- **Audience**: marine biologists and managers tracking how often conditions cross biologically meaningful thresholds (e.g., aragonite < 1.0 for shell-forming species)

### 6. All-time Records
- Shows the single highest and lowest observed values with their dates
- **Audience**: quick reference for anyone — educators, journalists, public

---

## Advanced Mode

Opened via the fullscreen icon; shares the same fetched series, adds 5 deeper analysis tabs:

| Tab | What it does | Audience |
|---|---|---|
| **Extreme Events** | Identifies and characterizes extreme-value episodes | Researchers studying marine heatwaves / acidification events |
| **Compound Stress** | Fetches a second variable and identifies co-occurrence of stress in both simultaneously | Researchers studying combined stressors (e.g., low pH + low O₂) - Expandable to more than two variables if needed |
| **Trend** | Statistical trend decomposition beyond the simple annual-summary line | Researchers or report authors wanting formal trend analysis |
| **Climatology Anomaly** | Computes anomalies relative to a climatological baseline | Researchers communicating departure from "normal" |
| **Correlation** | Cross-correlates the primary variable against a second variable | Researchers exploring variable relationships at a location |
