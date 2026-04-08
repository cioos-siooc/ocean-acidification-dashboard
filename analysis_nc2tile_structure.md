# nc2tile.py Code Analysis

**File:** `/home/taimazb/Projects/OA/process/nc2tile.py`  
**Lines:** 878  
**Comparison:** nc2tile (878 lines) vs cnvMaster_RGBcoded (256 lines) = **3.4x longer**

---

## 1. All Functions Defined (19 total + 2 classes)

### Classes (2)
- **Line 153:** `_PrecomputedLinearInterpolator` — Precomputes Delaunay triangulation for fast linear interpolation
- **Line 172:** `_PrecomputedNearestInterpolator` — Precomputes KD-tree for fast nearest-neighbor lookup

### Functions (19)

#### Database & Grid Management (4 functions)
| Line | Function | Purpose |
|------|----------|---------|
| 73 | `get_db_conn()` | Creates PostgreSQL connection using env vars |
| 82 | `_load_grid_cache()` | Loads cached curvilinear grid from `.npz` file |
| 95 | `_write_grid_cache()` | Saves grid cache to disk as compressed numpy |
| 103 | `get_grid_from_db()` | **[Core]** Loads lon/lat curvilinear grid from `grid` table; caches result module-wide |

#### GIS & Coordinate Transformation (6 functions)
| Line | Function | Purpose |
|------|----------|---------|
| 181 | `_get_interpolator()` | Factory to create precomputed interpolators; caches by grid signature |
| 195 | `compute_mercator_grid_bounds()` | Converts lon/lat bounds to Web Mercator (EPSG:3857) meters |
| 208 | `build_target_grid()` | Creates regular Web Mercator meshgrid preserving aspect ratio |
| 233 | `reproject_and_interpolate()` | **[UNUSED]** Interpolates values from source to target grid (dead code—not called) |
| 273 | `_process_task()` | **[WORKER]** Main pixel-level processing: interpolates & writes one PNG per task |
| 815 | `parse_args()` | CLI argument parser |

#### Image Encoding/Writing (4 functions) — CORE JOB
| Line | Function | Purpose |
|------|----------|---------|
| 494 | `scale_to_uint8()` | **[USED]** Scales float array to 0–255 grayscale; applies percentile clipping |
| 522 | `cap_to_range()` | **[USED]** Clips values to min/max range; preserves NaN cells |
| 557 | `write_png_rgba()` | **[UNUSED]** Writes simple RGBA grayscale PNG (superceded by write_png_packed) |
| 570 | `write_png_packed()` | **[USED]** Core encoding: packs float values into RGB channels (24-bit fixed-point) with alpha mask |

#### Metadata & Minmax (4 functions)
| Line | Function | Purpose |
|------|----------|---------|
| 601 | `write_sidecar_json()` | **[UNUSED]** Writes per-file JSON metadata (comment: "not written anymore") |
| 613 | `compute_global_minmax_exclude_zero()` | Computes global min/max across all time/depth, excluding exact zeros |
| 631 | `compute_global_minmax()` | Alias: calls `compute_global_minmax_exclude_zero()` for backwards compat |
| 635 | `process_variable()` | **[ORCHESTRATOR]** Main entry: orchestrates grid loading, task scheduling, parallel/serial execution |

#### Control Flow (1 function)
| Line | Function | Purpose |
|------|----------|---------|
| 830 | `main()` | CLI entry point; parses args and calls `process_variable()` for each variable |

---

## 2. Function Categorization

### ✅ CORE IMAGE ENCODING/WRITING (4 functions, heavily used)
Heavy lifting happens in `_process_task()` which calls:
- **`scale_to_uint8()`** (line 494) — converts float data to grayscale uint8
- **`cap_to_range()`** (line 522) — applies min/max clipping before packing
- **`write_png_packed()`** (line 570) — **THE main output**: packs packed float values into RGB 24-bit fixed-point + alpha

> **Note:** The packing scheme is the innovation here—stores floats as quantized integers split across RGB channels (big-endian), with alpha for transparency.

### 🔧 SUPPORTING INFRASTRUCTURE: GIS & COORDINATES (6 functions)
- **`get_db_conn()`** — Database connection
- **`get_grid_from_db()`** — Load curvilinear grid (Salish Sea specifics)
- **`compute_mercator_grid_bounds()`** — EPSG:3857 projection
- **`build_target_grid()`** — Regular grid meshgrid
- **`_get_interpolator()`** — Interpolator caching (precomputed Delaunay/KD-tree)
- **`_PrecomputedLinearInterpolator`** / **`_PrecomputedNearestInterpolator`** classes

> **Note:** Heavy investment in interpolation optimization (precomputed geometric structures to avoid per-task recomputation).

### 📦 GRID & DATA CACHING (3 functions)
- **`_load_grid_cache()`** — Load `.npz` cache
- **`_write_grid_cache()`** — Save `.npz` cache
- **`_process_task()`** — Caches interpolators in module-level `INTERP_CACHE` dict

> **Note:** Worker processes reuse grid via cache or DB; interpolators cached by grid signature.

### 📊 METADATA / ANALYSIS (4 functions)
- **`compute_global_minmax_exclude_zero()`** — Compute scale bounds across all time steps
- **`compute_global_minmax()`** — Backwards-compat wrapper
- **`write_sidecar_json()`** — Writes bounds/depth metadata (**UNUSED**)
- **`parse_args()` / `main()`** — CLI orchestration

### 🔴 DEAD CODE / UNUSED (2 functions)
| Function | Line | Reason | Notes |
|----------|------|--------|-------|
| `reproject_and_interpolate()` | 233 | Never called | Appears to be legacy; `_process_task()` does interpolation inline with different logic |
| `write_png_rgba()` | 557 | Superceded | Comment at line 481: "per-datetime sidecar files not written anymore" |
| `write_sidecar_json()` | 601 | Explicitly disabled | Comment at line 481: "we use per-variable meta.json instead" |

---

## 3. Why nc2tile is 3.4x Longer Than cnvMaster_RGBcoded

### cnvMaster_RGBcoded (256 lines) — Bathymetry GeoTIFF Tiler
**Strategy:** GeoTIFF → Web Mercator tiles (simpler problem)
- Input: pre-georeferenced GeoTIFF files (NONNA bathymetry)
- Output: Z/X/Y tile pyramid
- Uses `rasterio` to handle CRS/bounds automatically
- **One-shot reprojection** per tile using rasterio's fast C path
- Color lookup table (LUT) encoding
- Simple parallel pool of workers

**Minimal code because:**
- GeoTIFF already has spatial reference → no curvilinear grid complexity
- Tile grid is standard XYZ → simple math
- Trivial encoding (LUT indexing)

---

### nc2tile (878 lines) — NetCDF Curvilinear-Grid Tiler
**Strategy:** NetCDF (Salish Sea model) → Web Mercator images + JSON metadata

#### **Why the Extra 622 Lines:**

1. **Curvilinear Grid Complexity (80+ lines)**
   - Must fetch lon/lat arrays from PostgreSQL `grid` table (not in NetCDF)
   - Reshaping/indexing: convert flat DB records → 2D (nrows × ncols) arrays
   - Caching logic: `.npz` file cache + module-level `GRID_CACHE` (avoid repeated DB hits in workers)

2. **Heavy Interpolation Logic (200+ lines)**
   - Two precomputed interpolator classes: `_PrecomputedLinearInterpolator` (Delaunay), `_PrecomputedNearestInterpolator` (KD-tree)
   - Interpolator caching by grid signature to avoid recomputation
   - Multi-method support (linear, nearest, cubic)
   - Invalid-data masking with multiple fallbacks (explicit mask variables, `_FillValue`, finite-only detection)
   - **Adaptive masking:** if fill==0 causes >99% invalid points, relaxes to finite-only (defensive)
   - Separate mask interpolation using nearest-neighbor to avoid blurry alpha boundaries

3. **Per-Timestep PNG Packing (180+ lines in `_process_task()`)**
   - NetCDF loop: for each variable, time, depth → open dataset, select slice, interpolate, scale, pack, write
   - Packed float encoding: 24-bit fixed-point quantization split into RGB channels
   - Erddap min/max clamping (from `fields` table)
   - Alpha channel derivation from interpolated data
   - Fallback masking if interpolation produces no valid data
   - Verbose logging per-task

4. **Metadata Infrastructure (50+ lines)**
   - Global min/max computation (excluding zeros)
   - Per-variable `meta.json` with bounds + packing metadata (precision, base)
   - Multiprocessing coordination (ProcessPoolExecutor with tqdm progress bar)

5. **Worker Process Handling (100+ lines)**
   - Worker must independently open dataset + fetch grid to avoid large array serialization
   - Task tuple packing (20+ parameters passed to workers)
   - Exception handling in worker completion loop
   - Time-folder tracking to return ISO datetime results

---

## 4. Unused Functions & Code Consolidation Opportunities

### 🗑️ Dead Code to Remove

| Function | Lines | Why Remove | Replacement |
|----------|-------|-----------|-------------|
| `reproject_and_interpolate()` | 233–272 | Not called anywhere; logic duplicated in `_process_task()` | Delete; inline logic is cleaner |
| `write_png_rgba()` | 557–568 | Superseded by `write_png_packed()` | Delete; only `write_png_packed()` is used |
| `write_sidecar_json()` | 601–611 | Explicitly disabled per comment line 481 | Delete or archive in git history |
| `compute_global_minmax()` alias | 631–633 | Thin wrapper for backwards compat | Consolidate: just call `compute_global_minmax_exclude_zero()` directly |

**Estimated lines to remove: ~60 lines**

---

### 🔄 Functions Doing Similar Things (Consolation Candidates)

#### 1. **Interpolator Cacheing Logic**
   - `_get_interpolator()` function (line 181)
   - Similar logic already in `_process_task()` at line 396–408 (inline interpolator selection)
   - **Opportunity:** Consolidate into a single `_build_interpolator()` helper that is always called

#### 2. **Invalid Data Detection (Multiple Fallbacks)**
   - Lines 318–342 in `_process_task()` detect valid data with 4 nested detection strategies:
     1. Explicit mask variables (`{varname}_mask`, `{varname}_valid`, etc.)
     2. `_FillValue` / `missing_value` attributes
     3. Finite + non-zero test
     4. Aggressive sentinel filtering (`< 1e29`, `!= 0`)
   - **Opportunity:** Extract to `_detect_valid_data_mask()` function; reuse in both `_process_task()` and anywhere else

#### 3. **Grid Bounds Computation**
   - `compute_mercator_grid_bounds()` (line 195)
   - Transformer creation + corner projection happens at line 695–705 in `process_variable()`
   - **Opportunity:** Unify; maybe split: one for bounds, one for corners

#### 4. **Min/Max Computation**  
   - `compute_global_minmax_exclude_zero()` (line 613)
   - Only handles exclusion of zeros; logic is hardcoded
   - **Opportunity:** Generalize to a parameterizable `compute_global_minmax(ds_data, varname, exclude_values=None)` function

---

### ⚠️ Potentially Problematic Duplication

**Inline Interpolation Logic in `_process_task()` (lines 380–420):**
```python
# Transform target mercator → lon/lat for interpolation
transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
tgt_lon, tgt_lat = transformer.transform(xx_merc.ravel(), yy_merc.ravel())
tgt_pts = np.column_stack((tgt_lon, tgt_lat))

grid_sig = (...)
interp_engine = _get_interpolator(method, pts_src_full, tgt_pts, grid_sig)

mask_src_f = src_valid_flat.astype(float)
mask_tgt_flat = griddata(pts_src_full, mask_src_f, tgt_pts, method='nearest', ...)
interp_flat = interp_engine.apply(vals_full) or griddata(...)
```

This is the **main interpolation code** and it's quite complex with multiple falls and optimizations. The `reproject_and_interpolate()` function at line 233 appears to be an earlier draft that was never integrated.

---

## 5. Summary: Consolidation Roadmap

### Quick Wins (Remove Dead Code)
- [ ] Delete `reproject_and_interpolate()` (60 lines)
- [ ] Delete `write_png_rgba()` (12 lines)
- [ ] Delete `write_sidecar_json()` (10 lines)
- [ ] Replace `compute_global_minmax()` alias with direct calls (2 lines)

**Total: ~84 lines to remove. New count: ~794 lines.**

### Medium Refactors (Extract & Consolidate)
- [ ] Extract `_detect_valid_data_mask()` (saves ~25 lines, improves testability)
- [ ] Extract `_build_interpolator()` to unify logic from `_get_interpolator()` and `_process_task()`
- [ ] Consolidate bounds/corners computation helpers

### Structural Insights
1. **`_process_task()` is a colossus** (220 lines, lines 273–493): Does worker scheduling, data loading, interpolation, masking, scaling, and PNG writing. Deserves to be split into 3–4 helpers.
2. **Grid caching is over-engineered**: Two-tier cache (.npz file + module dict) is good for performance but adds complexity. Document the caching strategy in a docstring.
3. **Interpolator caching by signature** works but is non-obvious. Cache keys include grid geometry—if grid changes, cache becomes invalid. Add a docstring warning about this.
4. **Mask interpolation logic** (use nearest-neighbor for masks even when data uses linear) is clever for avoiding blurry alpha but not documented.

### Recommended Actions
1. **Remove dead code first** (~84 lines): `reproject_and_interpolate()`, `write_png_rgba()`, `write_sidecar_json()`, alias.
2. **Extract 2–3 helpers from `_process_task()`** to reduce its line count and improve testability:
   - `_interpolate_to_mercator()` — handles interpolator selection + data interpolation
   - `_detect_valid_source_data()` — handles all the mask/fillvalue logic
3. **Add docstrings** explaining caching strategies, interpolator cache invalidation, and mask handling.
