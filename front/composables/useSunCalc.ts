import SunCalc from 'suncalc'
import moment from 'moment-timezone'

export type NightRange = [string, string]

/**
 * Compute night ranges (sunset -> next sunrise) for the local days overlapping [startLocal, endLocal].
 * All inputs and outputs are localized to `tz` (i.e., ISO strings with TZ offset consistent with tz).
 */
export function computeNightRanges(opts: {
  lat: number,
  lon: number,
  tz: string,
  startLocalIso: string,
  endLocalIso: string,
}): NightRange[] {
  const { lat, lon, tz, startLocalIso, endLocalIso } = opts
  const startLocal = moment.tz(startLocalIso, tz).clone();
  const endLocal = moment.tz(endLocalIso, tz).clone();

  // iterate from day before start to day after end to capture nights spanning boundaries
  let day = startLocal.clone().startOf('day').subtract(1, 'day')
  const lastDay = endLocal.clone().endOf('day').add(1, 'day')
  const out: NightRange[] = []

  while (day.isBefore(lastDay)) {
    // compute sunset on this day and sunrise on next day
    const dayDate = day.toDate()
    let sunset: Date | null = null
    let sunriseNext: Date | null = null
    try {
      const times = SunCalc.getTimes(dayDate, lat, lon)
      sunset = times.sunset || null
    } catch (e) {
      sunset = null
    }
    try {
      const nextDayDate = day.clone().add(1, 'day').toDate()
      const times2 = SunCalc.getTimes(nextDayDate, lat, lon)
      sunriseNext = times2.sunrise || null
    } catch (e) {
      sunriseNext = null
    }

    if (!sunset || !sunriseNext) {
      day = day.add(1, 'day')
      continue
    }

    const nStart = moment(sunset).tz(tz)
    const nEnd = moment(sunriseNext).tz(tz)

    if (nEnd.isBefore(startLocal) || nStart.isAfter(endLocal)) {
      day = day.add(1, 'day')
      continue
    }

    const cs = nStart.isBefore(startLocal) ? startLocal.format() : nStart.format()
    const ce = nEnd.isAfter(endLocal) ? endLocal.format() : nEnd.format()
    out.push([cs, ce])

    day = day.add(1, 'day')
  }

  return out
}
