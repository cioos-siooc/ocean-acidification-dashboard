import { CalendarDate } from '@internationalized/date'

/**
 * Bridge between this app's `Date`/ISO date models and Nuxt UI's `UCalendar`,
 * which speaks `@internationalized/date`'s `CalendarDate`.
 *
 * The distinction matters: a `Date` is an *instant*, a `CalendarDate` is a
 * timezone-less Y/M/D triple. Every picker in this app means the latter — "the
 * user chose August 21st" — which is why the old `v-date-picker` call sites all
 * had to read local Y/M/D off a Date and rebuild it (see the comment that used
 * to sit in `TimeControls.confirmDatePicker`). Converting only at the picker
 * boundary keeps the UTC-vs-local conversion where it already lives: at the
 * API-query and chart-render edges, never in between.
 *
 * See [[project-depth-profile-bin-resolution]] for how the picked day is then
 * floored to a bin.
 */

/** `Date` (read via its LOCAL Y/M/D, i.e. what the user saw) -> calendar day. */
export function toCalendarDate(d: Date | string | null | undefined): CalendarDate | null {
    if (!d) return null
    if (typeof d === 'string') {
        // 'YYYY-MM-DD' — parse the digits directly rather than via `new Date()`,
        // which would treat a bare date string as UTC and can shift the day.
        const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(d)
        if (m) return new CalendarDate(Number(m[1]), Number(m[2]), Number(m[3]))
        d = new Date(d)
    }
    if (Number.isNaN(d.getTime())) return null
    return new CalendarDate(d.getFullYear(), d.getMonth() + 1, d.getDate())
}

/** Calendar day -> `Date` at LOCAL midnight — matches what `v-date-picker` returned. */
export function fromCalendarDate(cd: CalendarDate | null | undefined): Date | null {
    if (!cd) return null
    return new Date(cd.year, cd.month - 1, cd.day)
}

/** Calendar day -> 'YYYY-MM-DD'. */
export function calendarDateToIso(cd: CalendarDate | null | undefined): string | null {
    if (!cd) return null
    return `${cd.year}-${String(cd.month).padStart(2, '0')}-${String(cd.day).padStart(2, '0')}`
}
