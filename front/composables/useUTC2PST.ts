import moment from 'moment-timezone'
import { APP_TIMEZONE } from '@/config/app'

/**
 * Format a UTC moment as a human-readable local BC timestamp.
 *
 * Note the historical name: this returns Pacific *local* time, which is PST
 * only in winter. It previously used a fixed `utcOffset(-8)`, which silently
 * displayed times an hour early for the ~8 months a year BC is on PDT.
 * Going through `APP_TIMEZONE` picks up the DST transitions correctly.
 */
export function utc2pst(dt: moment.Moment): string {
    return dt.clone().tz(APP_TIMEZONE).format('ddd MMM DD, YYYY, HH:mm');
}
