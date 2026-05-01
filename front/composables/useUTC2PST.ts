export function utc2pst(dt: moment.Moment): string {
    // Convert a UTC moment to PST string format
    return dt.utcOffset(-8).format('ddd MMM DD, YYYY, HH:mm');
}