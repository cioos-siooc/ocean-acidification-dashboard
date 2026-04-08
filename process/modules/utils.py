"""Utility helpers for dl2 package."""
import logging
logger = logging.getLogger('dl2.utils')


def iso_timestr(dt):
    return dt.strftime('%Y%m%dT%H%M')
