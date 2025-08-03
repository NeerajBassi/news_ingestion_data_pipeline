from datetime import datetime
import hashlib
import logging

def parse_date(date_str, source= 'Skift'):
    """
    Parse a date string into a datetime object based on the source format.

    For 'Phocuswire' source, expects date in format like 'July 28, 2025'.
    For other sources (default 'Skift'), expects ISO8601 date string.

    Args:
        date_str (str): Date string to parse.
        source (str, optional): Source name to decide parse format. Defaults to 'Skift'.

    Returns:
        datetime.datetime or None: Parsed datetime object if successful; None otherwise.
    """
    try:
        if source == "Phocuswire":
            return datetime.strptime(date_str.strip(), "%B %d, %Y")
        else:
            return datetime.fromisoformat(date_str)
    except Exception:
        return None
    
def drop_timezone(date_str):
    """
    Remove timezone information from an ISO8601 datetime string 
    and return it formatted as 'YYYY-MM-DDTHH:MM:SS'.

    Args:
        date_str (str): ISO8601 datetime string with timezone (e.g. '2025-07-25T14:30:00+00:00').

    Returns:
        str: ISO8601 datetime string without timezone offset (e.g. '2025-07-25T14:30:00').
    """
    date_str = datetime.fromisoformat(date_str)
    return date_str.strftime("%Y-%m-%dT%H:%M:%S")
    
def generate_article_id(url):
    """
    Generate a unique article ID by applying MD5 hash on the UTF-8 encoded URL.

    Args:
        url (str): Article URL string.

    Returns:
        str: Hexadecimal MD5 hash string representing the unique article ID.
    """
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def datetime_to_iso_with_time(dt):
    """
    Convert a datetime object to an ISO8601-formatted string including time.

    Args:
        dt (datetime.datetime): A datetime object (date and time part used).

    Returns:
        str: ISO8601 string formatted as 'YYYY-MM-DDTHH:MM:SS'.

    Example:
        >>> datetime_to_iso_with_time(datetime(2025, 7, 25, 14, 30, 0))
        '2025-07-25T14:30:00'
    """
    date_part = dt.strftime("%Y-%m-%d")
    time_str = dt.strftime("%H:%M:%S")
    return f"{date_part}T{time_str}"