from datetime import datetime
import hashlib
import logging

def parse_date(date_str, source= 'Skift'):
    """
    Converts date string like 'July 28, 2025' to datetime object.
    Return None if parse fails.
    """
    try:
        if source == "Phocuswire":
            return datetime.strptime(date_str.strip(), "%B %d, %Y")
        else:
            return datetime.fromisoformat(date_str)
    except Exception:
        return None
    
def drop_timezone(date_str):
    date_str = datetime.fromisoformat(date_str)
    return date_str.strftime("%Y-%m-%dT%H:%M:%S")
    
def generate_article_id(url):
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def datetime_to_iso_with_time(dt):
    """
    Convert a datetime object to ISO8601 string with a fixed time part.

    Args:
        dt (datetime): A datetime object (date part used).

    Returns:
        str: Formatted ISO8601 string in 'YYYY-MM-DDTHH:MM:SS' format
    """
    date_part = dt.strftime("%Y-%m-%d")
    time_str = dt.strftime("%H:%M:%S")
    return f"{date_part}T{time_str}"