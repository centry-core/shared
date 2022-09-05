from datetime import datetime


def humanize_timestamp(timestamp: str):
    dt = datetime.fromtimestamp(int(timestamp)/1000.0)
    return format_datetime(dt)


def format_datetime(dt: datetime):
    return dt.strftime("%d.%m.%Y, %H:%M:%S")
