import logging
from pathlib import Path
from os.path import dirname, abspath
import requests
from dateutil import parser, rrule
from datetime import datetime
import configparser

from work_with_document import write_csv


_LOGGER = logging.getLogger(__name__)
cwd = Path(dirname(abspath(__file__)))


def get_weather_data(stations, firstday, lastday):
    """
    function gets data from weather.com per day (for each hour)
    for the specified stations and for a given period
    """
    conf = configparser.ConfigParser()
    conf.read(Path(dirname(abspath(__file__)), "config.ini"))
    apikey = conf["weather.com"]["apikey"]

    weather = [["date_time", "station", "weather"]]
    for station in stations:
        for date in rrule.rrule(rrule.DAILY, dtstart=firstday, until=lastday):
            day = requests.get(
                url=f"https://api.weather.com/v1/location/"
                f"{station}:9:US/observations/historical.json"
                f"?apiKey={apikey}&units=e"
                f"&startDate={date.strftime('%Y%m%d')}"
            ).json()
            for row in day["observations"]:
                weather.append(
                    [
                        datetime.fromtimestamp(row["valid_time_gmt"]),
                        row["obs_id"],
                        row["wx_phrase"],
                    ]
                )
        _LOGGER.info(f"end write for {station}")
    return weather


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    _LOGGER.info("start program")
    stations = ["KJFK", "KEWR", "KLGA", "KISP"]
    firstday = parser.parse("2016-01-01")
    lastday = parser.parse("2019-12-31")
    write_csv(
        Path(cwd, "resulting_data", "weather1.csv"),
        mode="w",
        values=get_weather_data(stations, firstday, lastday),
    )
    _LOGGER.info("end program")
