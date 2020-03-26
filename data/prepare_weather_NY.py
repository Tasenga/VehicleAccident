import pandas as pd


def parse_weather_csv(file):
    """
    from all type of weather in file, create nex set of weather types
    Cloudy, Fog, Freezing Rain, Heavy Rain, Heavy Snow,Mostly Cloudy,
    Partly Cloudy,Rain,Snow,T-Storm,Thunder,Windy,Wintry
    """
    df = pd.read_csv(file)

    df.replace(
        ['Heavy Snow / Windy', 'Heavy Snow with Thunder'],
        'Heavy Snow',
        inplace=True,
    )
    df.replace(
        [
            'Thunder / Windy',
            'Thunder and Small Hail',
            'Thunder in the Vicinity',
        ],
        'Thunder',
        inplace=True,
    )
    df.replace(
        [
            'Blowing Snow',
            'Blowing Snow / Windy',
            'Light Snow',
            'Light Snow / Windy',
            'Light Snow and Sleet',
            'Light Snow and Sleet / Windy',
            'Light Snow with Thunder',
            'Snow / Windy',
            'Snow and Sleet',
            'Snow and Sleet / Windy',
            'Snow and Thunder',
            'Sleet',
        ],
        'Snow',
        inplace=True,
    )
    df.replace('Heavy Rain / Windy', 'Heavy Rain', inplace=True)
    df.replace(
        [
            'Light Drizzle',
            'Light Drizzle / Windy',
            'Light Freezing Drizzle',
            'Light Freezing Rain',
            'Light Sleet',
            'Light Sleet / Windy',
            'Sleet / Windy',
            'Small Hail',
        ],
        'Freezing Rain',
        inplace=True,
    )
    df.replace(
        [
            'Light Rain',
            'Light Rain / Windy',
            'Light Rain with Thunder',
            'Rain / Windy',
            'Rain and Sleet',
        ],
        'Rain',
        inplace=True,
    )
    df.replace(
        [
            'Drizzle and Fog',
            'Drizzle and Fog / Windy',
            'Haze',
            'Haze / Windy',
            'Fog / Windy',
            'Mist',
            'Patches of Fog',
            'Patches of Fog / Windy',
            'Shallow Fog',
            'Smoke',
        ],
        'Fog',
        inplace=True,
    )
    df.replace('Partly Cloudy / Windy', 'Partly Cloudy', inplace=True)
    df.replace('Mostly Cloudy / Windy', 'Mostly Cloudy', inplace=True)
    df.replace('Cloudy / Windy', 'Cloudy', inplace=True)
    df.replace(
        ['Heavy T-Storm', 'Heavy T-Storm / Windy', 'T-Storm / Windy'],
        'T-Storm',
        inplace=True,
    )
    df.replace(
        ['Drizzle / Windy', 'Fair / Windy', 'Squalls / Windy'],
        'Windy',
        inplace=True,
    )
    df.replace('Unknown Precipitation', 'Fair', inplace=True)
    df.replace(
        ['Wintry Mix / Windy', 'Wintry Mix', 'Mix'], 'Wintry', inplace=True
    )

    return df


if __name__ == '__main__':
    parse_weather_csv('weather.csv')
