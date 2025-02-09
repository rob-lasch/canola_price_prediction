import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

def get_weather_hist(latitude, longitude):

	# Setup the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)

	# Make sure all required weather variables are listed here
	# The order of variables in hourly or daily is important to assign them correctly below
	url = "https://archive-api.open-meteo.com/v1/archive"
	params = {
		"latitude": latitude,
		"longitude": longitude,
		"start_date": "2020-01-01",
		"end_date": "2025-01-01",
		"daily": ["temperature_2m_mean", "sunshine_duration", "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours", "wind_speed_10m_max", "wind_gusts_10m_max"],
		"timezone": "GMT"
	}
	responses = openmeteo.weather_api(url, params=params)

	# Process first location. Add a for-loop for multiple locations or weather models
	response = responses[0]
	print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
	print(f"Elevation {response.Elevation()} m asl")
	print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
	print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

	# Process daily data. The order of variables needs to be the same as requested.
	daily = response.Daily()
	daily_temperature_2m_mean = daily.Variables(0).ValuesAsNumpy()
	daily_sunshine_duration = daily.Variables(1).ValuesAsNumpy()
	daily_precipitation_sum = daily.Variables(2).ValuesAsNumpy()
	daily_rain_sum = daily.Variables(3).ValuesAsNumpy()
	daily_snowfall_sum = daily.Variables(4).ValuesAsNumpy()
	daily_precipitation_hours = daily.Variables(5).ValuesAsNumpy()
	daily_wind_speed_10m_max = daily.Variables(6).ValuesAsNumpy()
	daily_wind_gusts_10m_max = daily.Variables(7).ValuesAsNumpy()

	daily_data = {"date": pd.date_range(
		start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
		end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
		freq = pd.Timedelta(seconds = daily.Interval()),
		inclusive = "left"
	)}

	daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
	daily_data["sunshine_duration"] = daily_sunshine_duration
	daily_data["precipitation_sum"] = daily_precipitation_sum
	daily_data["rain_sum"] = daily_rain_sum
	daily_data["snowfall_sum"] = daily_snowfall_sum
	daily_data["precipitation_hours"] = daily_precipitation_hours
	daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
	daily_data["wind_gusts_10m_max"] = daily_wind_gusts_10m_max

	return pd.DataFrame(data = daily_data)

def process_monthly_data(df, location_ref_number):
	df['date'] = pd.to_datetime(df['date'])
	name_changes = {'temperature_2m_mean': 'temp',
					'sunshine_duration': 'sun_time',
					'precipitation_sum': 'precip',
					'rain_sum': 'rain',
					'snowfall_sum': 'snow',
					'precipitation_hours': 'precip_time',
					'wind_speed_10m_max': 'max_wind',
					'wind_gusts_10m_max': 'max_gust'}
	df.rename(columns=name_changes, inplace=True)

	# change sun time from seconds to hours
	df['sun_time'] = df['sun_time'].div(3600)
	df['month'] = df['date'].dt.strftime('%Y-%m')
	df = df.groupby('month').agg({'temp': 'mean',
											'sun_time': 'sum',
											'precip': 'sum',
											'precip': 'max',
											'rain': 'sum',
											'snow': 'sum',
											'precip_time': 'sum',
											'precip_time': 'max',
											'max_wind': 'max',
											'max_gust': 'max'})
	df['loaction_ref'] = location_ref_number
	return df


weather_locations = pd.read_csv('../data/weather_data/weather_locations.csv')
df_list = []
for index, row in weather_locations.iterrows():
	tmp_dataframe = get_weather_hist(row['lat_num'], row['lon_num'])
	tmp_dataframe = process_monthly_data(tmp_dataframe, row['location_ref'])
	df_list.append(tmp_dataframe)

result = pd.concat(df_list)
result.to_csv('../data/weather_data/weather_data.csv')




