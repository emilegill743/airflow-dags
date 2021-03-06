import pandas as pd
from sqlalchemy import create_engine
import os
import time
import functools
import sys
import requests
import json
import io
from http import HTTPStatus
from pycountry_convert import (
    country_alpha2_to_continent_code,
    country_name_to_country_alpha2)

def jhu_cases_etl(connection_uri):

    """ETL job for Johns Hopkins Global Cases Data"""

    def extract_data():

        """Get JHU data for global confirmed cases"""

        jhu_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
        jhu_df = pd.read_csv(jhu_url)

        return(jhu_df)

    def transform_data(jhu_df):

        """Transform JHU data into Cases by Date by Region"""

        jhu_df['Province/State'].fillna(value='N/A', inplace=True)

        jhu_df = pd.melt(
                    frame=jhu_df,
                    id_vars=['Country/Region',
                             'Province/State',
                             'Lat',
                             'Long'],
                    var_name='Date',
                    value_name='cases')

        jhu_df['Date'] = pd.to_datetime(jhu_df['Date'])

        jhu_df.sort_values(
            by=['Country/Region', 'Province/State', 'Date'],
            inplace=True)

        jhu_df = jhu_df.rename(columns={
                    'Country/Region': 'region',
                    'Province/State': 'province',
                    'Lat': 'lat',
                    'Long': 'long',
                    'Date': 'date',
                    })

        return(jhu_df)

    def load_data(jhu_df, connection_uri):

        """Load JHU data into Postgres Database"""

        table_name = 'jhu_global_cases'

        db_engine = create_engine(connection_uri)

        with db_engine.connect() as con:
            con.execute(f'DROP TABLE IF EXISTS raw.{table_name} CASCADE')

        jhu_df.to_sql(
            name=table_name,
            con=db_engine,
            schema="raw",
            if_exists="replace",
            index=False,
            method='multi')

    data = extract_data()
    data = transform_data(data)
    load_data(data, connection_uri)


def jhu_deaths_etl(connection_uri):

    """ETL job for Johns Hopkins Global Deaths Data"""

    def extract_data():

        """Get JHU data for global deaths"""

        jhu_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv'
        jhu_df = pd.read_csv(jhu_url)

        return(jhu_df)

    def transform_data(jhu_df):

        """Transform JHU data into Deaths by Date by Region"""

        jhu_df['Province/State'].fillna(value='N/A', inplace=True)

        jhu_df = pd.melt(
                    frame=jhu_df,
                    id_vars=['Country/Region',
                            'Province/State',
                            'Lat',
                            'Long'],
                    var_name='Date',
                    value_name='deaths')

        jhu_df['Date'] = pd.to_datetime(jhu_df['Date'])

        jhu_df.sort_values(
            by=['Country/Region', 'Province/State', 'Date'],
            inplace=True)

        jhu_df = jhu_df.rename(columns={
                    'Country/Region': 'region',
                    'Province/State': 'province',
                    'Lat': 'lat',
                    'Long': 'long',
                    'Date': 'date',
                    })

        return(jhu_df)

    def load_data(jhu_df, connection_uri):

        """Load JHU data into Postgres Database"""

        table_name = 'jhu_global_deaths'

        db_engine = create_engine(connection_uri)

        with db_engine.connect() as con:
            con.execute(f'DROP TABLE IF EXISTS raw.{table_name} CASCADE')

        jhu_df.to_sql(
                name=table_name,
                con=db_engine,
                schema="raw",
                if_exists="replace",
                index=False,
                method='multi')

    data = extract_data()
    data = transform_data(data)
    load_data(data, connection_uri)


def jhu_lookup_etl(connection_uri):

    """ETL job for Johns Hopkins Reference 'Lookup' Table"""

    lookup_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv'
    lookup_df = pd.read_csv(lookup_url)

    def country_alpha2_to_continent(country_alpha2):

        """Map country name to continent"""

        try:
            continent_code = country_alpha2_to_continent_code(country_alpha2)
        except:
            continent_code = "N/A"

        return(continent_code)

    lookup_df['Continent'] = lookup_df['iso2'].apply(
                                country_alpha2_to_continent)

    lookup_df.rename(
        columns={col: col.lower() for col in lookup_df.columns},
        inplace=True)

    table_name = 'jhu_lookup'

    db_engine = create_engine(connection_uri)

    with db_engine.connect() as con:
        con.execute(f'DROP TABLE IF EXISTS raw.{table_name} CASCADE')

    lookup_df.to_sql(
        name=table_name,
        con=db_engine,
        schema="raw",
        if_exists="replace",
        index=False,
        method='multi')


def jhu_us_cases_etl(connection_uri):

    """ETL job for Johns Hopkins US Deaths Data"""

    def extract_data():

        """Get JHU data for US deaths"""

        jhu_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv'
        jhu_df = pd.read_csv(jhu_url)

        return(jhu_df)

    def transform_data(jhu_df):

        """Transform JHU data into Cases by Date by Region"""

        jhu_df = jhu_df.drop(
                            columns=[
                                'UID', 'iso2', 'iso3',
                                'code3', 'FIPS', 'Admin2',
                                'Combined_Key', 'Lat', 'Long_'])
        jhu_df = pd.melt(
                        frame=jhu_df,
                        id_vars=['Country_Region',
                                'Province_State'],
                        var_name='Date',
                        value_name='cases')

        jhu_df = jhu_df.groupby(
                            ['Country_Region',
                             'Province_State',
                             'Date']).sum().reset_index()

        jhu_df['Date'] = pd.to_datetime(jhu_df['Date'])

        jhu_df = jhu_df.rename(columns={
                    'Country_Region': 'region',
                    'Province_State': 'province',
                    'Date': 'date',
                    })

        return(jhu_df)

    def load_data(jhu_df, connection_uri):

        """Load JHU data into Postgres Database"""

        table_name = 'jhu_us_cases'

        db_engine = create_engine(connection_uri)

        with db_engine.connect() as con:
            con.execute(f'DROP TABLE IF EXISTS raw.{table_name} CASCADE')

        jhu_df.to_sql(
            name=table_name,
            con=db_engine,
            schema="raw",
            if_exists="replace",
            index=False,
            method='multi')

    data = extract_data()
    data = transform_data(data)
    load_data(data, connection_uri)


def jhu_us_deaths_etl(connection_uri):

    """ETL job for Johns Hopkins US Deaths Data"""

    def extract_data():

        """Get JHU data for US deaths"""

        jhu_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv'
        jhu_df = pd.read_csv(jhu_url)

        return(jhu_df)

    def transform_data(jhu_df):

        """Transform JHU data into Deaths by Date by Region"""

        jhu_df = jhu_df.drop(
                            columns=[
                                'UID', 'iso2', 'iso3',
                                'code3', 'FIPS', 'Admin2',
                                'Combined_Key', 'Population',
                                'Lat', 'Long_']) 
        jhu_df = pd.melt(
                        frame=jhu_df,
                        id_vars=['Country_Region',
                                'Province_State'],
                        var_name='Date',
                        value_name='deaths')

        jhu_df = jhu_df.groupby(
                            ['Country_Region',
                             'Province_State',
                             'Date']).sum().reset_index()

        jhu_df['Date'] = pd.to_datetime(jhu_df['Date'])

        jhu_df = jhu_df.rename(columns={
                    'Country_Region': 'region',
                    'Province_State': 'province',
                    'Date': 'date',
                    })

        return(jhu_df)

    def load_data(jhu_df, connection_uri):

        """Load JHU data into Postgres Database"""

        table_name = 'jhu_us_deaths'

        db_engine = create_engine(connection_uri)

        with db_engine.connect() as con:
            con.execute(f'DROP TABLE IF EXISTS raw.{table_name} CASCADE')

        jhu_df.to_sql(
            name=table_name,
            con=db_engine,
            schema="raw",
            if_exists="replace",
            index=False,
            method='multi')

    data = extract_data()
    data = transform_data(data)
    load_data(data, connection_uri)


def us_states_etl(connection_uri):

    """ETL job for loading US states
    Longitude/Latitude data"""

    dirname = os.path.dirname(__file__)
    us_states_dataset_path = os.path.join(dirname,
                                          'geo_data',
                                          'kaggle_states.csv')

    us_states_df = pd.read_csv(us_states_dataset_path)

    table_name = 'us_states_coords'

    db_engine = create_engine(connection_uri)

    with db_engine.connect() as con:
        con.execute(f'DROP TABLE IF EXISTS raw.{table_name} CASCADE')

    us_states_df.to_sql(
                name=table_name,
                con=db_engine,
                schema="raw",
                if_exists="replace",
                index=False,
                method='multi')


def local_uk_data_etl(connection_uri):

    """ETL job for UK local data"""

    def extract_data():

        """Pull local UK data from gov.uk API"""

        endpoint = "https://api.coronavirus.data.gov.uk/v1/data"

        filters = [
            "areaType=ltla"
        ]

        structure = {
            "date": "date",
            "areaName": "areaName",
            "areaCode": "areaCode",
            "newCasesByPublishDate": "newCasesByPublishDate",
            "cumCasesByPublishDate": "cumCasesByPublishDate"}

        api_params = {
            "filters": str.join(";", filters),
            "structure": json.dumps(structure, separators=(",", ":"))}

        data = []
        page_number = 1

        while True:

            api_params["page"] = page_number

            response = requests.get(endpoint, params=api_params, timeout=240)

            if response.status_code >= HTTPStatus.BAD_REQUEST:
                raise RuntimeError(f'Request failed: {response.text}')
            elif response.status_code == HTTPStatus.NO_CONTENT:
                break

            current_data = response.json()
            page_data = current_data['data']

            data.extend(page_data)

            if current_data["pagination"]["next"] is None:
                break

            page_number += 1

        df = pd.DataFrame(data)

        return(df)

    def transform_data(local_uk_df):

        local_uk_df = local_uk_df.rename(columns={
                            'areaName': 'area_name',
                            'areaCode': 'area_code',
                            'newCasesByPublishDate': 'new_cases',
                            'cumCasesByPublishDate': 'cum_cases'})

        return(local_uk_df)

    def load_data(local_uk_df, connection_uri):

        """Load UK local data into Postgres Database"""

        table_name = 'local_uk'

        db_engine = create_engine(connection_uri)

        with db_engine.connect() as con:
            con.execute(f'DROP TABLE IF EXISTS raw.{table_name} CASCADE')

        local_uk_df.to_sql(
            name=table_name,
            con=db_engine,
            schema="raw",
            if_exists="replace",
            index=False,
            method='multi')

    data = extract_data()
    data = transform_data(data)
    load_data(data, connection_uri)


def owid_global_vaccinations_etl(connection_uri):

    # Extract owid vaccinations data
    owid_global_vaccinations_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv"
    owid_global_vaccinations_df = pd.read_csv(owid_global_vaccinations_url)

    owid_global_vaccinations_df.date = pd.to_datetime(
                                            owid_global_vaccinations_df.date)

    # Load owid vaccinations data

    table_name = 'owid_global_vaccinations'

    db_engine = create_engine(connection_uri)

    with db_engine.connect() as con:
        con.execute(f'DROP TABLE IF EXISTS raw.{table_name} CASCADE')

    owid_global_vaccinations_df.to_sql(
        name=table_name,
        con=db_engine,
        schema="raw",
        if_exists="replace",
        index=False,
        method='multi')


def bloomberg_global_vaccinations_etl(connection_uri):

    # Extract owid vaccinations data
    bloomberg_global_vaccinations_url = "https://raw.githubusercontent.com/BloombergGraphics/covid-vaccine-tracker-data/master/data/current-global.csv"
    bloomberg_global_vaccinations_df = pd.read_csv(
                                            bloomberg_global_vaccinations_url)

    # Load owid vaccinations data

    table_name = 'bloomberg_global_vaccinations'

    db_engine = create_engine(connection_uri)

    with db_engine.connect() as con:
        con.execute(f'DROP TABLE IF EXISTS raw.{table_name} CASCADE')

    bloomberg_global_vaccinations_df.to_sql(
        name=table_name,
        con=db_engine,
        schema="raw",
        if_exists="replace",
        index=False,
        method='multi')


def load_to_s3(table_name, connection_uri, s3_storage_options):

    db_engine = create_engine(connection_uri)

    data = pd.read_sql_table(
        table_name=table_name,
        con=db_engine,
        schema='prod')

    data.to_csv(
        f's3://covid19-bokeh-app/data/{table_name}.csv',
        index=False,
        storage_options=s3_storage_options)
