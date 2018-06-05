""" Get non-farm, seasonally adjusted employment time series for every state from BLS """

import requests
import json
import re
import us
import pandas as pd

from config import API_KEY

def fips_to_id(st="09"):

    """ Convert a fips code to the ID for non-farm, seasonally adjusted state-level employment.
    More info on Bureau of Labor Statistics time series codes here: https://www.bls.gov/help/hlpforma.htm """

    return "SMS" + str(st).zfill(2) + "000000000000001"

def id_to_fips(series_id):

    """ Extract a state's FIPS code from a time series id. 
    More info on Bureau of Labor Statistics time series codes here: https://www.bls.gov/help/hlpforma.htm """

    match = re.match("SMS([0-9]{2})000000000000001", series_id)

    if match:
        return match.group(1)

    return None

# def state_fips():

#     """ Get a dictionary of FIPS codes """

#     ret = {}
    
#     for x in us.states.STATES:
#         ret[x.abbr] = x.fips

#     return ret

def download_employment_data(fips_list, start_year=1991, end_year=2010):

    """ Download state employment dataframe for a given list of states, for a
    given range of years """
    
    headers = {'Content-type':'application/json'}

    series_ids = [fips_to_id(x) for x in fips_list]

    data = json.dumps({"seriesid": series_ids,
                       "registrationkey":API_KEY,
                       "startyear":str(start_year),
                       "endyear":str(end_year)})

    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/',
                      data=data,
                      headers=headers)

    json_data = json.loads(p.text)

    # We'll have one row per state, with one column per year-month period
    rows = {}

    if 'series' not in json_data['Results']: return
    
    for series in json_data['Results']['series']:
        series_id = series['seriesID']
        for item in series['data']:        
            fips = id_to_fips(series_id)
            state = us.states.lookup(fips)
            st_abbr = state.abbr
            year_period = "-".join([item['year'], item['period'].replace("M","")])
            value = item['value']

            # If there is no entry for this series ID, create it
            if series_id not in rows:
                rows[series_id] = {
                    "state":st_abbr,
                    "fips":fips
                }

            # set the period "column" to the value
            rows[series_id][year_period] = value

    return pd.DataFrame(rows).transpose()
        
def download_all_employment_data(start_year=1991, end_year=2018):

    """ Chunk requests according to BLS API limits: No more than 50 states per
    request (DC puts us over that limit); and no more than 20 years of data at a time """


    # # This will give a dict of { '09':'CT', ....} 
    # fips_codes = us.states.mapping('fips', 'abbr')

    fips_codes = [x.fips for x in us.states.STATES]
    years = range(int(start_year), int(end_year))
    
    fips_index = 0
    year_index = 0

    fips_interval = 50
    year_interval = 20

    # We're going to have to collect data into rows and combine them
    # fips_frames will hold dataframes broken into chunks to accommodate the BLS API's
    fips_frames = []
    while fips_index < len(fips_codes):
        request_fips = fips_codes[fips_index:min(fips_index+fips_interval,
                                                 len(fips_codes) - 1)]

        year_frame = None
        while year_index < len(years):

            request_start_year = years[year_index]
            request_end_year = years[min(len(years) - 1,
                                         year_index + year_interval - 1)]

            year_index += year_interval

            year_chunk = download_employment_data(request_fips,
                                                  start_year=request_start_year,
                                                  end_year=request_end_year)

            if year_frame is None:
                year_frame = year_chunk

            else:
                year_frame = year_frame.join(year_chunk, rsuffix="_chk")

            year_frame

        fips_frames.append(year_frame)
            
        fips_index += fips_interval

    return pd.concat(fips_frames)


# download_employment_data([9], start_year="2008", end_year=2018).to_csv("CT-1990-2018.csv")

def main():

    """ Get the data and save it """

    all_df = download_all_employment_data(start_year=1991,end_year=2018)
    all_df.reindex(sorted(all_df.columns), axis=1).to_csv("ALL-1991-2018.csv")

if __name__ == "__main__":
    main()
