import csv
import os

import pandas as pd
from scripts.jb_database import DatabaseVacancies
from scripts.scraper_analyzer import JobAnalyzer

# ad hoc codes for multi country search
COUNTRIES = ['netherlands','belgium','germany','italy']

CONFIGS = {
    'country': 'netherlands',
    'search_term':'data scientist',
    'n_results':150, # get 150 results
    'hours_old':72, # job posting from the previous 72 hours
}

OUTPUT_FOLDER = 'outputs'
OUTPUT_FILE =  os.path.join(OUTPUT_FOLDER,'job_search.csv')

FILE_CHECK = os.path.isfile(OUTPUT_FILE)

os.makedirs(OUTPUT_FOLDER, exist_ok=True)


if __name__=='__main__':

    results = []

    for curr_country in COUNTRIES:

        CURR_CONFIGS =  CONFIGS.copy()

        CURR_CONFIGS['country'] = curr_country

        scraper = JobAnalyzer(CURR_CONFIGS)

        out = scraper.scrape_jobs()
        out['ref_country'] = curr_country

        results.append(out)

    dataframe = pd.concat(results,ignore_index=True).reset_index(drop=True)

    dataframe.to_csv(OUTPUT_FILE,mode='a',index=False,header=not FILE_CHECK) # Write header if not exists 


