from JobSettings import *
from scripts.jb_database import DatabaseVacancies
from scripts.scraper_analyzer import JobAnalyzer

from api import HF_TOKEN

CONFIGS = {
    'country':country,
    'search_term':search_term,
    'n_results':n_results,
    'hours_old':hours_old,
}

get = JobAnalyzer(CONFIGS,HF_TOKEN)
results = get.scrape_jobs()

db = DatabaseVacancies()
db.append_data(results)
