import duckdb
from pathlib import Path
import pandas as pd


class DatabaseVacancies():

    def __init__(self,root:str = 'db'):

        self.ROOT = root

        self.db_name = 'db_vacancies'

        self.db_path = root + '/' + self.db_name + '.duckdb'

        self._verify_db()

        


    def _verify_db(self):

        Path(self.ROOT).mkdir(parents=True, exist_ok=True)

        self.db_connection = duckdb.connect(self.db_path)

        count_res = self.db_connection.execute("""
                    SELECT count(*) FROM information_schema.tables
                    """).fetchone()

        if count_res==0:
            self._create_main_table()


    def _create_main_table(self):

        self.db_connection.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id VARCHAR PRIMARY KEY,
            scrape_time TIMESTAMP,          
            site VARCHAR,
            job_url VARCHAR,
            job_url_direct VARCHAR,
            title VARCHAR,
            company VARCHAR,
            location VARCHAR,
            date_posted DATE,
            job_type VARCHAR,
            salary_source VARCHAR,
            interval VARCHAR,
            min_amount DOUBLE,
            max_amount DOUBLE,
            currency VARCHAR,
            is_remote BOOLEAN,
            job_level VARCHAR,
            job_function VARCHAR,
            listing_type VARCHAR,
            emails VARCHAR,
            description TEXT,
            company_industry VARCHAR,
            company_url VARCHAR,
            company_logo VARCHAR,
            company_url_direct VARCHAR,
            company_addresses VARCHAR,
            company_num_employees VARCHAR,
            company_revenue VARCHAR,
            company_description TEXT,
            skills VARCHAR,
            experience_range VARCHAR,
            company_rating DOUBLE,
            company_reviews_count INTEGER,
            vacancy_count INTEGER,
            work_from_home_type VARCHAR,
            
            --  LLM Analysis 
            lang_descr VARCHAR,
            technical_score INTEGER,
            previous_experience_score INTEGER,
            interview_call_score INTEGER,
            languages_match BOOLEAN,
            explanation TEXT
        );
        """)


    def append_data(self, df: pd.DataFrame):
 
        if df is None or df.empty:
            print("No data to append.")
            return None

        try:
            self.db_connection.execute(f"""
                INSERT OR IGNORE INTO jobs 
                SELECT * FROM df
            """)
            
            changes = self.db_connection.execute("SELECT changes()").fetchone()[0]
            print(f"Successfully added {changes} new unique jobs to the database.")
            
        except Exception as e:
            print(f"Error appending data: {e}")




        




