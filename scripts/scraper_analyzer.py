import csv
from jobspy import scrape_jobs
from scripts.jb_database import DatabaseVacancies
from utils.check_proxies import ProxiesChecker

from langdetect import detect
from deep_translator import GoogleTranslator,MyMemoryTranslator

import random

import pandas as pd


from transformers import pipeline

from pathlib import Path
from pypdf import PdfReader
import json

import os

os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1" 

class JobAnalyzer():

    def __init__(self,configs:dict,
                 HF_TOKEN:str,
                 LLM_model:str = 'Qwen/Qwen2.5-1.5B-Instruct'
):

        for key, value in configs.items():
            setattr(self, key, value)

        self.connection = DatabaseVacancies()
        self.proxies = ProxiesChecker().get_valid_proxies()

        self.pipe = pipeline("text-generation", model=LLM_model, device_map="auto",token=HF_TOKEN)

        self.CV = self._get_CV()


    def scrape_jobs(self):


        jobs = scrape_jobs(
            site_name=["indeed"],
            search_term=self.search_term,
            results_wanted=self.n_results,
            hours_old=self.hours_old,
            country_indeed=self.country,
            proxies=["localhost"]#.extend(self.proxies),
        )

        jobs['lang_descr'] = jobs['description'].apply(self._detect_lang)

        jobs['description'] = jobs.apply(
                lambda x: self._translator(x['description'], x['lang_descr']), 
                axis=1
            )

        jobs['scrape_time'] = pd.Timestamp.now()

        jobs['raw_match_results'] = jobs['description'].apply(self._check_match)

        match_df = jobs['raw_match_results'].apply(self._parse_json).apply(pd.Series)
    
        self.jobs = pd.concat([jobs, match_df], axis=1).drop(columns=['raw_match_results'])

        return self.jobs


    
    def _check_match(self,cv_text, job_descr):
        prompt = f"""
            Compare my CV and the following Job Description. 
            Focus specifically on:
            1. Technical Skills match.
            2. My previous experience
            3. Probability of being called for interview.
            4. Language requirements.

            CV: {self.CV}
            JD: {job_descr}

            Output a JSON with:
            - "technical_score": (0-100)
            - "previous_experience_score": (0-100)
            - "interview_call_score": (0-100)
            - "languages_match": (true/false/None)
            - "explanation": (Short sentence explaining why)
        """
        
        messages = [{"role": "user", "content": prompt}]
        result = self.pipe(messages, max_new_tokens=256)
        return result[0]['generated_text']



    def _detect_lang(self,text):
        try:
            return detect(text)
        except:
            return None


    def _translator(self,txt,lang):
        print(f'-----------------------------lang {lang}\n\n\n')
        if lang.strip() == 'en':
            return txt
        
        else:
            pxs = {"https":px for px in self.proxies}

            ## need to split text 
            tool = MyMemoryTranslator('dutch' if lang.strip()=='nl' else 'auto',
                             target='english',
                             proxies=pxs)
            
            return tool.translate(txt)
        
    
    def _get_CV(self):

        assert Path('curriculum/CV.pdf').exists()

        reader = PdfReader('curriculum/CV.pdf')

        pages = reader.pages

        return '\n'.join([pages[i].extract_text() for i in range(len(pages))])

        

    def _parse_json(self,x):
        try:
            start = x.find('{')
            end = x.rfind('}') + 1
            return json.loads(x[start:end])
        except:
            return {"technical_score": 0,
                    'previous_experience_score':0,
                    'interview_call_score':0,
                    "languages_match": None,
                    "explanation": "Error parsing LLM response"}


