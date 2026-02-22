import csv
from jobspy import scrape_jobs
from scripts.jb_database import DatabaseVacancies
from utils.check_proxies import ProxiesChecker

from langdetect import detect
from deep_translator import ChatGptTranslator

import random

import pandas as pd
from time import sleep

from transformers import pipeline,AutoModelForCausalLM,BitsAndBytesConfig, MarianMTModel, MarianTokenizer# issues with translator when using pipeline

from pathlib import Path
from pypdf import PdfReader
import json

import os



# to employ this for translation with bigger GPU

os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1" 

class JobAnalyzer():

    def __init__(self,configs:dict,
                 HF_TOKEN:str,
                 LLM_model:str = 'Qwen/Qwen2.5-7B-Instruct'
                 ):

        for key, value in configs.items():
            setattr(self, key, value)

        self.connection = DatabaseVacancies()
        self.proxies = ProxiesChecker().get_valid_proxies()

        
        bits_config = BitsAndBytesConfig(load_in_4bit=True,
                                         bnb_4bit_use_double_quant=True,
                                         bnb_4bit_quant_type="nf4")

        LLM_model_q = AutoModelForCausalLM.from_pretrained(
            LLM_model,
            quantization_config=bits_config,
            device_map="auto",
            token=HF_TOKEN
        )

        self.pipe = pipeline("text-generation",
                             model=LLM_model_q,
                             device_map="auto",
                             token=HF_TOKEN)

        self.CV = self._get_CV()


        self.map_transl = {'nl':'Helsinki-NLP/opus-mt-nl-en','de':"Helsinki-NLP/opus-mt-de-en"}

        self.trans_tokenizer = {'nl':MarianTokenizer.from_pretrained(self.map_transl['nl']),
                                'de':MarianTokenizer.from_pretrained(self.map_transl['de'])}
        
        self.trans_model = {'nl':MarianMTModel.from_pretrained(self.map_transl['nl'],tie_word_embeddings=False),
                                'de':MarianMTModel.from_pretrained(self.map_transl['de'],tie_word_embeddings=False)}


    def scrape_jobs(self):

        print('''--- SCRAPING ---''')

        jobs = scrape_jobs(
            site_name=["indeed"],
            search_term=self.search_term,
            results_wanted=self.n_results,
            hours_old=self.hours_old,
            country_indeed=self.country,
            proxies=["localhost"]#+self.proxies,
        )

        jobs['lang_descr'] = jobs['description'].apply(self._detect_lang)


        print('''--- TRANSLATING ---''')

        jobs['description'] = jobs.apply(
                lambda x: self._translator(x['description'], x['lang_descr']), 
                axis=1
            )

        print('''--- FINISHED TRANSLATION ---''')
        jobs['scrape_time'] = pd.Timestamp.now()

        jobs['LLM_results'] = jobs['description'].apply(self._check_match)

        match_df = jobs['LLM_results'].apply(self._parse_json).apply(pd.Series)

        self.jobs = pd.concat([jobs, match_df], axis=1)

        return self.jobs


    
    def _check_match(self,job_descr):
        prompt = f"""
            Compare my CV and the following Job Description. 
            Focus specifically on:
            1. Technical Skills match.
            2a. Match with previous work experience
            2b. Match with previous education
            3. Probability of being called for interview.
            4. Language requirements.

            CV: {self.CV}
            JD: {job_descr}

            Output a JSON with realistic scores:
            - "technical_score": (0-100)
            - "previous_experience_score": (0-100)
            - "interview_call_score": (0-100)
            - "languages_match": (true/false/None)
            - "explanation": (Short sentence explaining why)
        """
        
        messages = [{"role": "user", "content": prompt}]
        result = self.pipe(messages, max_new_tokens=256)
        print(result)
        return result[0]['generated_text'][1]['content']



    def _detect_lang(self,text):
        try:
            return detect(text)
        except:
            return None


    def _translator(self,txt,lang):
        if lang.strip() == 'en':
            return txt
        
        else:
            return txt ### CURRENTLY NOT TRANSLATING
            chunks = txt.split('\n')
            translated_chunks = []
            for parag in chunks:
                tokenizer = self.trans_tokenizer[lang]
                model = self.trans_model[lang]

                inputs = tokenizer(parag, return_tensors="pt", padding=True)
                outputs = model.generate(**inputs)
                translation = tokenizer.decode(outputs[0], skip_special_tokens=True)

                translated_chunks.append(translation)
            
            return '\n'.join(translated_chunks)
        
    
    def _get_CV(self):

        assert Path('curriculum/CV.pdf').exists()

        reader = PdfReader('curriculum/CV.pdf')

        pages = reader.pages

        return '\n'.join([pages[i].extract_text() for i in range(len(pages))])

        

    def _parse_json(self,x):
        x = str(x)
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


