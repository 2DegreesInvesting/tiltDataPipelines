from lingua import Language, LanguageDetectorBuilder
from deep_translator import GoogleTranslator
from pandarallel import pandarallel
from transformers import pipeline
from textblob import TextBlob

import pandas as pd
import numpy as np
import pickle
import torch
import uuid

# Define a namespace (this can be any UUID)
namespace = uuid.NAMESPACE_DNS

class PreProcessor():

    def __init__(self):
        pandarallel.initialize(progress_bar=True)

        # specify the languages to be used for language detection to keep it within the scope of possible languages
        self.languages = [Language.ENGLISH, Language.FRENCH, Language.GERMAN, Language.SPANISH, Language.ITALIAN, Language.DUTCH, Language.ARABIC, Language.CHINESE]
        self.language_detector = LanguageDetectorBuilder.from_languages(*self.languages).build()
        self.typo_corrector = pipeline("text2text-generation", model="oliverguhr/spelling-correction-english-base", max_length=1000)
        self.translator = GoogleTranslator(source='auto', target='en')

    def preprocess(self, companies_europages_raw):
        print("Starting preprocessing...")
        print(companies_europages_raw.columns())
        products_data_raw = companies_europages_raw.copy()["product_id", "product_name"].drop_duplicates()
        companies_products_raw = companies_europages_raw.copy()[["company_id", "product_id"]].drop_duplicates()

        print("Typo correcting...")
        # first typo_correct
        typo_corrected_df = self.typo_correct_df(products_data_raw)
        print("Translating...")
        # then translate
        translated_df = self.translate_df(typo_corrected_df)

        # create a new ID based on the pre-processed columns to replace the old ones with
        translated_df['new_product_id'] = translated_df['product_name'].apply(lambda name: uuid.uuid5(namespace, name))

        companies_products_raw = companies_products_raw.merge(translated_df, on="product_id", how='left')
        # select thte columns we want to keep
        companies_products_raw = companies_products_raw[['company_id', 'new_product_id']]
        # rename new_product_id to product_idz
        companies_products_raw.rename(columns={"new_product_id": "product_id"}, inplace=True)

        # drop_products_id and rename new_product_id to products_id
        translated_df.drop("product_id", axis=1, inplace=True)
        translated_df.rename(columns={"new_product_id": "product_id"}, inplace=True)
        # only take the columns we want to keep
        translated_df = translated_df[['product_id', 'product_name']].drop_duplicates()

        print("Deduplicating...")
        # then deduplicate
        deduplicated_products, companies_products_mapping = self.deduplicate_df(translated_df, companies_products_raw)
        
        print("Preprocessing finished.")
        return deduplicated_products, companies_products_mapping
    
    def conf_ld_detect_language(self, text):
        """Language detection wrapper.
        
        Returns detected language (ISO-code) and confidence of detection. In case of 
        failure of detection string 'ident_fail' and a pd.NA value for confidence is 
        returned.
        
        Args:
            text (str): The string for which language shall be detected.
            model (str): The model to be used for language detection. Defaults to langdetect model.
        Returns:
            str: The detected language (ISO-code).
        """
        
        try:
            detected_language = self.language_detector.detect_language_of(text).iso_code_639_1.name.lower()
            return detected_language
        except:   
            return "ident_fail", pd.NA
        
    def typo_correction(self, text="", model="default"):
        """Typo correction wrapper.
        
        Returns corrected text. In case of failure of correction the original text 
        is returned. 
        
        Args:
            text (str): The string to be corrected.
            model (str): The model to be used for typo correction. Defaults to textblob model.
        Returns:
            str: The corrected string.
        """
        try:
            if model == "default":
                return(TextBlob(text).correct().string)
            elif model == "huggingface":
                return(self.typo_corrector(text)[0]["generated_text"])
        except:
            return text
    
    def typo_correct_df(self, df):
        """Typo correction wrapper for dataframes.
        
        Returns dataframe with corrected text. In case of failure of correction the 
        original text is returned. 
        
        Args:
            df (pd.DataFrame): The dataframe containing the text to be corrected.
        Returns:
            pd.DataFrame: The dataframe with corrected text.
        """
        # detect the language of the text but only for the rows that do not have a value in the automatic_processed_products_and_services column
        print("Detecting the language of the text...")
        # only take rows that have a True value in the to_process column
        to_process_df = df.copy()
        to_process_df.loc[:, "language (ISO-code)"] = to_process_df["product_name"].parallel_apply(self.conf_ld_detect_language)

        # then take subset of english texts
        print("Taking subset of English texts...")
        english_df = to_process_df[to_process_df["language (ISO-code)"] == "en"]
        # exclude enlgish texts from the original df
        to_process_df = to_process_df[to_process_df["language (ISO-code)"] != "en"]

        # apply typo correction to english texts
        print("Applying typo correction...")
        english_df = english_df.copy()
        english_df.loc[:, "typo_corrected"] = english_df["product_name"].parallel_apply(self.typo_correction)

        # merge the corrected english texts with the original df
        print("Merging the corrected english texts with the original df...")
        df = pd.concat([to_process_df, english_df], ignore_index=True)
        # replace empty values in typo_corrected with the original text
        df["typo_corrected"].fillna(df["product_name"], inplace=True)
        # make typo_corrected lowercase and remove all dots at the end
        df["typo_corrected"] = df["typo_corrected"].str.lower().str.replace("\.$", "")
        return df

    def translate_Google(self, text):
        """
        This function translates the text into English using Google Translator
        """
        
        try:
            translated = self.translator.translate(text)
            return translated
        except:
            return np.nan

    def translate_df(self, df):
        """
        This function translates the dataframe into English using Google Translator

        Args:
            df (pd.DataFrame): The dataframe to be translated.
        Returns:
            translated_df (pd.DataFrame): The translated dataframe.
        """
        # then take subset of english texts
        print("Taking subset of non-english texts...")
        # filter out non-english texts and text that do not have a language code
        non_english_df = df[(df["language (ISO-code)"].isnull() == False) & (df["language (ISO-code)"] != "en")]
        # exclude the rows from non_english_df from the original df
        df = df[~df.index.isin(non_english_df.index)]

        # apply typo correction to english texts
        print("Applying translation...")
        non_english_df = non_english_df.copy()
        non_english_df.loc[:, 'translated_text'] = non_english_df['typo_corrected'].parallel_apply(self.translate_Google)

        # merge the corrected english texts with the original df
        print("Merging the corrected english texts with the original df...\n")
        df = pd.concat([df, non_english_df], ignore_index=True)
        
        # replace empty values in translated column with the typo corrected text
        df["translated_text"].fillna(df["typo_corrected"], inplace=True)
        translated_df = df.copy().drop(columns=["typo_corrected", "language (ISO-code)"]).rename(columns={"product_name":"raw_product_name","translated_text": "product_name"})
        return translated_df
    
    def deduplicate_df(self, translated_df, companies_products_mapping):
        with open('tilt_deduper.pkl', 'rb') as f:
            pre_trained_deduplipy = pickle.load(f)

        deduplipy_result = pre_trained_deduplipy.predict(translated_df, score_threshold=0.95)

        combined_df = deduplipy_result.copy().merge(translated_df, on='product_name', how='left')

        # take the first product_id for each group and assign it as a new column to the test dataframe
        combined_df.loc[:, "prime_product_id"] = combined_df.groupby("deduplication_id").products_id.transform(lambda g: g.iloc[0])

        full_listing = combined_df.merge(companies_products_mapping, on="product_id", how="right")

        updated_companies_products_mapping = full_listing[["company_id", "prime_product_id"]].drop_duplicates().rename(columns={"prime_product_id": "product_id"})

        deduplicated_products = combined_df[["prime_product_id", "product_name"]].rename(columns={"prime_product_id": "product_id"}).drop_duplicates(subset=["product_id"])

        return deduplicated_products, updated_companies_products_mapping