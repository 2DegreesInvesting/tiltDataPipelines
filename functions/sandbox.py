# Databricks notebook source
# MAGIC %md
# MAGIC #Imports

# COMMAND ----------

# from spark_matcher.deduplicator.deduplicator import Deduplicator as SparkDeduplicator
from functions.dataframe_helpers import create_sha_values
from functions.spark_session import create_spark_session
from lingua import Language, LanguageDetectorBuilder
from functions.custom_dataframes import CustomDF 
from deduplipy.deduplicator import Deduplicator
from deep_translator import GoogleTranslator
from transformers import pipeline
from pyspark.sql.types import *
from textblob import TextBlob

import pyspark.sql.functions as F
import pandas as pd
import numpy as np
import pickle
import torch
import uuid
import os

# Define a namespace (this can be any UUID)
namespace = uuid.NAMESPACE_DNS

# COMMAND ----------

# MAGIC %md
# MAGIC # Pre-process raw data functions

# COMMAND ----------

languages = [Language.ENGLISH, Language.FRENCH, Language.GERMAN, Language.SPANISH, Language.ITALIAN, Language.DUTCH, Language.ARABIC, Language.CHINESE]
language_detector = LanguageDetectorBuilder.from_languages(*languages).build()
typo_corrector = pipeline("text2text-generation", model="oliverguhr/spelling-correction-english-base", max_length=1000)
translator = GoogleTranslator(source='auto', target='en')

# COMMAND ----------

def create_new_id(text):
    return str(uuid.uuid5(namespace, text))

# COMMAND ----------

def conf_ld_detect_language(text):
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
        detected_language = language_detector.detect_language_of(text).iso_code_639_1.name.lower()
        return detected_language
    except:   
        return "ident_fail"
    
def typo_correction(text="", model="default"):
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
            return(typo_corrector(text)[0]["generated_text"])
    except:
        return text

def typo_correct_df(df):
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
    # to_process_df = df.copy()
    to_process_df = df.alias("to_process_df")
    # to_process_df.loc[:, "language (ISO-code)"] = to_process_df["product_name"].apply(self.conf_ld_detect_language)
    to_process_df = to_process_df.withColumn("language (ISO-code)", F.udf(conf_ld_detect_language, StringType())("product_name"))

    # then take subset of english texts
    print("Taking subset of English texts...")
    # english_df = to_process_df[to_process_df["language (ISO-code)"] == "en"]
    english_df = to_process_df.filter(to_process_df["language (ISO-code)"] == "en")
    # exclude enlgish texts from the original df
    # to_process_df = to_process_df[to_process_df["language (ISO-code)"] != "en"]
    to_process_df = to_process_df.filter(to_process_df["language (ISO-code)"] != "en")

    # apply typo correction to english texts
    print("Applying typo correction...")
    # english_df.loc[:, "typo_corrected"] = english_df["product_name"].apply(self.typo_correction)
    english_df = english_df.withColumn("typo_corrected", F.udf(typo_correction, StringType())("product_name"))

    # merge the corrected english texts with the original df
    print("Merging the corrected english texts with the original df...")
    to_process_df = to_process_df.withColumn("typo_corrected", F.col("product_name"))
    # df = pd.concat([to_process_df, english_df], ignore_index=True)
    df = to_process_df.union(english_df)
    # replace empty values in typo_corrected with the original text
    # df["typo_corrected"].fillna(value=df["product_name"], inplace=True)
    df = df.withColumn("typo_corrected", F.when(F.col("typo_corrected").isNull(), F.col("product_name")).otherwise(F.col("typo_corrected")))
    # make typo_corrected lowercase and remove all dots at the end
    # df["typo_corrected"] = df["typo_corrected"].str.lower().str.replace("\.$", "")
    df = df.withColumn("typo_corrected", F.lower(df["typo_corrected"]))
    df = df.withColumn("typo_corrected", F.regexp_replace(df["typo_corrected"], "\.$", ""))
    return df

def translate_Google(text):
    """
    This function translates the text into English using Google Translator
    """
    
    try:
        translated = translator.translate(text)
        return translated
    except:
        return np.nan

def translate_df(df):
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
    # non_english_df = df[(df["language (ISO-code)"].isnull() == False) & (df["language (ISO-code)"] != "en")]
    non_english_df = df.filter((df["language (ISO-code)"].isNull() == False) & (df["language (ISO-code)"] != "en"))
    # exclude the rows from non_english_df from the original df
    # df = df[~df.index.isin(non_english_df.index)]
    df = df.exceptAll(non_english_df)

    # apply typo correction to english texts
    print("Applying translation...")
    # non_english_df = non_english_df.copy()
    # non_english_df.loc[:, 'translated_text'] = non_english_df['typo_corrected'].apply(self.translate_Google)
    non_english_df = non_english_df.withColumn("translated_text", F.udf(translate_Google, StringType())("typo_corrected"))

    # merge the corrected english texts with the original df
    print("Merging the translated non-english texts with the original df...\n")
    # df = pd.concat([df, non_english_df], ignore_index=True)
    df = df.withColumn("translated_text", F.col("typo_corrected"))
    df = df.union(non_english_df)
    
    # replace empty values in translated column with the typo corrected text
    # df["translated_text"].fillna(df["typo_corrected"], inplace=True)
    df = df.withColumn("translated_text", F.when(df["translated_text"].isNull(), df["typo_corrected"]).otherwise(df["translated_text"]))

    # translated_df = df.copy().drop(columns=["typo_corrected", "language (ISO-code)"]).rename(columns={"product_name":"raw_product_name","translated_text": "product_name"})
    translated_df = df.drop("typo_corrected", "language (ISO-code)").withColumnRenamed("product_name","raw_product_name")
    translated_df = translated_df.withColumnRenamed("translated_text", "product_name")
    # print(translated_df.head(1))
    return translated_df

def deduplicate_df(translated_df, companies_products_mapping):
    if os.path.exists('tilt_deduper.pkl'):
        with open('tilt_deduper.pkl', 'rb') as f:
            deduplicator = pickle.load(f)
    else:
        deduplicator = Deduplicator(col_names=['product_name'], interaction=True, verbose=1)
        deduplicator.fit(translated_df)
        with open('tilt_deduper.pkl', 'wb') as f:
            pickle.dump(deduplicator, f)

    deduplipy_result = spark.createDataFrame(deduplicator.predict(translated_df.toPandas(), score_threshold=0.90))

    # combined_df = deduplipy_result.copy().merge(translated_df, on='product_name', how='left')
    combined_df = deduplipy_result.join(translated_df, on="product_name", how="left")

    # take the first product_id for each group and assign it as a new column to the test dataframe
    # combined_df.loc[:, "prime_product_id"] = combined_df.groupby("deduplication_id").products_id.transform(lambda g: g.iloc[0])
    combined_df = combined_df.withColumn("prime_product_id", F.expr("first(products_id)").over(Window.partitionBy("deduplication_id")))
    
    # full_listing = combined_df.merge(companies_products_mapping, on="product_id", how="right")
    full_listing = combined_df.join(companies_products_mapping, on="product_id", how="right")

    # updated_companies_products_mapping = full_listing[["company_id", "prime_product_id"]].drop_duplicates().rename(columns={"prime_product_id": "product_id"})
    updated_companies_products_mapping = full_listing.select("company_id", "prime_product_id").dropDuplicates().withColumnRenamed("prime_product_id", "product_id")

    # deduplicated_products = combined_df[["prime_product_id", "product_name"]].rename(columns={"prime_product_id": "product_id"}).drop_duplicates(subset=["product_id"])
    deduplicated_products = combined_df.select("prime_product_id", "product_name").withColumnRenamed("prime_product_id", "product_id").dropDuplicates(["product_id"])

    return deduplicated_products, updated_companies_products_mapping

# COMMAND ----------

def preprocess(companies_europages_raw):
        print("Starting preprocessing...")
        # products_data_raw = companies_europages_raw.copy()[["product_id", "product_name"]].drop_duplicates()
        products_data_raw = companies_europages_raw.select("product_id", "product_name").dropDuplicates()
        # companies_products_raw = companies_europages_raw.copy()[["company_id", "product_id"]].drop_duplicates()
        companies_products_raw = companies_europages_raw.select("company_id", "product_id").dropDuplicates()
        print("Typo correcting...")
        # first typo_correct
        typo_corrected_df = typo_correct_df(products_data_raw)
        print("Translating...")
        # then translate
        translated_df = translate_df(typo_corrected_df)

        # create a new ID based on the pre-processed columns to replace the old ones with
        # translated_df['new_product_id'] = translated_df['product_name'].apply(lambda name: self.create_new_id(name))
        translated_df = translated_df.withColumn('new_product_id', F.udf(create_new_id, StringType())("product_name"))

        # companies_products_raw = companies_products_raw.merge(translated_df, on="product_id", how='left')
        companies_products_raw = companies_products_raw.join(translated_df, on="product_id", how='left')
        # select the columns we want to keep
        # companies_products_raw = companies_products_raw[['company_id', 'new_product_id']]
        companies_products_raw = companies_products_raw.select("company_id", "new_product_id")
        # rename new_product_id to product_id
        # companies_products_raw.rename(columns={"new_product_id": "product_id"}, inplace=True)
        companies_europages_raw = companies_europages_raw.withColumnsRenamed({"new_product_id": "product_id"})

        # drop_products_id and rename new_product_id to products_id
        # translated_df.drop("product_id", axis=1, inplace=True)
        translated_df = translated_df.drop("product_id")
        # translated_df.rename(columns={"new_product_id": "product_id"}, inplace=True)
        translated_df = translated_df.withColumnRenamed("new_product_id", "product_id")
        # only take the columns we want to keep
        # translated_df = translated_df[['product_id', 'product_name']].drop_duplicates()
        translated_df = translated_df.select('product_id', 'product_name').dropDuplicates()
        return companies_products_raw, translated_df
        # print("Deduplicating...")
        # # then deduplicate
        # deduplicated_products, companies_products_mapping = deduplicate_df(translated_df, companies_products_raw)

        # print("Preprocessing finished.")
        # return deduplicated_products, companies_products_mapping

# COMMAND ----------

companies_europages_raw = CustomDF(
    'companies_europages_raw', spark)

rename_dict = {"id": "company_id"}

companies_europages_raw.rename_columns(rename_dict)

companies_europages_raw.data = companies_europages_raw.data.withColumn("product_name", F.explode(F.split("products_and_services", "\|")))\
    .drop("products_and_services")

companies_europages_raw.data = companies_europages_raw.data.select(
    "product_name", "company_id")

companies_europages_raw.data = companies_europages_raw.data.withColumn(
    'product_id', F.sha2(F.col('product_name'), 256)).dropDuplicates()

companies_europages_mapping, translated_df = preprocess(companies_europages_raw._df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEDUPLIPY TESTING

# COMMAND ----------

# Read CSV file as pandas dataframe
translated_df = pd.read_csv("translated_df.csv")

# Rename the column from "products_and_services" to "product_name"
translated_df.rename(columns={"products_and_services": "product_name"}, inplace=True)

# COMMAND ----------

if os.path.exists('tilt_deduper.pkl'):
    with open('tilt_deduper.pkl', 'rb') as f:
        deduplicator = pickle.load(f)
else:
    deduplicator = Deduplicator(col_names=['product_name'], interaction=True, verbose=1)
    deduplicator.fit(translated_df)
    with open('tilt_deduper.pkl', 'wb') as f:
        pickle.dump(deduplicator, f)


# COMMAND ----------

deduplipy_result = deduplicator.predict(translated_df.toPandas(), score_threshold=0.90)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPARK_MATCHER TESTING (FAILURE ATM)

# COMMAND ----------

checkpoints_path = "/Workspace/Repos/selsabrouty@deloitte.nl/tiltDataPipelines/functions/checkpoints/"

if os.path.exists('tilt_deduplicator.pkl'):
    deduplicator = SparkDeduplicator(spark)
    deduplicator.load('tilt_deduplicator.pkl')
else:
    deduplicator = SparkDeduplicator(spark, col_names=['product_name'], checkpoint_dir=checkpoints_path)
    deduplicator.fit(tilt_products_translated)
    deduplicator.save('tilt_deduplicator.pkl')

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

deduplipy_result = deduplicator.predict(translated_df, threshold=0.90)

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate pre-processed tables and store

# COMMAND ----------

companies_europages_raw = CustomDF(
    'companies_europages_raw', spark)

rename_dict = {"id": "company_id"}

companies_europages_raw.rename_columns(rename_dict)

companies_europages_raw.data = companies_europages_raw.data.withColumn("product_name", F.explode(F.split("products_and_services", "\|")))\
    .drop("products_and_services")

companies_europages_raw.data = companies_europages_raw.data.select(
    "product_name", "company_id")

companies_europages_raw.data = companies_europages_raw.data.withColumn(
    'product_id', F.sha2(F.col('product_name'), 256)).dropDuplicates()

# pre-process the products
processed_products, companies_products_mapping = preprocess(companies_europages_raw._df)
