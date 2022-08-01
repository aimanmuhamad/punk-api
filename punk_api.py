import pandas as pd
import apache_beam as beam
import requests
import logging
from urllib.error import HTTPError
from datetime import datetime, timedelta
import time
from pandas.io.json import json_normalize
import google.auth
from google.cloud import bigquery
import awswrangler as wr
import boto3
os.environ
session = boto3.Session(profile_name='DataEngineer')
credentials = session.get_credentials()
 
# Credentials are refreshable, so accessing your access key / secret key
# separately can lead to a race condition. Use this to get an actual matched
# set.

credentials = credentials.get_frozen_credentials()
os.environ["AWS_ACCESS_KEY_ID"] = credentials.access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key
os.environ["AWS_SESSION_TOKEN"] = credentials.token

class CallAPI(beam.DoFn):
    def process(self, input_uri):
        try:
            #request to get json data
            res = requests.get(input_uri).json()
            # Indices list
            vv = []
            uv = []
            bv = []
            bu = []
            mt = []
            fe = []
            tw = []
            ah_1 = []
            ah_2 = []
            ah_3 = []
            ah_4 = []
            ah_5 = []
            mn_1 = []
            mn_2 = []
            mn_3 = []
            ma_1 = []
            ma_2 = []
            ma_3 = []
            hn_1 = []
            hn_2 = []
            hn_3 = []
            hn_4 = []
            hn_5 = []
            ha_1 = []
            ha_2 = []
            ha_3 = []
            ha_4 = []
            ha_5 = []
            hps_1 = []
            hps_2 = []
            hps_3 = []
            hps_4 = []
            hps_5 = []
            hats_1 = []
            hats_2 = []
            hats_3 = []
            hats_4 = []
            hats_5 = []
            yeast = []

            for i in range(0,25) :
              value_volume = res[i]['volume']['value']
              unit_volume = res[i]['volume']['unit']
              boil_volume = res[i]['boil_volume']['value']
              boil_unit = res[i]['boil_volume']['unit']
              mash_temp = res[i]['method']['mash_temp']
              fermentation = res[i]['method']['fermentation']
              twist = res[i]['method']['twist']
              malt_name_1 = res[i]['ingredients']['malt'][0]['name']
              malt_amount_1 = res[i]['ingredients']['malt'][0]['amount']
              malt_name_2 = res[i]['ingredients']['malt'][1]['name']
              malt_amount_2 = res[i]['ingredients']['malt'][1]['amount']
              malt_name_3 = res[i]['ingredients']['malt'][2]['name']
              malt_amount_3 = res[i]['ingredients']['malt'][2]['amount']
              hops_name_1 = res[i]['ingredients']['hops'][0]['name']
              hops_name_2 = res[i]['ingredients']['hops'][1]['name']
              hops_name_3 = res[i]['ingredients']['hops'][2]['name']
              hops_name_4 = res[i]['ingredients']['hops'][3]['name']
              hops_name_5 = res[i]['ingredients']['hops'][4]['name']
              hops_amount_1 = res[i]['ingredients']['hops'][0]['amount']
              hops_amount_2 = res[i]['ingredients']['hops'][1]['amount']
              hops_amount_3 = res[i]['ingredients']['hops'][2]['amount']
              hops_amount_4 = res[i]['ingredients']['hops'][3]['amount']
              hops_amount_5 = res[i]['ingredients']['hops'][4]['amount']
              hops_add_1 = res[i]['ingredients']['hops'][0]['add']
              hops_add_2 = res[i]['ingredients']['hops'][1]['add']
              hops_add_3 = res[i]['ingredients']['hops'][2]['add']
              hops_add_4 = res[i]['ingredients']['hops'][3]['add']
              hops_add_5 = res[i]['ingredients']['hops'][4]['add']
              hops_attribute_1 = res[i]['ingredients']['hops'][0]['attribute']
              hops_attribute_2 = res[i]['ingredients']['hops'][1]['attribute']
              hops_attribute_3 = res[i]['ingredients']['hops'][2]['attribute']
              hops_attribute_4 = res[i]['ingredients']['hops'][3]['attribute']
              hops_attribute_5 = res[i]['ingredients']['hops'][4]['attribute']
              yeast = res[i]['ingredients']['yeast']
              vv.append(value_volume)
              uv.append(unit_volume)
              bv.append(boil_volume)
              bu.append(boil_unit)
              mt.append(mash_temp)
              fe.append(fermentation)
              tw.append(twist)
              mn_1.append(malt_name_1)
              mn_2.append(malt_name_2)
              mn_3.append(malt_name_3)
              ma_1.append(malt_name_1)
              ma_2.append(malt_name_2)
              ma_3.append(malt_name_3)
              hn_1.append(hops_name_1)
              hn_2.append(hops_name_2)
              hn_3.append(hops_name_3)
              hn_4.append(hops_name_4)
              hn_5.append(hops_name_5)
              ha_1.append(hops_add_1)
              ha_2.append(hops_add_2)
              ha_3.append(hops_add_3)
              ha_4.append(hops_add_4)
              ha_5.append(hops_add_5)
              hps_1.append(hops_amount_1)
              hps_2.append(hops_amount_2)
              hps_3.append(hops_amount_3)
              hps_4.append(hops_amount_4)
              hps_5.append(hops_amount_5)
              hats_1.append(hops_attribute_1)
              hats_2.append(hops_attribute_2)
              hats_3.append(hops_attribute_3)
              hats_4.append(hops_attribute_4)
              hats_5.append(hops_attribute_5)
              yeast.append(yeast)
              # Add 1 after looping
              i+=1

            value_volume = pd.Series(vv)
            unit_volume = pd.Series(uv)
            boil_volume = pd.Series(bv)
            boil_unit = pd.Series(bu)
            mash_temp = pd.Series(mt)
            fermentation = pd.Series(fermentation)
            malt_amount_1 = pd.Series(ma_1)
            malt_amount_2 = pd.Series(ma_2)
            malt_amount_3 = pd.Series(ma_3)
            malt_name_1 = pd.Series(mn_1)
            malt_name_2 = pd.Series(mn_2)
            malt_name_3 = pd.Series(mn_3)
            hops_add_1 = pd.Series(ha_1)
            hops_add_2 = pd.Series(ha_2)
            hops_add_3 = pd.Series(ha_3)
            hops_add_4 = pd.Series(ha_4)
            hops_add_5 = pd.Series(ha_5)
            hops_name_1 = pd.Series(hn_1)
            hops_name_2 = pd.Series(hn_2)
            hops_name_3 = pd.Series(hn_3)
            hops_name_4 = pd.Series(hn_4)
            hops_name_5 = pd.Series(hn_5)
            hops_attribute_2 = pd.Series(hats_2)
            hops_attribute_1 = pd.Series(hats_1)
            hops_attribute_2 = pd.Series(hats_2)
            hops_attribute_3 = pd.Series(hats_3)
            hops_attribute_4 = pd.Series(hats_4)
            hops_attribute_5 = pd.Series(hats_5)
            yeast = pd.Series(yeast)

            #res.raise_for_status()

            df = pd.DataFrame(res)
            frame = { 'value_volume': value_volume, 'unit_volume': unit_volume, 'boil_unit' : boil_unit, 
                        'mash_temp' : mash_temp, 'fermentation' : fermentation, 'boil_volume' : boil_volume,
                        'malt_amount_1' : malt_amount_1, 'malt_amount_2' : malt_amount_2, 'malt_amount_3' : malt_amount_3,
                        'malt_name_1' : malt_name_1,'malt_name_2' : malt_name_2,'malt_name_3':malt_name_3, 'hops_add_1' : hops_add_1,
                        'hops_add_2' : hops_add_2,'hops_add_3' : hops_add_3,'hops_add_4' : hops_add_4,'hops_add_5' : hops_add_5, 
                        'hops_name_1' : hops_name_1, 'hops_name_2' : hops_name_2, 'hops_name_3' : hops_name_3,'hops_name_4' : hops_name_4,
                        'hops_name_5' : hops_name_5, 'hops_attribute_1' : hops_attribute_1, 'hops_attribute_2' : hops_attribute_2, 
                        'hops_attribute_3' : hops_attribute_3, 'hops_attribute_4' : hops_attribute_4, 'hops_attribute_5' : hops_attribute_5}

            df = pd.DataFrame(frame)
            df.drop('volume', inplace=True, axis=1)
            df.drop('boil_volume', inplace=True, axis=1)
            df.drop('method', inplace=True, axis=1)
            df.drop('ingredients', inplace=True, axis=1)
            # yield df

        except HTTPError as message:
            logging.error(message)

class TransformDF(beam.DoFn):
    def process(self, input):
        #df equals to input from function above
        df = input
        df = df.dropna()
        df = df.dropna(axis=0)
        # Reset index after drop
        df = df.dropna().reset_index(drop=True)
        df.to_csv('beam_pipeline.csv',index = False)
        yield df

class DFtoCSV(beam.DoFn):
    def process(self, input):
        # df equals to input from function above
        df = input
        # Convert dataframe to csv
        BUCKET = brewery
        wr.s3.to_csv(
            df= df,
            path=f"s3://{BUCKET}/brewery_data.csv")
        # Yield dataframe
        yield df

class DFtoJSON(beam.DoFn):
    def process(self, input):
        #df equals to input from function above
        df = input
        #Convert dataframe to csv
        BUCKET = brewery
        wr.s3.to_json(
            df = df,
            path = f"s3://{BUCKET}/brewery_data.json",
            default_handler = str,
            orient = 'records')
        yield df
        
class DFtoDWH(beam.DoFn):
    def process(self, input):
        bigquery_client = bigquery.Client()
        scope = [
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/bigquery.insertdata',
        ]

        creds, project_id = google.auth.default(scopes=scope)
        client_gcs = storage.Client(project=project_id, credentials=creds)
        bigquery_client = bigquery.Client(project=project_id, credentials=creds)

        project_id = PROJECT_ID

        #df equals to input from function above
        df = input
        table_name = 'punk_api'
        # Schema in Bigquery
        punk_api_schema = [bigquery.SchemaField("id", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("name", "STRING", "NULLABLE"),
                           bigquery.SchemaField("tagline", "STRING", "NULLABLE"),
                           bigquery.SchemaField("first_brewed", "STRING", "NULLABLE"),
                           bigquery.SchemaField("description", "STRING", "NULLABLE"),
                           bigquery.SchemaField("abv", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("ibu", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("target_fg", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("target_og", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("ebc", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("srm", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("ph", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("attenuation_level", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("yeast", "STRING", "NULLABLE"),
                           bigquery.SchemaField("malt_name_1", "STRING", "NULLABLE"),
                           bigquery.SchemaField("malt_amount_1", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("malt_name_2", "STRING", "NULLABLE"),
                           bigquery.SchemaField("malt_amount_2", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("malt_name_3", "STRING", "NULLABLE"),
                           bigquery.SchemaField("malt_amount_3", "NUMERIC", "NULLABLE"),
                           bigquery.SchemaField("hops_attribute_1", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_attribute_2", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_attribute_3", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_attribute_4", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_attribute_5", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_add_1", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_add_2", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_add_3", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_add_4", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_add_5", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_name_1", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_name_2", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_name_3", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_name_4", "STRING", "NULLABLE"),
                           bigquery.SchemaField("hops_name_5", "STRING", "NULLABLE"),
                           bigquery.SchemaField("food_pairing", "STRING", "NULLABLE"),
                           bigquery.SchemaField("brewers_tips", "STRING", "NULLABLE"),
                           bigquery.SchemaField("contributed_by", "STRING", "NULLABLE")]

        bq_schema = locals()[table_name + '_schema']

        job_config = bigquery.LoadJobConfig(schema=bq_schema,
                                            # autodetect=True,
                                            allow_quoted_newlines=True,
                                            skip_leading_rows=1,
                                            # The source format defaults to CSV, so the line below is optional.
                                            source_format=bigquery.SourceFormat.CSV,
                                            # field_delimiter=',',
                                            # allow_jagged_rows=True,
                                            write_disposition="WRITE_TRUNCATE")

        dataset_ref = bigquery_client.dataset(bq_dataset)
        job = bigquery_client.load_table_from_uri(file,
                                                  dataset_ref.table(table_name),
                                                  job_config=job_config)
        print(job.result())
        assert job.job_type == 'load'
        assert job.state == 'DONE'

        # load_into_bigquery(gcs_path, table_name, bq_schema)

        query_job = bigquery_client.query(f"""merge `{prod_table}` prod
                                              using `{stg_table}` stg
                                              when not matched then INSERT ROW""")
        if query_job.errors:
            raise Exception('Table was not successfullly merged in bigQuery')

        destination_table = bigquery_client.get_table(stg_table)  # Make an API request.
        destination_table.expires = datetime.now() + timedelta(days=1)
        bigquery_client.update_table(destination_table, ['expires'])

with beam.Pipeline() as p:
    data = (
        p
        | beam.Create(['https://api.punkapi.com/v2/beers'])
        | 'Call API ' >> beam.ParDo(CallAPI())
        | 'TransformDF' >> beam.ParDo(TransformDF())
        | 'DFtoCSV' >> beam.ParDo(DFtoCSV())
        | 'DFtoJSON' >> beam.ParDo(DFtoJSON())
        | 'DFtoDWH' >> beam.ParDo(DFtoDWH())
        | beam.Map(print)
    )