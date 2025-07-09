##############################################################################
import pyspark
import os
import sys
import time
import os
import gcsfs
import json
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup

#############################################################################
from delta import *
from delta.tables import *
from google.cloud import storage
from google.cloud import bigquery

#############################################################################
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark import SparkContext, SQLContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import DateType
from pyspark.sql import Window
from pyspark.sql.functions import to_timestamp, col, udf, sha2, concat_ws, current_date, current_timestamp, array, lpad,instr, window, lag, lead
from pyspark.sql import DataFrame

#############################################################################
import DataOperator as do

#############################################################################
############## FUNÇÃO QUE FAZ O CRAWLER DOS FERIADOS NACIONAIS ##############
#############################################################################
def crawler():

    def tabela_feriado_nacional_por_ano(year): # Função que faz a varredura de cada página disponível (uma por ano)

        url = main_url + year_link_dict[year]

        response = requests.get(url)

        if response.status_code == 200:

            soup = BeautifulSoup(response.text, 'html.parser')

            table = soup.find('table', {'class': 'interna'})

            # Check if the table is found
            if table:

                # Extract data from the table
                final_table_data = []
                rows = table.find_all('tr')
                for row in rows:
                    # Extract data from each row
                    columns = row.find_all('td')
                    data = [column.text.strip() for column in columns]
                    if data: 
                        date, day_of_week, holiday = data
                        final_table_data.append([date, day_of_week, holiday])              

                final_table = pd.DataFrame(final_table_data[1:], columns = final_table_data[0])

                ###TRATAMENTO DOS DADOS
                final_table['Data'] = pd.to_datetime(str(year) + final_table['Data'].str.split("/").str[1] + final_table['Data'].str.split("/").str[0], format='%Y%m%d')
                #Tratamento com quebra de coluna necessário pois as colunas tinham formatos diferentes de data, por exemplo "1/1/40" e "25/12/2040"
                final_table['Data'] = final_table['Data'].dt.date
                #Transformar coluna datetime para formato date
                final_table = final_table.rename(columns = {'Data': 'data', 'Feriado': 'feriado'})
                #Transformar colunas para letra minúscula conforme padrão do projeto
                final_table = final_table[['data', 'feriado']]
                #Retornar apenas as colunas de data e de descrição do feriado

            else:

                final_table = pd.DataFrame([]) #preferir retornar dataframe vazio em vez de quebrar lógica

        else:

            final_table = pd.DataFrame([]) #preferir retornar dataframe vazio em vez de quebrar lógica

        print(f'Coleta Ano: {year} - Total de registros: {str(len(final_table_data))}')

        time.sleep(3)

        return final_table
    
    try:

        global main_url
        main_url = "https://www.anbima.com.br/feriados/"

        response = requests.get(main_url)

        if response.status_code == 200:

            soup = BeautifulSoup(response.text, 'html.parser')

            # Todas as páginas que contém as informações de feriado
            links = [a['href'] for a in soup.find_all('a', class_='linkinterno') if '.asp' in a['href']]

            years = [int(href_value.split("/")[1].split('.')[0]) for href_value in links]

            global year_link_dict
            year_link_dict = dict(zip(years, links))
    
            feriados_nacionais = pd.concat([tabela_feriado_nacional_por_ano(year) for year in year_link_dict.keys()], ignore_index = True)

        else: 

            feriados_nacionais = pd.DataFrame([]) #preferir retornar dataframe vazio em vez de quebrar lógica

    except Exception as e:
        
        print(f'Erro: {str(e)}')

        feriados_nacionais = pd.DataFrame([]) #preferir retornar dataframe vazio em vez de quebrar lógica

    return feriados_nacionais

if __name__ == '__main__':

    try:
        # estanciamento do constructor da classe DataOperator
        bs2 = do.DataOperator('feriados_nacionais')
        print('---------------------------------------------------------------------------------------------------------------')
        #Crawler que faz a coleta de dados em um pandas dataframe    
        df_pandas = crawler()
        print('---------------------------------------------------------------------------------------------------------------')
        #Se algum crawler não trazer nenhum dado, o dataframe anterior não será substituído por algum dataframe vazio
        if len(df_pandas) == 0:
            print('Dataframe vazio.')
            pass
        else:
            print(f'Total de registros no dataframe: {len(df_pandas)}')
            df_spark = bs2.sqlContext.createDataFrame(df_pandas)
            bs2.writeBigQuery(df=df_spark, partition=None, mode="overwrite", dataset_table="silver_dw_dim.dim_feriados")

    except Exception as e:        
        print(f"{str(datetime.now()).split('.')[0]} - {str(e)}")

    finally:
        bs2.stopSparkDataOperator()
        print(f"{str(datetime.now()).split('.')[0]} - Contexto SPARK FINALIZADO.\n\n")
        print(">>>>>>>>>>>>>>>>>>>>> FIM <<<<<<<<<<<<<<<<<<<<")


        