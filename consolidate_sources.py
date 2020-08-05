
"""
consolidate_sources.py
Consolida visão dos vários documentos de descrição de relatorios em um unico grupo de arquivos
Mais fácil de ler por uma ferramenta de BI.
Esse script resolve um monte de problemas 

Por Alex Cordeiro



"""

import pandas as pd, os

print('Entre com o diretório origem:')
source_path = input()
print('Entre com o diretório destino (enter -> assume igual o origem:')
target_path = input()
If target_path == "":
	target_path = source_path

dir = os.listdir( source_path )
print(source_path)
df_reports = pd.DataFrame()
df_rep_kpi = pd.DataFrame()
df_datasources = pd.DataFrame()
df_rep_fields = pd.DataFrame()

for file in dir:
    full_path = os.path.join(source_path, file)
    if os.path.isfile(full_path) and file.endswith('.xlsx') and not file.startswith('consolidate'):
        print ("Processando arquivo '{}'".format(file))
        df_reports_temp = pd.read_excel (full_path, sheet_name='Relatórios')
        df_reports_temp = df_reports_temp.rename({"Relatorio":"report", 
                                                  "Ferramenta":"current_bi_tool", 
                                                  "area resp":"resp_department", 
                                                  "Resp Relatório": "resp_person", 
                                                  "Criticidade": "criticality", 
                                                  "Envio por e-mail?":"email_sent", 
                                                  "Hora atualização - Ideal":"update_time_ideal", 
                                                  "Hora at. Desejada": "update_time_desired", 
                                                  "Hora envio - ANS": "update_time_limit", 
                                                  "Publico": "consumers", 
                                                  "Quantidade de usuários":"consume_user_quantity", 
                                                  "Frequencia de uso": "consume_frequency"})

        df_datasources_temp = pd.read_excel (full_path, sheet_name='Fontes de dados')
        df_datasources_temp =  df_datasources_temp.rename({"Fonte de dados":"datasource", 
                                                 "Sistema Origem":"sourcesystem", 
                                                 "Descrição fonte":"datasource_desc", 
                                                 "Tipo":"type", 
                                                 "Servidor/site":"server_url", 
                                                 "Tabela/caminho":"table_path", 
                                                 "Tabela/caminho/ SQL CRC":"table_path_crc", 
                                                 "Como é carregado?":"how_is_loaded", 
                                                 "Periodicidade":"frequency", 
                                                 "Visibilidade":"visibility"})
        
        df_rep_fields_temp = pd.read_excel (full_path, sheet_name='Fontes de dados x Campos')
        df_rep_fields_temp =  df_rep_fields_temp.rename({"Fontes":"datasource", 
                                                 "Campo":"filed", 
                                                 "Descrição":"description", 
                                                 "Chave?":"primary_key", 
                                                 "Tipo":"type", 
                                                 "Tamanho":"size", 
                                                 "Decimal":"decimal",
                                                 "Sensível?":"is_sensible"})
        
        # converte linhas para colunas
        df_rep_kpi_temp = pd.read_excel (full_path, sheet_name='Relatórios x Indicadores')
        df_rep_kpi_temp = df_rep_kpi_temp.rename(columns={
                r'Relatorio': 'report_name', 
                r'Nome Indicador/ Metrica': 'kpi_name', 
                r'Descrição': 'kpi_desc', 
                r'Responsavel por Preencher': 'kpi_resp_filling', 
                r'Area responsável': 'kpi_resp_area', 
                r'Responsável negócio': 'kpi_resp_emploee'})
            
        df_rep_kpi_temp = df_rep_kpi_temp.melt(id_vars=['report_name', 
                                      'kpi_name', 
                                      'kpi_desc', 
                                      'kpi_resp_filling', 
                                      'kpi_resp_area', 
                                      'kpi_resp_emploee'], 
                            var_name="Datasource", 
                            value_name="Value")
        #df.drop(df[df.score < 50].index, inplace=True)
        #df = df.drop(df[df.score < 50].index)
        #print(train[train["Electrical"].isnull()][null_columns])
        df_rep_kpi_temp = df_rep_kpi_temp.drop(df_rep_kpi_temp[df_rep_kpi_temp.Value.isnull() ].index)
        df_rep_kpi_temp = df_rep_kpi_temp.drop(df_rep_kpi_temp[df_rep_kpi_temp.Value.str.contains('N', 'n') ].index)

        df_reports = pd.concat([df_reports, df_reports_temp])
        df_rep_kpi = pd.concat([df_rep_kpi , df_rep_kpi_temp])
        df_datasources = pd.concat([df_datasources, df_datasources_temp])
        df_rep_fields = pd.concat([df_rep_fields, df_rep_fields_temp])

#print(list(df_rep_kpi_temp.columns))
#print(df_rep_kpi_temp)

df_reports["consume_user_quantity"]  = df_reports["consume_user_quantity"].str.extract(r'^(\d{6})', expand=False)

df_reports.to_excel(target_path + 'consolidate_reports.xlsx', index = False) 
df_rep_kpi.to_excel(target_path  + 'consolidate_rep_kpi.xlsx', index = False)
df_datasources.to_excel(target_path  + 'consolidate_datasource.xlsx', index = False) 
df_rep_fields.to_excel(target_path  + 'consolidate_rep_fields.xlsx', index = False)
print("Finalizado com sucesso!")

#df_rep_kpi_temp.to_csv(output_file_name +'.csv', encoding='utf-8', sep = ';', quotechar = '"')
#compression_opts = dict(method='zip',
#                       archive_name='out.csv')  
#df.to_csv('out.zip', index=False,
#          compression=compression_opts)          
#df_rep_kpi_temp.head()
        #df_rep_kpi_temp.n
        


