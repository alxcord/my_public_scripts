# Databricks notebook source
# MAGIC %md
# MAGIC # Pyspark Guide - Basico
# MAGIC Por Alex Almeida Cordeiro
# MAGIC 01/2020

# COMMAND ----------

# MAGIC %md
# MAGIC ## O básico
# MAGIC 
# MAGIC ### Executando um Workbook
# MAGIC 
# MAGIC Execução efêmera, o conteúdo das variáveis será perdido
# MAGIC ```python
# MAGIC dbutils.notebook.run('/comum/setup_conn_sadatalakevcbr.ipynb',60)
# MAGIC ```
# MAGIC 
# MAGIC Execução semelhante a um include (uma célula somente com esse código):
# MAGIC 
# MAGIC ```
# MAGIC %run "/comum/eng_shared_functions.ipynb"
# MAGIC ```
# MAGIC 
# MAGIC ### Exibindo o conteúdo de um dataframe:
# MAGIC ```python
# MAGIC df.show(100) # lista 100 linhas
# MAGIC df.show(100, False) # lista 100 linhas, não trunca campos
# MAGIC ```
# MAGIC 
# MAGIC ### Mostrando estrutura de um dataframe
# MAGIC ```python
# MAGIC df.printSchema()
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC ### 
# MAGIC ```python
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recebendo parametros e retornando resultados
# MAGIC Recebe um parametro, converte para numerico, consiste no caso de parametro invalido, ao final do notebook termina passando Ok como retorno.
# MAGIC 
# MAGIC ```python
# MAGIC dbutils.widgets.text("param_num", "")
# MAGIC try:
# MAGIC   my_param  = int(dbutils.widgets.get("param_num"))
# MAGIC   flag_no_param = False
# MAGIC except ValueError:
# MAGIC   my_param = 0
# MAGIC   flag_no_param = True
# MAGIC if my_param > 99:
# MAGIC   raise ValueError("Parametros invalidos")
# MAGIC 
# MAGIC muitas_operacoes_depois()
# MAGIC 
# MAGIC dbutils.notebook.exit("Ok")
# MAGIC   
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções utilitárias (dbutils.fs)
# MAGIC 
# MAGIC ### Criar diretérios de forma recursiva
# MAGIC ```python
# MAGIC dbutils.fs.mkdirs('my/path')
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC ### Gravando um arquivo simples
# MAGIC Sim, ele vai gerar um arquivo simples, eu podia usar um dataframe ou RDD para isso? Podia mas ia gerar uma pasta com um monte de arquivos dentro.
# MAGIC ```python
# MAGIC my_file_content = 'Hello!! Good by!!'
# MAGIC dbutils.fs.put('my/file/path/and/name.txt', my_file_content, overwrite = True) 
# MAGIC ```
# MAGIC 
# MAGIC ### Lendo um arquivo simples (texto)
# MAGIC Ok, está um pouco fora do lugar mas faz sentido estar aqui por causa da gravação
# MAGIC ```python
# MAGIC rdd = spark.sparkContext.wholeTextFiles('my/file/path/and/name.txt')
# MAGIC my_file_content = rdd.collect()[0][1]
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lendo e gravando
# MAGIC TODO: Terminar
# MAGIC 
# MAGIC ### Lendo um dataset para um dataframe
# MAGIC 
# MAGIC #### json
# MAGIC 
# MAGIC ```python
# MAGIC spark.read.json('my/folder')
# MAGIC ```
# MAGIC 
# MAGIC #### csv
# MAGIC ```python
# MAGIC spark.read.csv('my/folder')
# MAGIC spark.read.csv('my/folder', header=True) # com cabeçalho
# MAGIC ```
# MAGIC 
# MAGIC #### parquet
# MAGIC ```python
# MAGIC df = spark.read.format("parquet").load('my/folder')
# MAGIC ```
# MAGIC 
# MAGIC #### delta
# MAGIC ```python
# MAGIC df = spark.read.format("delta").load('my/folder')
# MAGIC ```
# MAGIC 
# MAGIC ### Utilizando um esquema predefinido
# MAGIC ```python
# MAGIC spark.read.schema(my_schema).json(source_dir).createOrReplaceTempView('RawDetailed')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## criando um dataframe de teste

# COMMAND ----------

# https://dwgeek.com/replace-pyspark-dataframe-column-value-methods.html/
from pyspark.sql.functions import *

data = []
for ano in range(2020, 2023):
  for id in range (100):
    data.append ((ano, (ano - 2020) * 100 + id, str(id)))
df = spark.createDataFrame(data, ["year", "id", "d_id"])
df.show(5,False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Operações com Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manipulando dataframes
# MAGIC 
# MAGIC ### Preenche campos vazios, trata NAs:
# MAGIC Nesse exemplo troca os campos vazios de MyField1 por 0 e de MyField2 por 'NULO'
# MAGIC ```python
# MAGIC my_df = my_df.fillna({'MyField1': 0, 'MyField2': 'NULO'})
# MAGIC ```
# MAGIC 
# MAGIC ### Eliminando colunas
# MAGIC TODO: testar se dá para fazer com uma lista
# MAGIC 
# MAGIC ```python
# MAGIC my_df = my_df.drop(col('tag2')).drop(col('time2'))
# MAGIC ```
# MAGIC 
# MAGIC ### Criando um novo campo a partir da Substring de outro
# MAGIC Esse cria uma coluna ano com os primeiros quatro caracteres de uma data.
# MAGIC ```python
# MAGIC df = df.withColumn('Ano', substring('Data', 1,4)) \
# MAGIC ```
# MAGIC       
# MAGIC 
# MAGIC ### Join
# MAGIC Efetuando o join entre dois datasets
# MAGIC ```python
# MAGIC df_open_time = df_all.join(df_tag_list, (col('tag') == col('tag2')) & (col('time') >= col('time2')))
# MAGIC ```
# MAGIC 
# MAGIC ### Eliminando duplicidades por uma chave
# MAGIC Elimna duplicidades por tag e time.
# MAGIC ```python
# MAGIC df_raw = df_raw.drop_duplicates(subset=['tag', 'time']) 
# MAGIC ```
# MAGIC 
# MAGIC ### Exemplos de agregação e seleção
# MAGIC Aqui filtramos uma coluna (time), agrupamos pela TAG agregando time pelo minimo, renomeamos tag para tag2 e a coluna resultante "min(time)" para time2
# MAGIC ```python
# MAGIC df_tag_list = df_all.filter((col('Next_time') == maximum_timestamp)) \
# MAGIC               .filter((col('time') < '9999-12-31 99:99:99')) \
# MAGIC               .groupby('tag').agg({'time': 'min'}) \
# MAGIC               .select(col('tag').alias('tag2'), col('min(time)').alias('time2')) 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemplos de manipulação avançada
# MAGIC 
# MAGIC ### Clausula Window: diferença entre datas entre dois registros
# MAGIC Aqui vamos calcular a diferença em segundos de um campo data de um registro para o outro a partir de uma chave, ou seja, a partir do campo chave teremos a proxima data
# MAGIC Baseado em em https://www.arundhaj.com/blog/calculate-difference-with-previous-row-in-pyspark.html
# MAGIC ```python
# MAGIC from pyspark.sql.window import Window
# MAGIC w_lag = Window.partitionBy("Chave").orderBy(desc('Time'))
# MAGIC my_df = my_df.withColumn("Next_time", lag(df_raw['Time']).over(w_lag))
# MAGIC my_df = my_df.withColumn( "Time_gap", df_raw['Next_time'].cast("long") - df_raw['Time'].cast("long"))
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC ### Eliminando chave duplicada pelo menor timestamp
# MAGIC Aqui temos um exemplo onde filtramos um range de datas, criamos uma coluna de ranking por um timestamp agrupado por duas chaves (id_doc, id_item), depois filtramos onde ranking = 1 (somente o timestamp mais atual) e por fim eliminamos o ranking
# MAGIC ```python
# MAGIC from pyspark.sql.window import Window
# MAGIC 
# MAGIC processing_start_date = '2021-01-01'
# MAGIC processing_end_date = '2021-01-05'
# MAGIC timestamp_field = 'ts_insert_raw'
# MAGIC key_fields = ['id_doc', 'id_item']
# MAGIC window = Window.partitionBy(key_fields).orderBy(col(timestamp_field).desc())
# MAGIC df = df.filter(col(raw_layer_date_filter_field)>= processing_start_date) \
# MAGIC       .filter(col(raw_layer_date_filter_field)<= processing_end_date) \
# MAGIC       .withColumn("rank_temp", rank().over(window)).filter(col("rank_temp") == 1).drop("rank_temp") 
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC ### Monstrando contagem por mês/ano a partir de uma data
# MAGIC Ainda ordena decrescente
# MAGIC ```python
# MAGIC df_DadosCPS.withColumn('ANO_MES_REMESSA', substring('DATA_REMESSA', 1,7)) \
# MAGIC   .groupBy('ANO_MES_REMESSA') \
# MAGIC   .agg({'RESULTADO':'avg', "*":'count'}) \
# MAGIC   .select('ANO_MES_REMESSA', col('avg(RESULTADO)').alias('MEDIA_RESULTADO'), col('count(1)').alias('QUANTIDADE')) \
# MAGIC   .sort(col('ANO_MES_REMESSA').desc()) \
# MAGIC   .show()
# MAGIC ```
# MAGIC 
# MAGIC ```
# MAGIC +---------------+------------------+----------+
# MAGIC |ANO_MES_REMESSA|   MEDIA_RESULTADO|QUANTIDADE|
# MAGIC +---------------+------------------+----------+
# MAGIC |        2020-12|29.810059438896857|      4206|
# MAGIC |        2020-11| 33.53508969653114|     19343|
# MAGIC |        2020-10|34.256294685788326|     23917|
# MAGIC |        2020-09| 36.06784174470488|     15911|
# MAGIC |        2020-08|              13.3|         1|
# MAGIC |        2020-07|              34.3|         4|
# MAGIC |        2020-05|             43.56|         1|
# MAGIC |        2020-04|              28.4|         1|
# MAGIC +---------------+------------------+----------+
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibição de dados
# MAGIC 
# MAGIC Montrando um dataframe formatado (data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mais material
# MAGIC 
# MAGIC - http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.Column.alias

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando tabelas e views
# MAGIC 
# MAGIC ### Criando uma view a partir de um dataframe:
# MAGIC 
# MAGIC ```python
# MAGIC my_df.createOrReplaceTempView('MyTable')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rodando código SQL
# MAGIC Depois que você cria tabelas e views pode rodar SQL normalmente usando %sql na celula, no pyspark pode rodar assim:
# MAGIC 
# MAGIC ```python
# MAGIC my_df = spark.sql('SELECT * FROM MINHA_TABELA')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operações com delta
# MAGIC 
# MAGIC ### Verificando se uma pasta é delta
# MAGIC retorna True ou False
# MAGIC ``` python
# MAGIC DeltaTable.isDeltaTable(spark, 'my/path')
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta: Finalizando o tratamento dos dados
# MAGIC 
# MAGIC ### Otiminzando o tamanho dos arquivos
# MAGIC Elimina arquivos pequenos concatenando-os e gerando arquimos maiores
# MAGIC https://docs.databricks.com/delta/optimizations/file-mgmt.html#delta-optimize
# MAGIC ```sql 
# MAGIC %sql
# MAGIC OPTIMIZE CleansedPiOsiDetails
# MAGIC ```
# MAGIC Para rodar do pyspark (creio que não há uma função especifica):
# MAGIC ```python
# MAGIC folder = '/path/to/data'
# MAGIC spark.sql("OPTIMIZE delta.`{0}`".format(folder))
# MAGIC ```
# MAGIC 
# MAGIC ### Elimina arquivos não usados 
# MAGIC Elimina arquivos não ativos, predemos o timetravel mas reduzimos consumo espaço, retem 7 dias. 
# MAGIC ```sql 
# MAGIC %sql
# MAGIC VACUUM CleansedPiOsiDetails
# MAGIC ```
# MAGIC ```python
# MAGIC deltaTable.vacuum()     # vacuum files not required by versions more than 7 days old
# MAGIC deltaTable.vacuum(100)  # vacuum files not required by versions more than 100 hours old
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel
# MAGIC 
# MAGIC Vamos supor que temos uma tabela delta
# MAGIC ```python
# MAGIC delta_df = DeltaTable.forPath(spark, "/mnt/data/delta")
# MAGIC ```
# MAGIC 
# MAGIC ### Mostrando a lista de versões
# MAGIC ```python
# MAGIC delta_df.history().show(10, True)
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC ```
# MAGIC +-------+-------------------+----------------+--------------------+---------+--------------------+----+------------------+--------------------+-----------+-----------------+-------------+--------------------+------------+
# MAGIC |version|          timestamp|          userId|            userName|operation| operationParameters| job|          notebook|           clusterId|readVersion|   isolationLevel|isBlindAppend|    operationMetrics|userMetadata|
# MAGIC +-------+-------------------+----------------+--------------------+---------+--------------------+----+------------------+--------------------+-----------+-----------------+-------------+--------------------+------------+
# MAGIC |      2|2021-01-20 19:41:16|3998117388507181|alex.cordeiro.ac1...|    MERGE|[predicate -> (((...|null|[1481698408020379]|0114-184800-haste129|          1|WriteSerializable|        false|[numTargetRowsCop...|        null|
# MAGIC |      1|2021-01-20 19:03:00|3998117388507181|alex.cordeiro.ac1...|    MERGE|[predicate -> (((...|null|[1481698408020379]|0114-184800-haste129|          0|WriteSerializable|        false|[numTargetRowsCop...|        null|
# MAGIC |      0|2021-01-20 19:00:47|3998117388507181|alex.cordeiro.ac1...|    WRITE|[mode -> ErrorIfE...|null|[1481698408020379]|0114-184800-haste129|       null|WriteSerializable|         true|[numFiles -> 1, n...|        null|
# MAGIC +-------+-------------------+----------------+--------------------+---------+--------------------+----+------------------+--------------------+-----------+-----------------+-------------+--------------------+------------+
# MAGIC ```
# MAGIC 
# MAGIC trazendo uma versão específica:
# MAGIC ```python
# MAGIC version_1 = spark.read.format("delta").option("versionAsOf",1).load("/mnt/data/delta")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta: Mais material
# MAGIC - https://docs.delta.io/latest/delta-utility.html#table-utility-commands

# COMMAND ----------

# MAGIC %md
# MAGIC # Metadados

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Metadados - StructField
# MAGIC Documentação util:
# MAGIC - https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/
# MAGIC 
# MAGIC ### Criando uma estrutura
# MAGIC 
# MAGIC ```python
# MAGIC from pyspark.sql.types import StructType,StructField, StringType, IntegerType
# MAGIC schema = StructType([ 
# MAGIC     StructField("firstname",StringType(),True), 
# MAGIC     StructField("middlename",StringType(),True), 
# MAGIC     StructField("lastname",StringType(),True), 
# MAGIC     StructField("id", StringType(), True), 
# MAGIC     StructField("gender", StringType(), True), 
# MAGIC     StructField("salary", IntegerType(), True) 
# MAGIC   ])
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC ### Verificando se um campo existe em um Dataframe
# MAGIC Se você deseja realizar algumas verificações nos metadados do DataFrame, por exemplo, se uma coluna ou campo existe em um DataFrame ou tipo de dados da coluna; podemos fazer isso facilmente usando várias funções em SQL StructType e StructField.
# MAGIC ```python
# MAGIC print(df.schema.fieldNames.contains("firstname"))
# MAGIC print(df.schema.contains(StructField("firstname",StringType,true)))
# MAGIC ```
# MAGIC 
# MAGIC ### Alterando o metadado de um Dataframe
# MAGIC Também pode ser usado para renomear
# MAGIC ```python
# MAGIC def set_metadata(my_metadata):
# MAGIC   my_metadata["Teste"] = "Ok"
# MAGIC   return my_metadata
# MAGIC 
# MAGIC new_df = df
# MAGIC new_df = new_df.select([col(c).alias(c, metadata = set_metadata(new_df.schema[c].metadata)  ) for c in new_df.columns])
# MAGIC 
# MAGIC for sc_col in new_df.schema:
# MAGIC   print (sc_col.jsonValue())
# MAGIC ```
# MAGIC Também pode ser usado para renomear as colunas com:
# MAGIC ```python
# MAGIC new_df = new_df.select([col(c).alias(c + "_new", metadata = set_metadata(new_df.schema[c])  ) for c in 
# MAGIC ```

# COMMAND ----------

# https://dwgeek.com/replace-pyspark-dataframe-column-value-methods.html/
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

schema = StructType([ 
    StructField("firstname",StringType(),True, metadata = {"desc":"Nome"}), 
    StructField("middlename",StringType(),True), 
    StructField("lastname",StringType(),True), 
    StructField("id", StringType(), True), 
    StructField("gender", StringType(), True), 
    StructField("salary", IntegerType(), True) 
  ])

data = [("João", "Milho", "de Freitas", 1, "M", 2000),
        ("Maria", "de Lourdes", "de Freitas", 1, "M", 3000),
        ("Michael","", "Rose","40288","M",4000),
        ("Robert","","Williams","42114","M",4000),
        ("Maria","Anne","Jones","39192","F",4000),
        ("Jen","Mary","Brown","","F",3000),
       ]

df = spark.createDataFrame(data, schema )
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Alterando e mostrando o metadado de um dataframe
def set_metadata(my_metadata):
  my_metadata["Teste"] = "Ok"
  return my_metadata

new_df = df
new_df = new_df.select([col(c).alias(c, metadata = set_metadata(new_df.schema[c].metadata)  ) for c in new_df.columns])

for sc_col in new_df.schema:
  print (sc_col.jsonValue())


# COMMAND ----------

# DBTITLE 1,Renomeando uma coluna com base no metadado
for sc_col in new_df.schema:
  if "newname" in sc_col.metadata and sc_col.metadata["newname"] != "" and sc_col.metadata["newname"] != sc_col.name:
    new_df = new_df.withColumnRenamed(sc_col.metadata["newname"], col(sc_col.name))
  
for sc_col in new_df.schema:
  print (sc_col.jsonValue())                             
new_df.show(2)
new_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark API - Metadados de tabelas
# MAGIC TODO: Checar & testar
# MAGIC 
# MAGIC Ver https://www.learningjournal.guru/courses/spark/spark-foundation-training/spark-data-types-and-metadata/
# MAGIC 
# MAGIC Esse código é scala???
# MAGIC ```python
# MAGIC spark.catalog.listDatabases
# MAGIC // same as SHOW DATABASES
# MAGIC //This API gives you a dataset for the list of all databases. You can display the list using the show method.
# MAGIC spark.catalog.listDatabases.show
# MAGIC //You can collect it back to the master node as a Scala Array.
# MAGIC val dbs = spark.catalog.listDatabases.collect
# MAGIC //Then you can loop through the array and apply a function on each element. Let's apply the println.
# MAGIC dbs.foreach(println)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Mais operações com metadados
# MAGIC 
# MAGIC ### Como manter o metadado durante um ajuste
# MAGIC Ao ajustar uma coluna com o withColumn, por exemplo, todos os seus Metadados são perdidos.
# MAGIC 
# MAGIC ```python
# MAGIC df = df.withColumn('NF', rtrim('NF').alias("", metadata = df.schema['NF'].metadata)) \
# MAGIC ```

# COMMAND ----------

# MAGIC %md 
# MAGIC # Outros

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outras operações interessantes
# MAGIC 
# MAGIC ### Criando uma função aplicavel a um dataframe
# MAGIC Ver 
# MAGIC https://stackoverflow.com/questions/46667810/how-to-update-pyspark-dataframe-metadata-on-spark-2-1
# MAGIC 
# MAGIC ```python
# MAGIC def withMeta(self, alias, meta):
# MAGIC     sc = SparkContext._active_spark_context
# MAGIC     jmeta = sc._gateway.jvm.org.apache.spark.sql.types.Metadata
# MAGIC     return Column(getattr(self._jc, "as")(alias, jmeta.fromJson(json.dumps(meta))))
# MAGIC 
# MAGIC Column.withMeta = withMeta
# MAGIC 
# MAGIC # new metadata:
# MAGIC meta = {"ml_attr": {"name": "label_with_meta",
# MAGIC                     "type": "nominal",
# MAGIC                     "vals": [str(x) for x in range(6)]}}
# MAGIC 
# MAGIC df_with_meta = df.withColumn("label_with_meta", col("label").withMeta("", meta))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Erros e soluções
# MAGIC 
# MAGIC ### TypeError: 'StructField' object is not callable usando a função col
# MAGIC Exemplo: df.filter(col('CAMPO') >= 1234).show()
# MAGIC Olhe no seu cósigo se você tem alguma função que usa uma variável chamada col... 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mais material
# MAGIC 
# MAGIC - spark-gotchas -- https://github.com/awesome-spark/spark-gotchas/blob/master/06_data_preparation.md#python
# MAGIC - https://data.solita.fi/pyspark-execution-logic-code-optimization/
# MAGIC - https://medium.com/analytics-vidhya/getting-hands-dirty-in-spark-delta-lake-1963921e4de6
