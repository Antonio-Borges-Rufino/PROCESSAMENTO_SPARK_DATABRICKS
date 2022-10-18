# DETALHES
1. O PROCESSAMENTO FOI INTEIRAMENTE FEITO NA PLATAFORMA FREE DO DATABRICKS
2. O DATASET UTILIZADO FOI O coronavirusdataset
3. OS SUBSET UTILIZADOS FORAM: case, PatientInfo, PatientRoute
4. [O CÓDIGO COMPLETO COM AS SAIDAS PODE SER VISTO AQUI](https://github.com/Antonio-Borges-Rufino/PROCESSAMENTO_SPARK_DATABRICKS/blob/main/estudo.ipynb)
5. IMPORTANDO O DATASET PARA RESPONDER ALGUMAS PERGUNTAS POSTERIORES:
```
df_caso = spark.read.csv("dbfs:/databricks-datasets/COVID/coronavirusdataset/Case.csv", header="true", inferSchema="true")
df_paciente = spark.read.csv("dbfs:/databricks-datasets/COVID/coronavirusdataset/PatientInfo.csv", header="true", inferSchema="true")
df_caminho = spark.read.csv("dbfs:/databricks-datasets/COVID/coronavirusdataset/PatientRoute.csv", header="true", inferSchema="true")
```
6. CRIANDO AS TABLES TEMPORARIAS NO SPARK PARA UTILIZAR O RDD
```
df_caso.createOrReplaceTempView("caso")
df_paciente.createOrReplaceTempView("paciente")
df_caminho.createOrReplaceTempView("caminho")
```
# PERGUNTAS
### Quantas linhas existem no dataset Case.csv?
```
spark.sql("SELECT COUNT(*) FROM caso").show()
```
### No dataset Case.csv, a média de confirmed é:
```
spark.sql("SELECT AVG(confirmed) FROM caso").show()
```
### No dataset Case.csv, a cidade (city) que apresentou mais casos (confirmed) foi:
```
spark.sql("SELECT city, COUNT(*) as contador FROM caso GROUP BY city ORDER BY contador DESC").show()
```
### No dataset Case.csv, a média de casos (confirmed), na província (province) de Seoul foi de:
```
spark.sql("SELECT AVG(confirmed) FROM caso WHERE province='Seoul'").show()
```
### Quantas linhas existem no dataset PatientInfo.csv?
```
spark.sql("SELECT COUNT(*) FROM paciente").show()
```
### No dataset PatientInfo.csv, assinale a alternativa VERDADEIRA.
```
spark.sql("SELECT DISTINCT(infection_case) FROM paciente").show()
spark.sql("SELECT DISTINCT(sex) FROM paciente").show()
spark.sql("SELECT DISTINCT(state) FROM paciente").show()
```
### No dataset PatientInfo.csv, pessoas do sexo (sex) feminino (female), da cidade (city) Jongno-gu, e do grupo de idade (age) 10s são:
```
spark.sql("SELECT COUNT(*) FROM paciente WHERE sex='female' and city='Jongno-gu' and age='10s'").show()
```
### Quantas linhas existem no dataset PatientRoute.csv?
```
spark.sql("SELECT COUNT(*) FROM caminho").show()
```
### No dataset PatientRoute.csv, a maior latitude encontrada é de:
```
df_caminho.printSchema()
spark.sql("SELECT latitude FROM caminho ORDER BY latitude DESC LIMIT 2").show()
```
### No dataset PatientRoute.csv, a menor longitude encontrada é de:
```
spark.sql("SELECT longitude FROM caminho ORDER BY longitude LIMIT 2").show()
```
### Pacientes detectados (type) no aeroporto (airport), e que tiveram caso de infecção (infection_case) como contato com pacientes (contact with patient) retornam uma contagem de:
```
spark.sql("SELECT DISTINCT(type) FROM caminho").show()
df_paciente.printSchema()
spark.sql("SELECT COUNT(*) from caminho JOIN paciente ON caminho.patient_id = paciente.patient_id WHERE caminho.type='airport' and paciente.infection_case='contact with patient'").show()
```
