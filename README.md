Processo para desenvolvimento da pipeline ETL do desafio:

Pesquisas inicias:
Como fazer pipeline de ETL com airflow
Como processar o cache de pivot de um xls
Como rodar pipeline de airflow no kubernets
Melhores práticas de ETL no airflow
Melhorés práticas de convenção de nomes de DAGs no airflow

Passos iniciais:
Entender o problema.
Entender o que o recrutador quer com o teste.
Entender o dado
Entender como extrair esse dado
Entender como processar esse dado
Entender como carregar esse dado em determinado local

Passos para extração:
Extrair e transformar em dataframe os dados do segundo sheet para derivativos do óleo e dados do terceiro sheet para etanol.

Ferramentas a serem utilizadas:
Airflow
Python
Pandas
Docker + libreoffice
openpyxl
requests

Passos para criar o script de extração
Achar um forma de ler os dados do xls
Colocar num dataframe pandas
Processar o dataframe
Salvar esse dataframe em formato parquet

---------------------------------------------------

Converter xls para xlsx com libreoffice

docker run --rm -it -v $(pwd):/tmp --name libreoffice-headless ipunktbs/docker-libreoffice-headless:latest --convert-to xlsx "vendas-combustiveis-m3.xls"

---------------------------------------------------

Pesquisa:
Pesquisa no Google: read xlsx python
https://pythonbasics.org/read-excel/
Pesquisa: panda print sheets excel
https://stackoverflow.com/questions/17977540/pandas-looking-up-the-list-of-sheets-in-an-excel-file
How to extract Excel PivotCache in python
https://stackoverflow.com/questions/4433952/extracting-data-from-excel-pivot-table-spreadsheet-in-linux
Pesquisa: how to convert xls to xlsx
https://www.studytonight.com/post/converting-xlsx-file-to-csv-file-using-python
https://marclamberti.com/blog/how-to-use-dockeroperator-apache-airflow/
https://marclamberti.com/blog/apache-airflow-best-practices-1/
https://towardsdatascience.com/how-to-use-airflow-without-headaches-4e6e37e6c2bc
https://www.youtube.com/watch?v=wDr3Y7q2XoI

Comandos:
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker build . -f Dockerfile --tag airflow-raizen-challenge:0.0.1
docker-compose up -d