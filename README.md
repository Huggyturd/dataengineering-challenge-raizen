Para realizar esse desafio comecei com as seguintes perguntas:
Como fazer uma pipeline no airflow?
Como processar o cache do pivor de um xls?
Como rodar pipeline do airflow no docker/kubernets?
Quais são as melhores práticas de ETL no airflow?

Depois eu defini os passos iniciais:
Entender o problema.
Entender o que o recrutador quer com o teste.
Entender o dado
Entender como extrair esse dado
Entender como processar esse dado
Entender como carregar esse dado em determinado local
Extrair e transformar em dataframe os dados do segundo sheet para derivativos do óleo e dados do terceiro sheet para etanol.

Depois eu defini as ferramentas a serem utilizadas durante o processo de desenvolvimento:
Airflow
Python
Pandas
Docker + libreoffice
openpyxl
requests

Depois eu defini os passos para criação do script teste para realizar a extração
Achar um forma de ler os dados do xls com python
Colocar os dados num dataframe pandas
Processar o dataframe
Salvar esse dataframe em formato parquet

Após vários testes com bibliotecas sem sucesso, decidir usar o DockerOperator do Airflow + um container com libreoffice para converter o arquivo pra um formato mais recente o xlsx baseado no seguinte comando:
    docker run --rm -it -v $(pwd):/tmp --name libreoffice-headless ipunktbs/docker-libreoffice-headless:latest --convert-to xlsx "vendas-combustiveis-m3.xls"

Através do arquivo de ouput no formato xlsx é possível pegar os dados em cache através da biblioteca openpyxl, com isso o script inicial foi terminado.
Após isso adaptei o script para rodar como uma pipeline airflow com os métodos e processos do airflow.
Depois eu transformei tudo isso em container criando um Dockerfile e criei um docker-compose.yaml para subir um ambiente de teste.

Comandos:
docker build . -f Dockerfile --tag airflow-raizen-challenge:0.0.1
docker-compose up -d

Algumas pesquisas que lembrei de salvar no caminho:
Pesquisa no Google: read xlsx python
https://pythonbasics.org/read-excel/
Pesquisa: panda print sheets excel
https://stackoverflow.com/questions/17977540/pandas-looking-up-the-list-of-sheets-in-an-excel-file
Pesquisa: How to extract Excel PivotCache in python
https://stackoverflow.com/questions/4433952/extracting-data-from-excel-pivot-table-spreadsheet-in-linux
Pesquisa: how to convert xls to xlsx
https://www.studytonight.com/post/converting-xlsx-file-to-csv-file-using-python
https://marclamberti.com/blog/how-to-use-dockeroperator-apache-airflow/
https://marclamberti.com/blog/apache-airflow-best-practices-1/
https://towardsdatascience.com/how-to-use-airflow-without-headaches-4e6e37e6c2bc
https://www.youtube.com/watch?v=wDr3Y7q2XoI

