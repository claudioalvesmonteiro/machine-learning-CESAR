'''
ML CESAR
'''

#----------------------------- ACCESSING CLOUD DATA ON AMAZON S3

# from gcsfs import GCSFileSystem    -->> Acessar arquivos no google cloud
from s3fs import S3FileSystem      #-->> Acessar arquivos no amazon s3
import time
import pandas as pd

# gcs = GCSFileSystem(token='cloud', access='read_only')
s3 = S3FileSystem(anon=True)     # Acesso em modo anônimo # PERMISSAO

folder_name = 'cesarschool-data-samples/sample01' # bucket/pasta

# Listar arquivos armazenados na CLOUD
# details = gcs.ls(folder_name, detail=True)    #-->> google cloud
details = s3.ls(folder_name, detail=True)    #-->> amazon
print(details)

# A variável details é uma lista de dicionários de informações sobre os arquivos.
# Acesso aos campos pela sintaxe details['nome_do_campo']
for file in details:
    # Imprime nome e tamanho do arquivo em MBytes
    print(file['Key'], round(file['Size']/(1024*1024.0), 2), 'MBytes')

# Abre arquivos para leitura e imprime primeiras linhas
start_time = time.time()
headers = []
for file in details:
    current_header = ''
    # Se for um arquivo
    if file['Size'] > 0:
        # Abre arquivo para leitura
        current_file = s3.open(file['Key'], 'rb')
        #print(current_file)
        print(file['Key'])
        headers.append('\n' + file['Key'] + '\n')
        print(current_file.readline())
        for i in range(6):
            headers.append(str(current_file.readline()) + '\n')
        current_file.close()
        headers.append(current_header)

#print(''.join(headers))
end_time = time.time()
print("Total execution time: {}".format(end_time - start_time))

#------------------------------- PANDAS PROCESSING

# LOAD DATA
start_time = time.time()

details = s3.ls('cesarschool-data-samples/sample01')
with s3.open(details[1], 'rb') as f:
    df = pd.read_csv(f)

end_time = time.time()
print("Total execution time: {}".format(end_time - start_time))

# Visualize data

df.head() # first lines
df.tail() # final lines

df.shape() # dimensions of data
df.info() # type of data
df.describe() # descriptive stats

df.corr() # carrelate columns

# filter one column [Pandas Series]
df['col1']

# filter two or more columns [Pandas DF]
df[[]'col1', 'col2']]

# ordenar valores
df.sort_values('col1')

# Grouing values
gdf = df.groupby('col1')
gdf.mean()
gdf.sum()

df.groupby(['col1', 'col2'])
