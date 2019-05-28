import pandas as pd

d = {
'Nome': ['Claudio', 'Ge', 'Zé'],
'Idade': [22, 20, 30],
'Mulher': [False, True, False]
}

data = pd.DataFrame(d)
print(data)

#=========================#
# data exploring
#=========================#

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# load
df = pd.read_csv('https://gist.githubusercontent.com/figueredo/bf1dfac91dc436c594aa928ec040af7b/raw/6cb242923c7154c0676bf902ba0be9bd61e7f6c6/data.csv')

# describe
df.describe()

# score por attempts
df['ratio'] = df['score'] / df['attempts']

# select columns and rows
#df.loc[0:4, 'name':'ratio']

# max ratio
id = df.ratio.idxmax()
print('\n',df.loc[id,'name'], "teve o maior Ratio, de: ", df.loc[id, 'ratio'],'\n')

# max ratio
id = df.ratio.idxmin()
print('\n',df.loc[id,'name'], "teve o menor Ratio, de: ", df.loc[id, 'ratio'],'\n')

# ordenar valores
df = df.sort_values(['ratio'], ascending=False)

# salvar arquivo
df.to_csv("dados/df.csv", sep=';', encoding='utf-8')

#=========================#
# data visualization
#=========================#

# box plot RATIO
sns.set(style="whitegrid")
ax = sns.boxplot(y = df['ratio'])
plt.show()
