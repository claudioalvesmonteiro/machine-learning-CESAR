import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv(
'https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data',
header=None
)

df.columns = ['sepal_len', 'sepal_wid', 'petal_len', 'petal_wid', 'class']

# remover NAs
df.dropna(how = 'all', inplace = True)

# N de casos por class
df.groupby('class').size()

# histograma
df.hist()
plt.show()

# histograma SEABORN
sns.set()
sns.distplot(df['sepal_len'])
plt.show()

# scatterplot SEABORN
sns.scatterplot(x="sepal_len", y="petal_len",
                sizes=(1, 8), linewidth=1,
                data=df)
