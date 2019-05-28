
d = {
'Thiago': [1, 2, 3],
'Joao': [1, 2, 3, 4]
}

print(d)

import numpy as np

x = [x for x in range(101)]
y = np.arange(101)

print(np.max(x))
print(np.var(x))
print(np.std(x))
print(np.mean(x))
print(np.cumsum(x))

# seletor condicional
pares = y[y % 2 == 0]
print(pares)

# rendimento mensal de 0,469% durante
# 10 anos com invest_init inicial de 1000
def calcPoupancaLista(anos, invest_init):
    string = 'ano,valor_acumulado\n'
    ano = 2019
    for i in np.arange(anos*12 + 1):
        invest_init = invest_init + (invest_init*0.00469)
        if i % 12 == 0:
            ano += 1
            string += str(ano) + "," + str(round(invest_init, 2)) + "\n"
    return string

# gravar arquivo
def writeArq(string):
    file = open('dados/arquivo.csv', 'w')
    file.write(string)
    file.close()

#====================#
# Execucao
#====================#

# capturar investimento inicial
invest_init = None
while invest_init is None or not invest_init.isnumeric():
        invest_init = input("Quanto você quer investir inicialmente? ")
invest_init = int(invest_init)

# capturar tempo
anos = None
while anos is None or not anos.isnumeric():
        anos = input("Em quantos anos você pretende resgatar o dinheiro? ")
anos = int(anos)

# executar calculo
valorStr = calcPoupancaLista(anos, invest_init)
writeArq(valorStr)
print("Dados salvos em: dados/arquivos.txt")
