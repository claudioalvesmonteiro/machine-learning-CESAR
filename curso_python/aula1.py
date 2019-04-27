'''
ML
Python
26/04/2019
@Claudio Alves Monteiro
'''


# importar pacotes
import time

# CTRL+d PARA SAIR

# rendimento mensal de 0,469% durante
# 10 anos com invest_init inicial de 1000

def calcPoupancaProf(anos, invest_init):
    return invest_init * (1+0.00469)**(anos*12)

def calcPoupanca(anos, invest_init):
    '''  calcula o rendimento mensal de 0,469% durante
        X com invest_init inicial de X
    '''
    for i in [x for x in range(anos*12 + 1)]:
        invest_init = invest_init + (invest_init*0.00469)
    return invest_init

def calcPoupancaLista(anos, invest_init):
    lista = []
    for i in [x for x in range(anos*12 + 1)]:
        invest_init = invest_init + (invest_init*0.00469)
        if i % 12 == 0:
            lista.append(round(invest_init, 2))
    return lista

def calcPoupancaListaPrint(anos, invest_init):
    print("\nA cada ano você terá um retorno de: ")
    ano = 2019
    for i in [x for x in range(anos*12 + 1)]:
        invest_init = invest_init + (invest_init*0.00469)
        if i % 12 == 0:
            print(ano, ":", round(invest_init, 2))
            ano += 1

def calcCDI(anos, invest_init):
    '''  calcula o rendimento anual de 105% de %7,5 durante
        `anos` com invest_init inicial de `invest_init`
    '''
    x = list(range(1,((anos) +1)))
    for i in x:
        invest_init = invest_init + (invest_init*0.075*1.05)
    return invest_init

#====================#
# Execucao
#====================#

# capturar invest_init
eval = False
while eval is False:
    try:
        invest_init = int(input("Quanto você quer investir inicialmente? "))
        eval = True
    except:
        print("\nDigite um valor numérico :) \n")

# capturar tempo
anos = None
while anos is None or not anos.isnumeric():
        anos = input("Em quantos anos você pretende resgatar o dinheiro? ")
anos = int(anos)

# executar calculo
valor1 = calcPoupancaListaPrint(anos, invest_init)
#str = 'Você terá {valor} de retorno na Poupança'.format(valor = valor1)
print(valor1)
