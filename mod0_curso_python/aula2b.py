import matplotlib.pyplot as plt

#plt.plot([1,2,3])
#plt.show()

juros = open('dados/arquivo.csv', 'r')
juros = juros.readlines()

jurosLista = []
for i in juros[1:len(juros)]:
    jurosLista.append(i.split(','))

anos = []
valores = []
for i in jurosLista:
    anos.append(int(i[0]))
    valor = ''
    for carac in i[1]:
        if carac != '\n':
            valor+=carac
    valores.append(float(valor))

print(anos)
print(valores)
plt.plot(anos, valores)
plt.show()
