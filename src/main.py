import os
from  model.Dataframe import Dataframe

actual_directory = os.path.dirname(os.path.realpath(__file__))

fornPath = os.path.join(actual_directory, "..","data","Fornecedores.csv")
vendasPath = os.path.join(actual_directory, "..","data","VendasGlobais.csv")
transPath = os.path.join(actual_directory, "..","data","Transportadoras.csv")
vendedorPath = os.path.join(actual_directory, "..","data","Vendedores.csv")

fornDF = Dataframe(fornPath)
vendasDF = Dataframe(vendasPath)
transDF = Dataframe(transPath)
vendedorDF = Dataframe(vendedorPath)

print(fornDF)
print(vendasDF)
print(transDF)
print(vendedorDF)
