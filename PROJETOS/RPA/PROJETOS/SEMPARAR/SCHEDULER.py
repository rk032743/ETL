import os, sys
from SEMPARAR_WPP_SELENIUM import *

try:
    comando = f"taskkill /f /im chrome.exe"
    os.system(comando)

except Exception:
    pass

param1 = report_atendimentos()
# param2 = agent_event_range()

print('>>>>>>>>>>>>>> param1 <<<<<<<<<<<<<<<')

print(param1[0])
print(param1[1])
print(param1[2])

# print('>>>>>>>>>>>>>> param2 <<<<<<<<<<<<<<<')

# print(param2[0])
# print(param2[1])
# print(param2[2])


######################################### PENTAHO #####################################################
# dirJob = "\\\\\SRVUSUARIOS\\BI_Adm\\Data_Science_Team\\Ewerton\\CODE\\DATASCIENCE\\PENTAHO\\DATAVIZ\\"
dirJob = "C:\\ETL\\DATASCIENCE\\PENTAHO\\DATAVIZ\\"

pentahoKitchen = "C:\\Pentaho\\kitchen.bat"
pentahoJob = "JB_SEMPARAR_MASTER"
pentahoLog = ['Nothing', 'Error', 'Minimal', 'Basic', 'Detailed', 'Debug', 'Row Level']
cmd1 = f"call {pentahoKitchen} /file:{dirJob}{pentahoJob}.kjb /level:{pentahoLog[2]}"
cmd2 = f""" "/param:pArquivo1={param1[0]}" "/param:pDestino1={param1[1]}" "/param:pCmd1={param1[2]}" """ 
# cmd3 = f""" "/param:pArquivo2={param2[0]}" "/param:pDestino2={param2[1]}" "/param:pCmd2={param2[2]}" """ 
# comando = f"{cmd1}{cmd2}{cmd3}"
comando = f"{cmd1}{cmd2}"
os.system(comando)
sys.exit()