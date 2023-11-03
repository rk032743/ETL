from datetime import datetime, timedelta
from tb_ligacao import d0 as ligacoes_d0
from tb_ligacao import d1 as ligacoes_d1
from tb_resultado_lig_new import d1 as result_lig_d1
from tb_resultado_lig_new import d0 as result_lig_d0


def d1(dia=1):

    ligacoes_d1(dia)
    result_lig_d1(dia)


def d0():
    
    ligacoes_d0()
    result_lig_d0()
    

if __name__ == '__main__':
    d1()

    # for dia in range(1,4):
    #     ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    #     print(ETL_DATA)
    #     d1(dia)
    # print("Carga concluída!")