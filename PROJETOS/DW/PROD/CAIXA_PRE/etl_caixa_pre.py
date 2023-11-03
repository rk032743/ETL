from tb_ativo_caixa_pre import batch as b1, update as u1, nrt as n1
from tb_ativo_caixa_pred2 import batch as b2, update as u2, nrt as n2
from tb_prop_caixa_pre import batch as b3, update as u3, nrt as n3
from tb_prop_retencao import batch as b4, update as u4, nrt as n4
from tb_prop_tag_receptivo import batch as b5, update as u5, nrt as n5
from tb_proposta_pred import batch as b6, update as u6, nrt as n6
from tb_ativo_retencao import batch as b7, update as u7, nrt as n7


from datetime import datetime, timedelta


def d1(dia=1):

    b1(dia)
    b2(dia)
    b3(dia)
    b4(dia)
    b5(dia)
    b6(dia)
    b7(dia)
    update(dia)


def update(dia=1):

    u1(dia)
    u2(dia)
    u3(dia)
    u4(dia)
    u5(dia)
    u6(dia)
    u7(dia)


def d0():

    n1()
    n2()
    n3()
    n4()
    n5()
    n6()
    n7()


if __name__ == '__main__':
    b7(1)
    u7(1)
    # for dia in range(0,6):
    #     ETL_DATA = (datetime.now() - timedelta(days=dia)).strftime('%Y-%m-%d')
    #     print(ETL_DATA)
        # d1(dia)