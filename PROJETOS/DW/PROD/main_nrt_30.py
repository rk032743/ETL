
from YDUQS.etl_yduqs import d0 as etl_yduqs
from ORIZON.orizon_tb_follow_atividade import nrt as etl_orizon
from WFM.etl_wfm import d0 as wfm_d0

if __name__ == "__main__":
        
        etl_yduqs()
        etl_orizon()
        wfm_d0()
        
