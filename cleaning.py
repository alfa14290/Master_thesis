

import pandas as pd
from dask import dataframe as dd
#from autoviz.AutoViz_Class import AutoViz_Class
#%matplotlib_inline
import matplotlib as plt
import numpy as np


#df = dd.read_csv('khzgysrufhvytvix.csv', dtype = 'unicode')
#dd.to_parquet(df, path= "C:/Users/LENOBVO/Master-Thesis", engine="pyarrow", compression=None)
#print(df.compute())
#df1 = pd.read_parquet("C:/Users/LENOBVO/Master-Thesis/part.1.parquet")
#ddf= dd.read_parquet("C:/Users/LENOBVO/Master-Thesis/part*.parquet", engine="pyarrow")


#print(ddf.groupby('company_symbol')[['x', 'y']].mean().compute().head())
#print(ddf.columns.values)
fisd_main= pd.read_csv('mergedissue.csv')
print('Issues in FISD', len(fisd_main)) 
print (fisd_main.shape)
cusip_list = ['17453BAF8','17453BAJ0','873168AM0','873168AP3','873168AN8','873168AQ1','451663AA6',
              '816752AA7','451663AC2','800907AL1','800907AN7','882330AA1','882330AC7','458204AD6',
              '458204AF1','184502AZ5','882330AF0','882330AG8','184502BC5','184502BB7','184502BE1',
              '97314XAE4','780097AW1','97315LAA7','458204AH7','458204AJ3','69545QAA7','184502BF8',
              '695459AF4','89255MAA4','184502BG6','18451QAF5','18451QAE8','18451QAH1','18451QAG3',
              '18451QAK4','18451QAJ7']
fisd_main = fisd_main[fisd_main['complete_cusip'].isin(cusip_list)]
print (fisd_main.shape)
print (fisd_main.prospectus_issuer_name.unique())
fisd_main = fisd_main[(fisd_main['bond_type'] =='CDEB') | (fisd_main['bond_type'] =='CPIK') |
                      (fisd_main['bond_type'] =='CZ') | (fisd_main['bond_type'] =='CS')]

fisd_main = fisd_main[fisd_main['foreign_currency'] =='N']
print (len(fisd_main))

fisd_main['cusip_id'] = fisd_main['issuer_cusip'] + fisd_main['issue_cusip']
good_cusips = pd.DataFrame(fisd_main['cusip_id'])
good_cusips['identifier'] = 1

print (len(good_cusips))
print ('Issues in FISD', len(fisd_main))


trace = pd.read_csv('small_trace.csv')
print(trace.dtypes)
print( trace.memory_usage(deep=True))
print('common cusip_id are', np.intersect1d(fisd_main['cusip_id'], trace['cusip_id'].astype(str)))
print(fisd_main['cusip_id'])
print(type(trace['cusip_id']))
trace['cusip_id'].to_csv('cusipid.csv')
x = '184502BB7'
if x in trace['cusip_id'].values:
    print(True)
# #trace.rename(columns={"trd_exctn_dt": "TRD_EXCTN_DT", "entrd_vol_qt": "ASCII_RPTD_VOL_TX"}, errors="raise")
# # #changing date to datetime
# trace['trd_exctn_dt'] = trace.trd_exctn_dt.str[:4] + '-' + trace.trd_exctn_dt.str[5:7] + '-' + trace.trd_exctn_dt.str[8:10]
# trace['trd_exctn_dt'] = pd.to_datetime(trace['trd_exctn_dt'])
# print ('Dataset runs from', trace['trd_exctn_dt'].min(), 'to', trace['trd_exctn_dt'].max())

# #changing trade size format so we can operate on sizes as floats
# trace.ascii_rptd_vol_tx[trace.ascii_rptd_vol_tx =='1MM+' ] =  1000005
# trace.ascii_rptd_vol_tx[trace.ascii_rptd_vol_tx =='5MM+' ] =  5000005
# trace['ascii_rptd_vol_tx'] = trace['ascii_rptd_vol_tx'].astype(float)
# print(len(trace), trace.asof_cd.unique(), trace.trc_st.unique())
# print(trace.dtypes)
# print( trace.memory_usage(deep=True))
# print ('unique trace cusips',len(trace['cusip_id'].unique()))
# print ('total trades',len(trace))
# print ('unique fisd len',len(good_cusips),'(same as above)')
# trace = pd.merge(trace,good_cusips,on=['cusip_id'],how = 'inner')
# print(trace.head())
# trace = trace[pd.notnull(trace['rptd_pr'])]
# print ('Unique CUSIPs in overlapping TRACE/FISD datasets',len(trace['cusip_id'].unique()))
# print ('Unique Trades',len(trace))

# trace.head()
# import dtale
# dtale.show(df)
# AV = AutoViz_Class()
# AV.AutoViz("mergedissue.csv")
# plt.show()
