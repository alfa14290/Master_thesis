

import pandas as pd
from dask import dataframe as dd
#from autoviz.AutoViz_Class import AutoViz_Class
#%matplotlib_inline
import matplotlib as plt
import numpy as np
import scipy

#dd.to_parquet(df, path= "C:/Users/LENOBVO/Master-Thesis", engine="pyarrow", compression=None)
#print(ddf.groupby('company_symbol')[['x', 'y']].mean().compute().head())
#print(ddf.columns.values)
fisd_main= pd.read_csv('mergedissue.csv')
print('Issues in FISD', len(fisd_main)) 

print (len(fisd_main.prospectus_issuer_name.unique()))

fisd_main = fisd_main[(fisd_main['bond_type'] =='CDEB') | (fisd_main['bond_type'] =='CPIK') |
                      (fisd_main['bond_type'] =='CZ') | (fisd_main['bond_type'] =='CS')]

fisd_main = fisd_main[fisd_main['foreign_currency'] =='N']
print (len(fisd_main))

fisd_main['cusip_id'] = fisd_main['issuer_cusip'] + fisd_main['issue_cusip']
good_cusips = pd.DataFrame(fisd_main['cusip_id'])
good_cusips['identifier'] = 1
print (len(good_cusips))
print ('Issues in FISD', len(fisd_main))

### Trace million rows ###
trace = pd.read_csv('./data/small_trace.csv')
print(trace.dtypes)
print( trace.memory_usage(deep=True))
print('common cusip_id are', np.intersect1d(fisd_main['cusip_id'], trace['cusip_id'].astype(str)))

# #changing date to datetime
trace['trd_exctn_dt'] = trace.trd_exctn_dt.str[:4] + '-' + trace.trd_exctn_dt.str[5:7] + '-' + trace.trd_exctn_dt.str[8:10]
trace['trd_exctn_dt'] = pd.to_datetime(trace['trd_exctn_dt'])
print ('Dataset runs from', trace['trd_exctn_dt'].min(), 'to', trace['trd_exctn_dt'].max())

# #changing trade size format so we can operate on sizes as floats
trace.ascii_rptd_vol_tx[trace.ascii_rptd_vol_tx =='1MM+' ] =  1000005
trace.ascii_rptd_vol_tx[trace.ascii_rptd_vol_tx =='5MM+' ] =  5000005
trace['ascii_rptd_vol_tx'] = trace['ascii_rptd_vol_tx'].astype(float)
print(len(trace), trace.asof_cd.unique(), trace.trc_st.unique())
print(trace.dtypes)
print( trace.memory_usage(deep=True))
print ('unique trace cusips',len(trace['cusip_id'].unique()))
print ('total trades',len(trace))
print ('unique fisd len',len(good_cusips),'(same as above)')
trace = pd.merge(trace,good_cusips,on=['cusip_id'],how = 'inner')
print(trace.head())
trace = trace[pd.notnull(trace['rptd_pr'])]
print ('Unique CUSIPs in overlapping TRACE/FISD datasets',len(trace['cusip_id'].unique()))
print ('Unique Trades',len(trace))

"""Applying Dick-Nielsen - Part 1
The first part of the filter deletes trades that were canceled on the same day as 
the original transaciton. Same day cancels are identified by the TRC_ST column.
The logic is:
If TRC_ST = H or C then delete it
Also delete the trade from that day whose MSG_SEQ_NB equals the deleted H or C trade's ORIG_MSG_SEQ_NB
IF TRC_ST = I or W, then delete the I or W trade.
I or W means that the original trade was updated to reflect the error so deleting this fixes the problem."""
trace_len_pre_filter = len(trace)
print ('pre filter length', trace_len_pre_filter)

#delete I/W
trace = trace[(trace['trc_st'] != 'I')&(trace['trc_st'] != 'W')]
print ('post I/W delete length', trace_len_pre_filter)

#create dataframe of H/C trades
trace_same_day_cancel = trace[(trace['trc_st'] == 'H') | (trace['trc_st'] == 'C')]
print(trace_same_day_cancel)
trace_same_day_cancel = trace_same_day_cancel[['cusip_id','trd_exctn_dt','orig_msg_seq_nb']]
print(trace_same_day_cancel)
trace_same_day_cancel['cancel_trd'] = 1
#trace_same_day_cancel = trace_same_day_cancel.rename(columns={'orig_msg_seq_nb':'MSG_SEQ_NB'})

trace = pd.merge(trace,trace_same_day_cancel,on=['cusip_id','trd_exctn_dt','orig_msg_seq_nb'],how = 'outer')
trace = trace[(trace['trc_st'] != 'H') & (trace['trc_st'] != 'C')]
trace = trace[pd.notnull(trace['trc_st'])]
trace = trace[(trace['cancel_trd'] != 1)]
trace = trace[(pd.notnull(trace['trd_exctn_dt']))]
trace.drop(['trc_st', 'cancel_trd', 'orig_msg_seq_nb'], axis=1,inplace = True)
#len trace should decline by> trace_same_day_cancel
print ('final length', len(trace))

"""The second part of the filter deletes trades that were canceled on day different from 
the original transaction. Different day cancels are identified by the ASOF_CD column.
The logic is:
If ASOF_CD = X then delete
Canceled trade
If ASOF_CD = R, then
Delete R trade
Delete prior day trade with same CUSIP/price/size"""

trace = trace[(trace['asof_cd'] != 'X')]

trace_diff_day_cancel = trace[(trace['asof_cd'] == 'R')]
trace = trace[(trace['asof_cd'] != 'R')]

trace_diff_day_cancel = trace_diff_day_cancel[['cusip_id','rptd_pr','ascii_rptd_vol_tx']]
trace_diff_day_cancel['cancel_trd'] = 1

trace = pd.merge(trace,trace_diff_day_cancel,on=['cusip_id','rptd_pr','ascii_rptd_vol_tx'],how = 'outer')
trace = trace[(trace['asof_cd'] != 'R')]
trace = trace[(trace['cancel_trd'] != 1)]
trace = trace[(pd.notnull(trace['trd_exctn_tm']))]
trace.drop(['asof_cd', 'cancel_trd'], axis=1,inplace = True)
print (len(trace))

fisd_for_yield = fisd_main[['cusip_id','maturity','coupon','interest_frequency','principal_amt']]
fisd_for_yield['maturity'] = pd.to_datetime(fisd_for_yield['maturity'])
trace = trace.merge(fisd_for_yield,on='cusip_id',how='inner')
trace = trace[pd.notnull(trace['rptd_pr'])]
trace['days_to_maturity'] = (trace['maturity'] - trace['trd_exctn_dt']).astype('timedelta64[D]')
trace['n_maturity'] = (trace['days_to_maturity'] / 365) * 2
trace = trace[trace['days_to_maturity'] > 0]
print (len(trace))
print(trace.head())
print(trace["principal_amt"].unique())
def Px(Rate,Mkt_Price,Face,Freq,N,C):
    return Mkt_Price - (Face * ( 1 + Rate / Freq ) ** ( - N ) + ( C / Rate ) * ( 1 - (1 + ( Rate / Freq )) ** -N ) )

def YieldCalc(guess,Mkt_Price,Face,Freq,N,C):
    x = scipy.optimize.newton(Px, guess,args = (Mkt_Price,Face,Freq,N,C), tol=.0000001, maxiter=100)
    return x
yld = {}
for i in trace.index:
    try:
        yld.update({i:YieldCalc(trace['coupon'].loc[i]/(trace['principal_amt'].loc[i]/10),trace['rptd_pr'].loc[i],
                                trace['principal_amt'].loc[i]/10, trace['interest_frequency'].loc[i],
                                trace['n_maturity'].loc[i], trace['coupon'].loc[i])})
    except(RuntimeError):
        pass
    else:
        pass
trace['ytm']=pd.Series(yld)
trace = trace.sort_values(by = ['cusip_id','trd_exctn_dt','trd_exctn_tm'])
print(trace.head())
trace = trace[trace['ascii_rptd_vol_tx'] > 99999]
print(len(trace))

# trace.head()
# import dtale
# dtale.show(df)
# AV = AutoViz_Class()
# AV.AutoViz("mergedissue.csv")
# plt.show()
