import pandas as pd
import numpy as np
from multiprocessing import Pool
from time import process_time
import h5py
import sys
from functools import partial
import datetime
import xstrm.network_calc as net_calc


if __name__ == '__main__':
#def intitial():
    print (datetime.datetime.now())
    stream_df = pd.read_pickle('data/flow_table_sub.pkl')
    stream_df = stream_df[['seg_id', 'AreaSqKM', 'proc_unit', 'StartFlag']]
    stream_df.rename(columns={'AreaSqKM':'area_sqkm'}, inplace=True)
    stream_df.index.name = 'seg_id'
    headwater_df = stream_df.loc[stream_df['StartFlag']==1]
    headwater_df = headwater_df[['seg_id', 'area_sqkm']]

    non_headwater_df = stream_df[['seg_id','area_sqkm','proc_unit']]
    proc_unit = list(set(non_headwater_df['proc_unit'].tolist()))
    p = Pool()
    list_agg_values = partial(net_calc.upstream_sum, non_headwater_df)
    result = p.map(list_agg_values, proc_unit)
    p.close()
    p.join()
    print (datetime.datetime.now())
    final_list = [item for sublist in result for item in sublist] 
    df_output = pd.DataFrame(final_list)
    print (df_output.shape)
    df_output.to_csv('test_out.csv', sep=',', encoding='utf-8')
