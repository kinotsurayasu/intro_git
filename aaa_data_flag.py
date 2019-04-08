ああああ

import datetime
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')
import talib as ta
import os


def data_flag(datas,df_logic):

	top_logic = list(df_logic['logic_no'].unique())

	deviation = [1,2,3]

	windows = [2,3,10,20,40,80,160,320][::-1]

	logic_df = pd.DataFrame()
	
	n = 0
	for slope in ['sma_','ema_']:
		for i in windows:
			for j in windows:
				if i == j:
					pass
				elif i < j:
					pass
				else:
					# cross
					n += 1
					logic1 = 'logic' + str('{0:05d}'.format(n))
					data[logic1 + '_slope_'+slope+str(i)+'_'+str(j)] = data[slope+str(i)] - data[slope+str(j)]
					data[logic1 + '_sell'] = (data[logic1 + '_slope_'+slope+str(i)+'_'+str(j)] * data[logic1 + '_slope_'+slope+str(i)+'_'+str(j)].shift(1) < 0) & (data[slope+str(i)] > data[slope+str(j)])
					data[logic1 + '_buy'] = (data[logic1 + '_slope_'+slope+str(i)+'_'+str(j)] * data[logic1 + '_slope_'+slope+str(i)+'_'+str(j)].shift(1) < 0) & (data[slope+str(i)] < data[slope+str(j)])
					logic_df = pd.concat([logic_df,pd.DataFrame([logic1,slope+str(i)+'_'+str(j)]).T])

					# cross + rsi
					for r in windows:
						n += 1
						logic2 = 'logic' + str('{0:05d}'.format(n))
						if logic2 in top_logic:
							data[logic2 + '_sell'] = (data[logic1 + '_sell'] == True) & (data['rsi_' + str(r)] >= 70)
							data[logic2 + '_buy'] = (data[logic1 + '_buy'] == True) & (data['rsi_' + str(r)] <= 30)
							logic_df = pd.concat([logic_df,pd.DataFrame([logic2,slope+str(i)+'_'+str(j)+'_rsi'+str(r)]).T])
						else:
							pass
						# cross + rsi + gradient
						for g in windows:
							n += 1
							logic3 = 'logic' + str('{0:05d}'.format(n))
							if logic3 in top_logic:
								data[logic3 + '_sell'] = (data[logic1 + '_sell'] == True) & (data['rsi_' + str(r)] >= 70) & (data['gradient_' + str(g)] < 0)
								data[logic3 + '_buy'] = (data[logic1 + '_buy'] == True) & (data['rsi_' + str(r)] <= 30) & (data['gradient_' + str(g)] > 0)
								logic_df = pd.concat([logic_df,pd.DataFrame([logic3,slope+str(i)+'_'+str(j)+'_rsi'+str(r)+'_gradient'+str(g)]).T])
							else:
								pass
							# cross + rsi + gradient + macdhist
							n += 1
							logic4 = 'logic' + str('{0:05d}'.format(n))
							if logic4 in top_logic:
								data[logic4 + '_sell'] = (data[logic1 + '_sell'] == True) & (data['rsi_' + str(r)] >= 70) & (data['gradient_' + str(g)] < 0) & (data['macdhist']  < 0)
								data[logic4 + '_buy'] = (data[logic1 + '_buy'] == True) & (data['rsi_' + str(r)] <= 30) & (data['gradient_' + str(g)] > 0) & (data['macdhist']  > 0)
								logic_df = pd.concat([logic_df,pd.DataFrame([logic4,slope+str(i)+'_'+str(j)+'_rsi'+str(r)+'_gradient'+str(g)+'_macdhist']).T])
							else:
								pass
					for g in windows:
						# cross + gradient
						n += 1
						logic5 = 'logic' + str('{0:05d}'.format(n))
						if logic5 in top_logic:
							data[logic5 + '_sell'] = (data[logic1 + '_sell'] == True) & (data['gradient_' + str(g)] < 0)
							data[logic5 + '_buy'] = (data[logic1 + '_buy'] == True) & (data['gradient_' + str(g)] > 0)
							logic_df = pd.concat([logic_df,pd.DataFrame([logic5,slope+str(i)+'_'+str(j)+'_gradient'+str(g)]).T])
						else:
							pass
						# cross + gradient + macdhist
						n += 1
						logic6 = 'logic' + str('{0:05d}'.format(n))
						if logic6 in top_logic:
							data[logic6 + '_sell'] = (data[logic1 + '_sell'] == True) & (data['gradient_' + str(g)] < 0) & (data['macdhist']  < 0)
							data[logic6 + '_buy'] = (data[logic1 + '_buy'] == True) & (data['gradient_' + str(g)] > 0) & (data['macdhist']  > 0)
							logic_df = pd.concat([logic_df,pd.DataFrame([logic6,slope+str(i)+'_'+str(j)+'_gradient'+str(g)+'_macdhist']).T])
						else:
							pass
					# cross + macdhist
					n += 1
					logic7 = 'logic' + str('{0:05d}'.format(n))
					if logic7 in top_logic:
						data[logic7 + '_sell'] = (data[logic1 + '_sell'] == True) & (data['macdhist']  < 0)
						data[logic7 + '_buy'] = (data[logic1 + '_buy'] == True) & (data['macdhist']  > 0)
						logic_df = pd.concat([logic_df,pd.DataFrame([logic7,slope+str(i)+'_'+str(j)+'_macdhist']).T])
					else:
						pass

	# bb
	for bb in windows:
		for dev in [1,2,3]:
			n += 1
			logic8 = 'logic' + str('{0:05d}'.format(n))
			if logic8 in top_logic:
				data[logic8 + '_sell'] = data['openAsk'] >= data['upper_' + str(bb) + '_dev' + str(dev)]
				data[logic8 + '_buy'] = data['openAsk'] <= data['lower_' + str(bb) + '_dev' + str(dev)]
				logic_df = pd.concat([logic_df,pd.DataFrame([logic8,'bb_'+str(bb) + '_dev' + str(dev)]).T])
			else:
				pass
			# bb + rsi
			for r in windows:
				n += 1
				logic9 = 'logic' + str('{0:05d}'.format(n))
				if logic9 in top_logic:
					data[logic9 + '_sell'] = (data['openAsk'] >= data['upper_' + str(bb) + '_dev' + str(dev)]) & (data['rsi_' + str(r)] >= 70)
					data[logic9 + '_buy'] = (data['openAsk'] <= data['lower_' + str(bb) + '_dev' + str(dev)]) & (data['rsi_' + str(r)] <= 30)
					logic_df = pd.concat([logic_df,pd.DataFrame([logic9,'bb_'+str(bb) + '_dev' + str(dev)+'_rsi'+str(r)]).T])
				else:
					pass
				# bb+ rsi + gradient
				for g in windows:
					n += 1
					logic10 = 'logic' + str('{0:05d}'.format(n))
					if logic10 in top_logic:
						data[logic10 + '_sell'] = (data['openAsk'] >= data['upper_' + str(bb) + '_dev' + str(dev)]) & (data['rsi_' + str(r)] >= 70) & (data['gradient_' + str(g)] < 0)
						data[logic10 + '_buy'] = (data['openAsk'] <= data['lower_' + str(bb) + '_dev' + str(dev)]) & (data['rsi_' + str(r)] <= 30) & (data['gradient_' + str(g)] > 0)
						logic_df = pd.concat([logic_df,pd.DataFrame([logic10,'bb_'+str(bb) + '_dev'+ str(dev)+'_rsi'+str(r)+'_gradient'+str(g)]).T])
					else:
						pass
					# bb + rsi + gradient + macdhist
					n += 1
					logic11 = 'logic' + str('{0:05d}'.format(n))
					if logic11 in top_logic:
						data[logic11 + '_sell'] = (data['openAsk'] >= data['upper_' + str(bb) + '_dev' + str(dev)]) & (data['rsi_' + str(r)] >= 70) & (data['gradient_' + str(g)] < 0) & (data['macdhist']  < 0)
						data[logic11 + '_buy'] = (data['openAsk'] <= data['lower_' + str(bb) + '_dev' + str(dev)]) & (data['rsi_' + str(r)] <= 30) & (data['gradient_' + str(g)] > 0) & (data['macdhist']  > 0)
						logic_df = pd.concat([logic_df,pd.DataFrame([logic11,'bb_'+str(bb) + '_dev' + str(dev) +'_rsi'+str(r)+'_gradient'+str(g)+'_macdhist']).T])
					else:
						pass
			for g in windows:
				# bb + gradient
				n += 1
				logic12 = 'logic' + str('{0:05d}'.format(n))
				if logic12 in top_logic:
					data[logic12 + '_sell'] = (data['openAsk'] >= data['upper_' + str(bb) + '_dev' + str(dev)]) & (data['gradient_' + str(g)] < 0)
					data[logic12 + '_buy'] = (data['openAsk'] <= data['lower_' + str(bb) + '_dev' + str(dev)]) & (data['gradient_' + str(g)] > 0)
					logic_df = pd.concat([logic_df,pd.DataFrame([logic12,'bb_'+str(bb) + '_dev'+ str(dev) + '_gradient'+str(g)]).T])
				else:
					pass
				# bb + gradient + macdhist
				n += 1
				logic13 = 'logic' + str('{0:05d}'.format(n))
				if logic13 in top_logic:
					data[logic13 + '_sell'] = (data['openAsk'] >= data['upper_' + str(bb) + '_dev' + str(dev)]) & (data['gradient_' + str(g)] < 0) & (data['macdhist']  < 0)
					data[logic13 + '_buy'] = (data['openAsk'] <= data['lower_' + str(bb) + '_dev' + str(dev)]) & (data['gradient_' + str(g)] > 0) & (data['macdhist']  > 0)
					logic_df = pd.concat([logic_df,pd.DataFrame([logic13,'bb_'+str(bb) + '_dev'+ str(dev) + '_gradient'+str(g)+'_macdhist']).T])
				else:
					pass
			# bb + macdhist
			n += 1
			logic14 = 'logic' + str('{0:05d}'.format(n))
			if logic14 in top_logic:
				data[logic14 + '_sell'] = (data['openAsk'] >= data['upper_' + str(bb) + '_dev' + str(dev)]) & (data['macdhist']  < 0)
				data[logic14 + '_buy'] = (data['openAsk'] <= data['lower_' + str(bb) + '_dev' + str(dev)]) & (data['macdhist']  > 0)
				logic_df = pd.concat([logic_df,pd.DataFrame([logic14,'bb_'+str(bb) + '_dev'+ str(dev) + '_macdhist']).T])
			else:
				pass

	# DMI
	# n += 1
	# logic15 = 'logic' + str('{0:05d}'.format(n))
	# if logic15 in top_logic:
	# 	data[logic15 + '_sell'] = (data['PositiveDI'] < data['NegativeDI']) & (data['ADX'] > 40) &(data['PositiveDI'].shift(1) > data['NegativeDI'].shift(1))
	# 	data[logic15 + '_buy'] = (data['PositiveDI'] > data['NegativeDI']) & (data['ADX'] > 40) & (data['PositiveDI'].shift(1) < data['NegativeDI'].shift(1))
	# 	logic_df = pd.concat([logic_df,pd.DataFrame([logic15,'DMI']).T])
	# else:
	# 	pass

	data = data.reset_index()
	data['idx'] = data.index
	# data = data.set_index('time')
	data.index = data['time']

	logic_df = logic_df.reset_index(drop=True)
	logic_df.columns = ['logic_no','content']

	t2 = time.time()

	#print('droped_DATA SIZE:' + str(len(data)))
	print(t2-t1)
	return data, logic_df
