import numpy as np
import pandas as pd
import pyodbc as pyo
import sqlalchemy as sa
from sqlalchemy import create_engine, text
import numpy as np
import math
pd.set_option('display.max_columns', None)
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
pd.set_option('display.float_format', '{:.2f}'.format)
from datetime import datetime
from numpy import nan
#Kết nối SV:
connection_uri = (
    "mssql+pyodbc://SASDBPRD02/Scorecard_RPT?driver=ODBC+Driver+13+for+SQL+Server"
)
conn = sa.create_engine(connection_uri)
engine = create_engine(connection_uri)
start_time = datetime.now()
#2. Base table:
base_query = f"""
WITH STEP_1 AS (
SELECT process_dt, account_number, aging_bucket, outstanding_principal 
FROM ODS_AGING_BUCKET_DAILY_V WITH (NOLOCK)
where aging_bucket IN ('1a1','1a2','1a3','1a4') 
)
, STEP_2 AS (
SELECT account_number, invoice_date, exceed_limit, total_amount_due, min_amount_due, is_mad_paid, is_tad_paid, due_date
FROM BO_CRD_INVOICE a WITH (NOLOCK)
LEFT JOIN (SELECT id, account_number FROM BO_ACC_ACCOUNT WITH (NOLOCK)) b
on a.account_id = b.id
where invoice_date >= dateadd(day, 1, EOMONTH(cast(GETDATE() as date), -3))
)
, step_3 as (SELECT a.*, DATEADD(MONTH, 1, invoice_date) as nextmonth_invoice, DATEADD(DAY, -1, invoice_date) as yesterday_invoice 
FROM STEP_2 a
)
, step_4 as (SELECT ACCOUNT_ID, WO_DATE_ACTUAL FROM T_WO_LIST WITH (NOLOCK))

, step_6 as (SELECT ACCOUNT_NUMBER, NGAY_PHE_DUYET AS 'TCC_CONFIRMED_DATE', NGAY_GIA_HAN_NO_CUOI_CUNG AS 'TCC_END_CYCLE' FROM T_TCC_LIST WITH (NOLOCK))

, step_5 as (SELECT CMS_ACCOUNTID, Score, Score_Type FROM [dbo].[Risk_7_Score_Group] WITH (NOLOCK))
, step_7 as (SELECT account_number, card_type_group_name, region_cde as region_code, app_last_operations_check_date as open_date FROM ODS_DW_APPLICATION_DIM WITH (NOLOCK))

select A.account_number, invoice_date, exceed_limit, total_amount_due, min_amount_due, is_mad_paid, is_tad_paid, due_date,
 b.aging_bucket, c.outstanding_principal as OS, d.aging_bucket AS NEXT_AGING, card_type_group_name, region_code, open_date,
 Score_Type, Score,
 TCC_CONFIRMED_DATE, TCC_END_CYCLE, 
 CASE WHEN GETDATE() <= TCC_END_CYCLE THEN 1 ELSE 0 END AS 'TCC_FLAG'
from step_3 A
left join STEP_1 B
ON A.account_number = B.account_number and b.process_dt = a.invoice_date -- LẤY RA TRẠNG THÁI AGING TẠI NGÀY INVOICE
LEFT JOIN STEP_1 C
ON a.yesterday_invoice = c.process_dt and a.account_number = c.account_number -- LẤY RA OS TẠI NGÀY INVOICE -1
LEFT JOIN STEP_1 D
ON a.nextmonth_invoice = d.process_dt and a.account_number = d.account_number -- LẤY RA NEXT AGING TẠI INVOICE + 1 MONTH
LEFT JOIN step_4 e
on a.account_number = e.ACCOUNT_ID and a.invoice_date >= e.WO_DATE_ACTUAL --KO LẤY KỲ INVOICE SAU NGÀY WO
LEFT JOIN step_5 f
on a.account_number = f.CMS_ACCOUNTID
LEFT JOIN step_6 g
on a.account_number = g.ACCOUNT_NUMBER 
LEFT JOIN step_7 H
ON A.account_number = H.account_number
WHERE e.ACCOUNT_ID IS NULL AND  b.aging_bucket IS NOT NULL
"""
base_info = pd.read_sql(base_query, conn)
base_t = base_info.drop_duplicates()
base_t['Row_Check'] = base_t.sort_values('invoice_date', ascending=False).groupby(['account_number', 'invoice_date']).cumcount() +1
base_t = base_t[base_t['Row_Check'] == 1]
base_t['invoice_date'] = pd.to_datetime(base_t['invoice_date'])
base_t['end_cycle'] = base_t['invoice_date'] + pd.DateOffset(months=1)
base_t['end_cycle'] = base_t['end_cycle'] - pd.DateOffset(days=1)
start_invoice = base_t['invoice_date'].min().strftime('%Y-%m-%d')
max_invoice = base_t['end_cycle'].max().strftime('%Y-%m-%d')
# 3. Lay bang payment
mad_payment_query = f"""
WITH STEP_1 AS (
SELECT account_number
FROM BO_CRD_INVOICE a WITH (NOLOCK)
LEFT JOIN (SELECT id, account_number FROM BO_ACC_ACCOUNT WITH (NOLOCK)) b
on a.account_id = b.id
where invoice_date >= dateadd(day, 1, EOMONTH(cast(GETDATE() as date), -3))
)

, STEP_2 AS (
SELECT a.*, b.eff_date,  min_amount_due FROM  (
SELECT account_number, process_dt, sum(amount) AS TOTAL_PAYMENT, 
SUM(overdraft_payment + overdue_payment + write_off_principal_payment) AS OS_PAYMENT,
SUM(interest_payment +overdue_interest_payment + write_off_interest_payment) AS INTEREST_PAYMENT,
SUM(fees_payment +write_off_fee) AS FEE_PAYMENT from (
SELECT account_number, process_dt,amount, overdraft_payment, overdue_payment, write_off_principal_payment,
interest_payment, overdue_interest_payment, write_off_interest_payment,
fees_payment, write_off_fee
 FROM ODS_DW_CARD_LOAN_PAYMENT_FCT WITH (NOLOCK)
where process_dt BETWEEN '{start_invoice}' AND '{max_invoice}'
AND is_successful_txn = 1 
AND merchant_name NOT IN ('ADJUST','HUYBH','KMMTRT','CSC','MGM','MEGASALE','RLPF','RILF','NAPAS','NAPASECOM','writeoff','BOITHUONGBH','CFCADJ', 'MRKMOBIFONE') ) a
GROUP BY account_number, process_dt) a
LEFT JOIN (
SELECT account_number, account_id, eff_date, min_amount_due, split_hash, invoice_id FROM BO_CST_CFC_PERIODIC_MAD a WITH (NOLOCK)
LEFT JOIN (SELECT id, account_number, status FROM BO_ACC_ACCOUNT WITH (NOLOCK) ) b
on a.account_id = b.id) b
on a.account_number = b.account_number and a.process_dt = b.eff_date)

select a.* from STEP_2 a
left join STEP_1 b
on a.account_number = b.account_number
where b.account_number IS NOT NULL
"""
mad_payment = pd.read_sql(mad_payment_query, conn)
payment_t = mad_payment
payment_t = payment_t.loc[:, ~payment_t.columns.duplicated()]
payment_t['PAID_FLAG'] = np.where(payment_t['TOTAL_PAYMENT'] >= payment_t['min_amount_due'], 1, 0)
payment_t['PAID_MAD_DATE'] = np.where(payment_t['TOTAL_PAYMENT'] >= payment_t['min_amount_due'], payment_t['eff_date'], "") 
payment_t['PAID_ANY_DATE'] = np.where(payment_t['TOTAL_PAYMENT'] < payment_t['min_amount_due'], payment_t['eff_date'], "")
payment_t['process_dt'] = pd.to_datetime(payment_t['process_dt'])
payment_t['eff_date'] = pd.to_datetime(payment_t['eff_date'])
mad_inv_payment = base_t.merge(payment_t, on = 'account_number', how = 'left').query('process_dt.between(`invoice_date`, `end_cycle`)')
mad_inv_payment['PAID_MAD_DATE'] = pd.to_datetime(mad_inv_payment['PAID_MAD_DATE'])
mad_inv_payment['PAID_ANY_DATE'] = pd.to_datetime(mad_inv_payment['PAID_ANY_DATE'])
min_paid_t = mad_inv_payment
min_paid_t['Row_Check_2'] = min_paid_t.sort_values('PAID_MAD_DATE').groupby(['account_number', 'invoice_date']).cumcount() +1
min_paid_t = min_paid_t[min_paid_t['Row_Check_2'] == 1]
min_paid_t = min_paid_t[['account_number', 'invoice_date','PAID_MAD_DATE']]
min_paid_t = min_paid_t.dropna(subset=['PAID_MAD_DATE'])
min_paid_t = min_paid_t.merge(payment_t, left_on = ['account_number', 'PAID_MAD_DATE'], right_on = ['account_number', 'process_dt'], how = 'left')
min_paid_t = min_paid_t[['account_number', 'invoice_date','PAID_MAD_DATE_x', 'TOTAL_PAYMENT', 'OS_PAYMENT', 'INTEREST_PAYMENT', 'FEE_PAYMENT']].\
            rename(columns={'PAID_MAD_DATE_x': 'PAID_MAD_DATE', 'TOTAL_PAYMENT':'MAD_PAYMENT'})
min_paid_mad = min_paid_t
min_paid_mad['Row_Check'] = min_paid_mad.sort_values('PAID_MAD_DATE', ascending=True).groupby(['account_number', 'invoice_date']).cumcount() +1
min_paid_mad = min_paid_mad[min_paid_mad['Row_Check'] == 1][['account_number', 'invoice_date', 'PAID_MAD_DATE']]
min_paid_mad.rename(columns={'PAID_MAD_DATE': 'MIN_PAID_MAD'}, inplace=True)
max_any_t = mad_inv_payment
max_any_t['Row_Check_2'] = max_any_t.sort_values('PAID_ANY_DATE', ascending = False).groupby(['account_number', 'invoice_date']).cumcount() +1
max_any_t = max_any_t[max_any_t['Row_Check_2'] == 1]
max_any_t = max_any_t[['account_number', 'invoice_date','PAID_ANY_DATE']]
max_any_t.dropna(subset=['PAID_ANY_DATE'], inplace = True)
max_any_t = max_any_t.merge(payment_t, left_on = ['account_number', 'PAID_ANY_DATE'], right_on = ['account_number', 'process_dt'], how = 'left')
max_any_t = max_any_t [['account_number', 'invoice_date','PAID_ANY_DATE_x', 'TOTAL_PAYMENT', 'OS_PAYMENT', 'INTEREST_PAYMENT', 'FEE_PAYMENT']].\
            rename(columns={'PAID_ANY_DATE_x': 'PAID_ANY_DATE', 'TOTAL_PAYMENT':'ANY_PAID_PAY'})
base_t_1 = base_t[['account_number', 'invoice_date', 'end_cycle', 'due_date','exceed_limit', 'min_amount_due', 
                 'is_mad_paid', 'is_tad_paid', 'aging_bucket', 'OS', 'NEXT_AGING', 'card_type_group_name', 'region_code', 'open_date', 
                   'Score', 'Score_Type', 'TCC_CONFIRMED_DATE', 'TCC_END_CYCLE', 'TCC_FLAG']]
base_t_1 = base_t_1.merge(min_paid_t[['account_number', 'invoice_date', 'PAID_MAD_DATE', 'MAD_PAYMENT', 'OS_PAYMENT', 'INTEREST_PAYMENT', 'FEE_PAYMENT']], on = ['account_number', 'invoice_date'], how = 'left')
base_t_1 = base_t_1.merge(max_any_t[['account_number', 'invoice_date', 'ANY_PAID_PAY']], on = ['account_number', 'invoice_date'], how = 'left')
base_t_1['RB'] = np.where(base_t_1['PAID_MAD_DATE'].notnull(), 1, 0)
base_t_1['ANY_PAID'] = np.where((base_t_1['PAID_MAD_DATE'].isnull()) & (base_t_1['ANY_PAID_PAY'] >0) & (base_t_1['ANY_PAID_PAY'] < base_t_1['MAD_PAYMENT']),1,0)
base_t_1 = base_t_1.rename(columns={'due_date': 'PD'})
base_t_1['PD']= pd.to_datetime(base_t_1['PD'])
base_t_1['PD_PAID'] = (base_t_1['PAID_MAD_DATE']- base_t_1['PD']).dt.days
base_t_1 = base_t_1[['account_number', 'invoice_date', 'end_cycle', 'PD', 'exceed_limit', 'min_amount_due', 'is_mad_paid', 'is_tad_paid', 
'aging_bucket', 'OS', 'NEXT_AGING', 'PAID_MAD_DATE', 'MAD_PAYMENT', 'OS_PAYMENT','INTEREST_PAYMENT', 'FEE_PAYMENT',  'RB', 'ANY_PAID','PD_PAID',
'card_type_group_name', 'region_code', 'open_date', 'Score', 'Score_Type', 'TCC_CONFIRMED_DATE', 'TCC_END_CYCLE', 'TCC_FLAG']]
base_t_1['GROUP_QLRR'] = \
np.where((base_t_1['aging_bucket'] == '1a1') & (base_t_1['OS'] == 0) & (base_t_1['min_amount_due'] == 0), 'A0',
np.where((base_t_1['aging_bucket'] == '1a1') & (base_t_1['OS'] > 0) & (base_t_1['min_amount_due'] == 0), 'SKIP_MAD',
np.where((base_t_1['aging_bucket'] == '1a1') & (base_t_1['min_amount_due'] > 0) & 
         ((base_t_1['PAID_MAD_DATE'] >= base_t_1['invoice_date']) & (base_t_1['PAID_MAD_DATE'] <= base_t_1['PD'])), 'AUTO_PAID','AR_ALLOCATION')))
base_t_1['AGING_GROUP'] = np.where(base_t_1['aging_bucket'] == '1a1', 'M1',
                        np.where(base_t_1['aging_bucket'] == '1a2', 'M2', np.where(base_t_1['aging_bucket'].isin(['1a3','1a4']), 'M3-M4', "")))
base_t_1['AC'] = 1
base_t_1['RB_AC'] = np.where((base_t_1['GROUP_QLRR'].isin(['SKIP_MAD', 'AUTO_PAID'])) | (base_t_1['PAID_MAD_DATE'].notnull()) |
        (base_t_1['is_mad_paid'] == 1) | (base_t_1['is_tad_paid'] ==1) | (base_t_1['NEXT_AGING'] == '1a1'), 1, 0)
base_t_1['AR'] = np.where(base_t_1['GROUP_QLRR'] == 'AR_ALLOCATION', 1,0)
base_t_1['RB_AR'] = \
np.where(((base_t_1['GROUP_QLRR'] == 'AR_ALLOCATION') & (base_t_1['PAID_MAD_DATE'].notnull())) |
((base_t_1['GROUP_QLRR'] == 'AR_ALLOCATION') & (base_t_1['is_mad_paid'] == 1)) |
((base_t_1['GROUP_QLRR'] == 'AR_ALLOCATION') & (base_t_1['is_tad_paid'] == 1)), 1,0)    
base_t_1['A0_flag'] = np.where(base_t_1['GROUP_QLRR'] == 'A0', 'A0', 'A1') ## SỬA 3/10/2024
base_t_1.drop_duplicates(inplace= True)
#
allocation_query = f"""
 SELECT ACC_NUMBER AS account_number,INV_DATE as invoice_date,CASE WHEN  AGING_2 LIKE '%M1%' then 'M1'
                                                            WHEN  AGING_2 LIKE '%M2%' then 'M2'
                                                            WHEN AGING_2 like '%M3%' then 'M3-M4'
                                                            WHEN  AGING_2 like '%M4%' then 'M3-M4'
                                                            END AS AGING_THN

     FROM  RISK2_ALLOCATION_CALL_MONTHLY
     WHERE INV_DATE >= dateadd(day, 1, EOMONTH(cast(GETDATE() as date), -3))
"""
allocation_info = pd.read_sql(allocation_query , conn)
hold_query = f"""
SELECT  A.*
                    , CONVERT (DATE,	
                                                    CASE WHEN A.END_DATE IS NULL  and b.NGAY_GIA_HAN_NO_CUOI_CUNG is not null THEN b.NGAY_GIA_HAN_NO_CUOI_CUNG
                                                                    WHEN A.END_DATE IS NULL and b.NGAY_GIA_HAN_NO_CUOI_CUNG is null THEN GETDATE() 
                                                                    ELSE A.END_DATE 
                                                                    END) AS END_DATE_ACTUAL
    FROM qlrr2.THN_HOLD_MASKING A
    left join T_TCC_LIST b on b.ACCOUNT_NUMBER=a.ACCOUNT_NUMBER;
"""
hold_info = pd.read_sql(hold_query , conn)
hold_info.rename(columns={'ACCOUNT_NUMBER': 'account_number'}, inplace=True)
#
hold_info_t = hold_info
hold_info_t['END_DATE_ACTUAL']= pd.to_datetime(hold_info_t['END_DATE_ACTUAL'])
#
base_t_2 = base_t_1.merge(allocation_info, on =['account_number','invoice_date'], how='left')
base_t_2['AR_THN'] = np.where(base_t_2['AGING_THN'].notnull(),1,0)
base_t_2['RB_THN'] = np.where((base_t_2['AGING_THN'].notnull()) & (base_t_2['PAID_MAD_DATE'].notnull()),1,
        np.where((base_t_2['AGING_THN'].notnull()) & (base_t_2['is_mad_paid'] == 1) ,1,
        np.where((base_t_2['AGING_THN'].notnull()) & (base_t_2['is_tad_paid'] == 1),1,0)))
base_t_2 = base_t_2.merge(hold_info[['account_number', 'END_DATE_ACTUAL', 'Action', 'hold_marking' ]], on = 'account_number', how = 'left')
base_t_2['HOLD'] = \
np.where((base_t_2['Action'] == 'Marking') & ((base_t_2['invoice_date'] >= base_t_2['hold_marking']) & (base_t_2['invoice_date'] <= base_t_2['END_DATE_ACTUAL'])),1,
np.where((base_t_2['Action'] == 'Hold case') & ((base_t_2['invoice_date'] >= base_t_2['hold_marking']) & (base_t_2['invoice_date'] <= base_t_2['END_DATE_ACTUAL'])),1,
np.where((base_t_2['Action'] == 'Unhold') & ((base_t_2['invoice_date'] >= base_t_2['hold_marking']) & (base_t_2['invoice_date'] <= base_t_2['END_DATE_ACTUAL'])),1,
np.where((base_t_2['invoice_date'] >= base_t_2['hold_marking']) & (base_t_2['invoice_date'] <= base_t_2['END_DATE_ACTUAL']), 1, 0))))
base_t_2 = base_t_2[['account_number', 'invoice_date', 'end_cycle', 'PD', 'exceed_limit','min_amount_due', 'is_mad_paid', 'is_tad_paid', 'aging_bucket', 
    'OS', 'NEXT_AGING', 'PAID_MAD_DATE', 'MAD_PAYMENT', 'OS_PAYMENT', 'INTEREST_PAYMENT', 'FEE_PAYMENT', 'RB', 'ANY_PAID', 'PD_PAID', 'AR_THN', 'RB_THN',
       'GROUP_QLRR', 'AGING_GROUP', 'AC', 'RB_AC', 'AR', 'RB_AR', 'A0_flag','AGING_THN', 'HOLD', 'card_type_group_name', 'region_code', 'open_date',
         'Score', 'Score_Type', 'TCC_CONFIRMED_DATE', 'TCC_END_CYCLE', 'TCC_FLAG'           ]]
base_t_2['open_date'] = pd.to_datetime(base_t_2['open_date'])
base_t_2['MOB'] = ((base_t_2['invoice_date'].dt.year - base_t_2['open_date'].dt.year) * 12 + 
                          (base_t_2['invoice_date'].dt.month - base_t_2['open_date'].dt.month))
base_t_2['COLLECTION_ALLOCATION_FLAG'] = np.where(base_t_2['AGING_THN'].notnull(), 'COLLECTION_ALLOCATED', 'NO_ALLOCATE')
base_t_2.drop_duplicates(inplace = True)
#
base_t_3 = base_t_2.merge(min_paid_mad, on =['account_number', 'invoice_date'], how='left')
condition_flagfull30 = (pd.to_datetime('today') + pd.DateOffset(months=-1)) + pd.DateOffset(days=-1)
base_t_3['FLAG_Full30PD'] = np.where(base_t_3['invoice_date'] >= condition_flagfull30, 0, 1)
base_t_3['RB_OS'] = np.where(base_t_3['RB_AC'] == 1, base_t_3['OS'], "")
base_t_3['PD_MAX'] = (base_t_3['MIN_PAID_MAD'] - base_t_3['PD'] ).dt.days
base_t_3['INVOICE_MONTH'] = base_t_3['invoice_date'].dt.strftime('%Y-%m')
base_t_3.rename(columns={'card_type_group_name':'Card_type_group', 'end_cycle':'END_CYCLE', 'OS':'outstanding_principal', 'region_code':'Region_Code'}, inplace = True)
base_t_3 = base_t_3[['INVOICE_MONTH', 'account_number', 'invoice_date','PD', 'END_CYCLE',  'exceed_limit', 'open_date', 'MOB',
'Card_type_group', 'Region_Code',  'min_amount_due', 'is_mad_paid', 'is_tad_paid', 'aging_bucket','NEXT_AGING', 'outstanding_principal', 
'PAID_MAD_DATE', 'MAD_PAYMENT','OS_PAYMENT', 'INTEREST_PAYMENT', 'FEE_PAYMENT', 'RB', 'RB_THN', 'RB_AC', 'RB_AR', 'RB_OS', 'ANY_PAID', 'PD_PAID', 
'GROUP_QLRR','AGING_GROUP', 'AC', 'AR', 'AR_THN', 'A0_flag', 'AGING_THN', 'HOLD',  'MIN_PAID_MAD', 'FLAG_Full30PD', 'PD_MAX', 'COLLECTION_ALLOCATION_FLAG',
                  'Score', 'Score_Type', 'TCC_CONFIRMED_DATE', 'TCC_END_CYCLE', 'TCC_FLAG']]
base_t_3.drop_duplicates(inplace=True)
#
##LẤY THÔNG TIN HARDCALL, FIELD 
hard_field_late_query = f"""
SELECT ALLOCATED_MONTH, ALLOCATED_DATE ,ACCOUNT_NUMBER, BOOK FROM qlrr2.RISK2_MONTHLY_FIELD_HC_ALLOCATION
WHERE BOOK ='HARD CALL' AND  ALLOCATED_DATE  >= dateadd(day, 1, EOMONTH(cast(GETDATE() as date), -3))
"""
hard_field_late = pd.read_sql(hard_field_late_query, conn)
#
base_t_4 = base_t_3.merge(hard_field_late, left_on =['INVOICE_MONTH', 'account_number'],right_on =['ALLOCATED_MONTH', 'ACCOUNT_NUMBER'], how = 'left')
base_t_4['HC'] = np.where(base_t_4['BOOK'] == 'HARD CALL', 1, 0)
base_t_4 = base_t_4[['INVOICE_MONTH', 'account_number', 'invoice_date', 'PD', 'END_CYCLE',
       'exceed_limit', 'open_date', 'MOB', 'Card_type_group', 'Region_Code',
       'min_amount_due', 'is_mad_paid', 'is_tad_paid', 'aging_bucket',
       'NEXT_AGING', 'outstanding_principal', 'PAID_MAD_DATE', 'MAD_PAYMENT',
       'OS_PAYMENT', 'INTEREST_PAYMENT', 'FEE_PAYMENT', 'RB', 'RB_THN',
       'RB_AC', 'RB_AR', 'RB_OS', 'ANY_PAID', 'PD_PAID', 'GROUP_QLRR',
       'AGING_GROUP', 'AC', 'AR', 'AR_THN', 'A0_flag', 'AGING_THN', 'HOLD',
       'MIN_PAID_MAD', 'FLAG_Full30PD', 'PD_MAX', 'COLLECTION_ALLOCATION_FLAG', 'HC', 
                    'Score', 'Score_Type', 'TCC_CONFIRMED_DATE', 'TCC_END_CYCLE', 'TCC_FLAG']]
base_t_4.drop_duplicates(inplace=True)
#
c_score_query = f"""
SELECT account_number,invoice_date  , CASE WHEN A.SCORECARD_POINTS<745 THEN 'R5'
                                    WHEN A.SCORECARD_POINTS>=745 AND  A.SCORECARD_POINTS<832 THEN 'R4'
                                    WHEN A.SCORECARD_POINTS>=832 AND  A.SCORECARD_POINTS<919 THEN 'R3'
                                    WHEN A.SCORECARD_POINTS>=919 AND A.SCORECARD_POINTS<1006 THEN 'R2'
                                    WHEN A.SCORECARD_POINTS>=1006 THEN 'R1' 
                                    END AS SCORE_SEG
FROM RISK_7_CSCORE_6M A
WHERE INVOICE_DATE >= dateadd(day, 1, EOMONTH(cast(GETDATE() as date), -3))
"""
c_score_t = pd.read_sql(c_score_query, conn)
#
c_score_info = c_score_t
c_score_info['INVOICE_MONTH'] = pd.to_datetime(c_score_info['invoice_date']).dt.strftime('%Y-%m')
c_score_info = c_score_info[['account_number', 'SCORE_SEG', 'INVOICE_MONTH']]
c_score_info['account_number'] = c_score_info['account_number'].astype(object) 
base_t_5 = base_t_4.merge(c_score_info, on=['account_number', 'INVOICE_MONTH'], how= 'left')
base_t_5['Score_Type'].replace(np.nan, '10. No Score', inplace= True)
base_t_5['Score'].replace(np.nan, 0, inplace= True)
base_t_5['Customer_group'] = np.where(base_t_5['open_date'] >= pd.to_datetime('2023-01-01'), 'New_customer_2023', 'Existing_Customer')
base_t_5.replace({np.nan: None}, inplace = True)
for column in base_t_5.columns.to_list():
    base_t_5[column] = base_t_5[column].astype(str).str.replace('.00', '', regex=False)
    base_t_5[column] = base_t_5[column].astype(str).str.replace('.0', '', regex=False)
#
list = [
    'O', 'O', 'O', 'O', 'O', 'float64', 'O', 'int64', 'O', 'O', 
    'float64', 'float64', 'float64', 'O', 'O', 'float64', 'O', 
    'float64', 'float64', 'float64', 'float64', 'int64', 'int64', 
    'int64', 'int64', 'float64', 'int64', 'float64', 'O', 'O', 
    'int64', 'int64', 'int64', 'O', 'O', 'int64', 'O', 'int64', 
    'int64', 'O', 'int64', 'float64', 'O', 'O', 'O', 'int64', 'O', 'O']
for col, type in zip(base_t_5.columns, list):
    base_t_5[col] = base_t_5[col].replace('', np.nan)
    if type == 'float64' or type == 'int64':
        base_t_5[col] = pd.to_numeric(base_t_5[col], errors='coerce')
        base_t_5[col] = base_t_5[col].fillna(0).astype(type)
    base_t_5[col] = base_t_5[col].astype(type)
col_fix = ['invoice_date', 'PD', 'END_CYCLE', 'open_date', 'PAID_MAD_DATE', 'MIN_PAID_MAD', 'TCC_CONFIRMED_DATE', 'TCC_END_CYCLE']
for date_col in col_fix:
    base_t_5[date_col] = pd.to_datetime(base_t_5[date_col], errors='coerce', format='mixed')
for date_col in col_fix:
    base_t_5[date_col] = base_t_5[date_col].fillna(nan)
base_t_5.replace('None', nan, inplace=True)
base_t_5.drop_duplicates(inplace=True)
####
with engine.connect() as connect:
    connect.execute(text(f"DELETE FROM DATAMART_RISK1 WHERE invoice_date >= '{start_invoice}' "))
    connect.commit()
base_t_5.to_sql('DATAMART_RISK1', con=conn, if_exists='append', index=False)
######

end_time = datetime.now()
execution_time = (end_time - start_time).total_seconds() / 60
execution_time= f"{execution_time:.2f} minutes" 
date_print = datetime.today().date().strftime('%Y-%m-%d')
timestamp_format = '%Y-%h-%d-%H:%M:%S'
now = datetime.now()
timestamp = now.strftime(timestamp_format)
with open("log_datamart.txt", 'a') as file:
    file.write ('On ' + date_print +' Finished At ' + timestamp + '.' + ' took ' +  execution_time + '\n')
