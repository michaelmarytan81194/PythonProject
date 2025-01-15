----------------------------------#Bonus by collector-------------------------------------------------
	  drop table if exists #bonus_by_collector;
select MONTH
		,a.NAME
		,a.MANAGER
		,a.[TARGET(AMOUNT)]
		,a.MCA
		,a.TA_RB
		,a.FLAG
		,case when a.BOOK = 'INDUE' and convert(varchar(10),MONTH,120) <= '2023-11-01' then 'M1-M4'
			  when a.BOOK = 'INDUE' and convert(varchar(10),MONTH,120) > '2023-11-01' then BOOK2
			  When a.BOOK = 'LEGAL' then 'LEGAL'
			  When a.BOOK = 'FIELD' then 'FIELD'
			  when a.BOOK2 = 'CIC' then 'CIC' 
			  when a.BOOK2 = 'TCC' then 'EARLY CALL' 
			  when a.book2 = 'EARLY CALL' then 'EARLY CALL'
			  when BOOk3 in ('LATE CALL','LATE CALL (N4 - 5)') then 'LATE CALL' 
			  when BOOK3 in ('HARD CALL TCC') then 'TCC'
		      end book -------- Team l?n -------------- không ???c ?? null
		,case when BOOK3 = 'REGION 1' then 'VÙNG 1'
				when BOOK3 = 'REGION 3' then 'VÙNG 3'
				when BOOK3 = 'REGION 4' then 'VÙNG 4'
				when BOOK3 = 'REGION 5' then 'VÙNG 5'
				when BOOK3 = 'REGION 6' then 'VÙNG 6'
				when book2 in ('AGENCY','LEGAL CALL', 'LITIGATION') then book2
				when BOOK2 = 'CIC' then 'CIC'
				when BOOK2 like '%M1%'then 'M1'
				when BOOK2 like '%M2%'then 'M2'
				when BOOK2 like '%M3-M4%' then 'M3-M4'
				when book2 in ('EARLY CALL','LATE CALL','HARD CALL') then 'HARD CALL'
				else a.book end book2 -- by team , NOT NULL
		,a.BONUS
		,a.CNT
		,a.PERCENT_PAID
		,a.[FULL PAID]
		,a.[% BONUS]
		,a.TEAM
		,a.CONTEST
		,a.BOOK3
into #Bonus_by_collector
from qlrr2.Bonus_by_collector a

select book, book2, BOOK3, sum(BONUS) from #Bonus_by_collector where MONTH='2024-11-01'
group by book, book2, BOOK3
--select * from #bonus_by_collector where MONTH='2024-11-01'
--select BOOK,BOOK2,BOOK3 from qlrr2.bonus_by_collector



----------------------------------Employee Base------------------------------------------------
-----------------------------------------------------------------------------------------------

drop table if exists  #sum_bonus_basse1
select CONVERT(varchar(7),month,120) process_month
	  ,case when book2 in ('AGENCY','LEGAL CALL', 'LITIGATION') then book2
			when BOOK2 = 'CIC' then 'CIC'
			when BOOK2 like '%M1%'then 'M1'
			when BOOK2 like '%M2%'then 'M2'
			when BOOK2 like '%M3-M4%' then 'M3-M4'
			when BOOK3 in ('EARLY CALL', 'TCC') then 'EARLY CALL'  -----change
			when BOOk3 in ('LATE CALL','LATE CALL (N4 - 5)') then 'LATE CALL' 
			when BOOK3 in ('HARD CALL TCC') then 'EARLY CALL'
			else a.book end book
	  ,case when BOOK3 = 'REGION 1' then 'VÙNG 1'
			when BOOK3 = 'REGION 3' then 'VÙNG 3'
			when BOOK3 = 'REGION 4' then 'VÙNG 4'
			when BOOK3 = 'REGION 5' then 'VÙNG 5'
			when BOOK3 = 'REGION 6' then 'VÙNG 6'
			when book2 in ('AGENCY','LEGAL CALL', 'LITIGATION') then book2
			when book2 in ('EARLY CALL','LATE CALL','HARD CALL','TCC_CIC') then 'HARD CALL'
			when BOOK2 = 'CIC' then 'CIC'
			when BOOK2 like '%M1%'then 'M1'
			when BOOK2 like '%M2%'then 'M2'
			when BOOK2 like '%M3-M4%' then 'M3-M4'
			else a.book end book2
	  ,flag                               employee_lvl
	  ,SUM(bonus + isnull(CONTEST,0))     bonus_deduction 
	  ,sum(cnt)                           no_of_allocation
	  ,count(distinct name)               No_of_employee
into #sum_bonus_basse1  
from qlrr2.bonus_by_collector a
group by CONVERT(varchar(7),month,120) 
	  ,case when book2 in ('AGENCY','LEGAL CALL', 'LITIGATION') then book2
			when BOOK2 = 'CIC' then 'CIC'
			when BOOK2 like '%M1%'then 'M1'
			when BOOK2 like '%M2%'then 'M2'
			when BOOK2 like '%M3-M4%' then 'M3-M4'
			when BOOK3 in ('EARLY CALL', 'TCC') then 'EARLY CALL'  -----change
			when BOOk3 in ('LATE CALL','LATE CALL (N4 - 5)') then 'LATE CALL' 
			when BOOK3 in ('HARD CALL TCC') then 'EARLY CALL'
			else a.book end 
	  ,case when BOOK3 = 'REGION 1' then 'VÙNG 1'
			when BOOK3 = 'REGION 3' then 'VÙNG 3'
			when BOOK3 = 'REGION 4' then 'VÙNG 4'
			when BOOK3 = 'REGION 5' then 'VÙNG 5'
			when BOOK3 = 'REGION 6' then 'VÙNG 6'
			when book2 in ('AGENCY','LEGAL CALL', 'LITIGATION') then book2
			when book2 in ('EARLY CALL','LATE CALL','HARD CALL','TCC_CIC') then 'HARD CALL'  ---change 
			when BOOK2 = 'CIC' then 'CIC'
			when BOOK2 like '%M1%'then 'M1'
			when BOOK2 like '%M2%'then 'M2'
			when BOOK2 like '%M3-M4%' then 'M3-M4'
			else a.book end 
	  ,flag 



---- select BOOK, BOOK2, BOOK3 from qlrr2.bonus_by_collector
---select * from #sum_bonus_basse1 where process_month='2024-11'
-- select distinct book,book2 from #sum_bonus_basse1

drop table if exists #Employee_base
select a.process_month
      ,a.book
	  ,a.book2
	  ,a.employee_lvl
	  ,a.No_of_employee
	  ,b.total_employee 
into #Employee_base
from #sum_bonus_basse1 a
left join (select process_month 
				 , sum(No_of_employee)          total_employee 
			from #sum_bonus_basse1 a
			where a.employee_lvl in ('COLLECTOR','TEAM LEADER','TEAMLEADER')
			group by process_month
           ) b on a.process_month = b.process_month 

-- select * from #Employee_base where process_month='2024-10'


		   ----------------------- Allowance base --------------------------------------
drop table if exists #Allowance
select CONVERT(varchar(7),month,120) process_month
	  ,case when book2 in ('AGENCY','LEGAL CALL', 'LITIGATION') then book2
			when BOOK2 = 'CIC' then 'CIC'
			when BOOK2 like '%M1%'then 'M1'
			when BOOK2 like '%M2%'then 'M2'
			when BOOK2 like '%M3-M4%' then 'M3-M4'
			when BOOK3 = 'EARLY CALL' then 'EARLY CALL'
			when BOOk3 in ('LATE CALL','LATE CALL (N4 - 5)') then 'LATE CALL' 
			when BOOK3 in ('HARD CALL TCC') then 'EARLY CALL'
			else a.book end book
	  ,case when BOOK3 = 'REGION 1' then 'VÙNG 1'
			when BOOK3 = 'REGION 3' then 'VÙNG 3'
			when BOOK3 = 'REGION 4' then 'VÙNG 4'
			when BOOK3 = 'REGION 5' then 'VÙNG 5'
			when BOOK3 = 'REGION 6' then 'VÙNG 6'
			when book2 in ('AGENCY','LEGAL CALL', 'LITIGATION') then book2
			when book2 in ('EARLY CALL','LATE CALL','HARD CALL','TCC_CIC') then 'HARD CALL'---- change
			when BOOK2 = 'CIC' then 'CIC'
			when BOOK2 like '%M1%'then 'M1'
			 when BOOK2 like '%M2%'then 'M2'
			 when BOOK2 like '%M3-M4%' then 'M3-M4'
			else a.book end book2
	  ,SUM(ALLOWANCE) ALLOWANCE
into #Allowance
from qlrr2.qltdkh_allowance a
group by  CONVERT(varchar(7),month,120) 
	  ,case when book2 in ('AGENCY','LEGAL CALL', 'LITIGATION') then book2
			when BOOK2 = 'CIC' then 'CIC'
			when BOOK2 like '%M1%'then 'M1'
			when BOOK2 like '%M2%'then 'M2'
			when BOOK2 like '%M3-M4%' then 'M3-M4'
			when BOOK3 = 'EARLY CALL' then 'EARLY CALL'
			when BOOk3 in ('LATE CALL','LATE CALL (N4 - 5)') then 'LATE CALL' 
			when BOOK3 in ('HARD CALL TCC') then 'EARLY CALL'
			else a.book end 
	  ,case when BOOK3 = 'REGION 1' then 'VÙNG 1'
			when BOOK3 = 'REGION 3' then 'VÙNG 3'
			when BOOK3 = 'REGION 4' then 'VÙNG 4'
			when BOOK3 = 'REGION 5' then 'VÙNG 5'
			when BOOK3 = 'REGION 6' then 'VÙNG 6'
			when book2 in ('AGENCY','LEGAL CALL', 'LITIGATION') then book2
			 when book2 in ('EARLY CALL','LATE CALL','HARD CALL','TCC_CIC') then 'HARD CALL'---- change
			when BOOK2 = 'CIC' then 'CIC'
			when BOOK2 like '%M1%'then 'M1'
			 when BOOK2 like '%M2%'then 'M2'
			 when BOOK2 like '%M3-M4%' then 'M3-M4'
			else a.book end 
			
			drop table if exists #sum_bonus_basse; ----- Merge bonus v?i allowance
			select a.process_month
				  ,a.book
				  ,a.book2
				  ,a.employee_lvl
				  ,a.bonus_deduction + ISNULL(b.ALLOWANCE,0) bonus_deduction
				  ,a.no_of_allocation
				  ,a.No_of_employee
			 into #sum_bonus_basse
			 from #sum_bonus_basse1 a
			 left join #Allowance b on a.process_month = b.process_month and a.book = b.book and a.book2 = b.book2 and a.employee_lvl = 'COLLECTOR' ----- ch? tr? allowance cho collector

-- select BOOK, BOOK2, BOOK3, Sum(ALLOWANCE)     sum_allowance from qlrr2.qltdkh_allowance where MONTH='2024-10-01' group by BOOK, BOOK2, BOOK3

--select * from #Allowance

--------------------------------Allocation Base------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

--------HARDCALL
Drop table if exists #HC_by_team
select  a.ALLOCATED_MONTH
	   ,a.ACCOUNT_NUMBER      acc_number
	   ,case when a.ALLOCATED_MONTH <= '2023-12' then b.REGION_NAME
			 when a.ALLOCATED_MONTH > '2023-12' then a.REGION_NAME
			 end REGION_NAME
	   ,a.BOOK
into #HC_by_team
from qlrr2.RISK2_MONTHLY_FIELD_HC_ALLOCATION a
left join qlrr2.HC_by_team                   b      on a.ACCOUNT_NUMBER = b.ACC_NUMBER    and a.ALLOCATED_MONTH = b.ALLOCATED_MONTH 
where a.BOOK = 'HARD CALL'

-- select * from qlrr2.RISK2_MONTHLY_FIELD_HC_ALLOCATION
-- select distinct BOOK from qlrr2.RISK2_MONTHLY_FIELD_HC_ALLOCATION   --b?ng này có 3 books là FIELD, HARDCALL, LATECALL

-- select distinct REGION_NAME from #HC_by_team where ALLOCATED_MONTH='2024-10'
-- SELECT * FROM #HC_by_team where ALLOCATED_MONTH='2024-10'


----------FIELD
Drop table if exists #field_region_mapping
select  ALLOCATED_MONTH
	   ,ACCOUNT_NUMBER        acc_number
	   ,case when REGION_NAME like '%1%' or REGION_NAME is null then 'VÙNG 1' 
			 when REGION_NAME like '%3%' then 'VÙNG 3'
			 when REGION_NAME like '%4%' then 'VÙNG 4'
			 when REGION_NAME like '%5%' then 'VÙNG 5'
			 when REGION_NAME like '%6%' then 'VÙNG 6'
			 END REGION_NAME
	   ,BOOK
into #field_region_mapping
from qlrr2.RISK2_MONTHLY_FIELD_HC_ALLOCATION
where BOOK = 'FIELD'

-- select * from #field_region_mapping 



drop table if exists #allocation_base1;------ B?ng này l?u thông tin allocation theo t?ng team bao g?m: ngày allocate, ngày end , nhóm n? , d? n? t?i ngày allocate theo CS&A cung c?p

-------------------LEGAL allocation
select CONVERT(varchar(7),month,120)               Process_month 
	   ,convert(varchar(10),month,120)             allocation_date
	   ,convert(varchar(10),eomonth(month),120)    end_date
	   ,acc_number                                 account_number 
	   ,case when BOOK_1 in (N'NGUY?N TH? THANH TRÚC 1') then 'LEGAL CALL'
			 else Book_1 end Book
	   ,Book_1                                     Book_2
	   ,Bom_aging                                  Allocation_aging
	   ,Bom_TAD                                    allocation_TAD
	   ,Bom_pos                                    allocation_MAD
into #allocation_base1
from qlrr2.LEGAL_ALLOCATION_MONTHLY with(nolock)
where MONTH >= '2023-01-01'
union all
-- select * from #allocation_base1 where process_month='2024-10'

-------------------FIELD/ HARDCALL allocation
select a.allocated_month                                      process_month 
	   ,convert(varchar(10),allocated_date,120)               allocation_date
	   ,convert(varchar(10),eomonth(allocated_date),120)      end_date
	   ,a.account_number
	   ,case when a.BOOK = 'HARD CALL' and a.REGION_NAME = 'TCC_CIC' then 'CIC' ------bo sung code
			 when a.book = 'HARD CALL' and c.region_name is not null then c.region_name
			 when a.book = 'HARD CALL' and c.region_name is null and a.region_name is null and a.BOM_AGING < '3a' then 'EARLY CALL'
			 when a.book = 'HARD CALL' and c.region_name is null and a.region_name is null and a.BOM_AGING >= '3a' then 'LATE CALL'
			 else a.book end book
	   ,case when a.BOOK = 'FIELD' and b.region_name is not null then b.REGION_NAME
			 when a.BOOK = 'FIELD' and b.region_name is null then 'VÙNG 1' ---- Các tr??ng h?p tr?ng Region ?? t?m vào vùng 1
			 when a.BOOK = 'HARD CALL' and a.REGION_NAME = 'TCC_CIC' then 'CIC' ------bo sung code
			 else a.book end book_2
	   ,Bom_aging            Allocation_aging
	   ,Bom_TAD              allocation_TAD
	   ,Bom_pos              allocation_MAD 
from qlrr2.RISK2_MONTHLY_FIELD_HC_ALLOCATION a 
left join #field_region_mapping              b on b.acc_number = a.ACCOUNT_NUMBER and a.ALLOCATED_MONTH = b.ALLOCATED_MONTH    -------- L?y region Field cho các tháng CS&A không b? sung region n?u không có import vào thêm (v?n có tr??ng h?p null)
left join #HC_by_team                        c on a.ACCOUNT_NUMBER = c.ACC_NUMBER and a.ALLOCATED_MONTH = c.ALLOCATED_MONTH ------ L?y EARLY v?i LATE
where a.allocated_date >= '2023-01-01'	
union all
-- SELECT * from qlrr2.RISK2_MONTHLY_FIELD_HC_ALLOCATION
-- select * from qlrr2.LEGAL_ALLOCATION_MONTHLY
-- select distinct Book, Book_2 from #allocation_base1 where process_month='2024-10'

-------------------INDUE allocation
select  invoice_month                                      process_month 
	   ,min(convert(varchar(10),allocated_date,120))       allocation_date
	   ,max(convert(varchar(10),last_date,120))            end_date
	   ,acc_number                                         account_number
	   ,case when aging_2 like '%M1%'then 'M1'
			 when aging_2 like '%M2%'then 'M2'
			 when aging_2 like '%M3-M4%' then 'M3-M4' end book
		,case when aging_2 like '%M1%'then 'M1'
			 when aging_2 like '%M2%'then 'M2'
			 when aging_2 like '%M3-M4%' then 'M3-M4' end book_2
		,case when aging_2 like '%M1%'then '1a1'
			 when aging_2 like '%M2%'then '1a2'
			 when aging_2 like '%M3-M4%' then '1a3-1a4' end Allocation_aging
		,c.total_amount_due                           allocation_TAD
		,(c.overdraft_balance + overdue_balance)      allocation_OS
from RISK2_ALLOCATION_CALL_MONTHLY A with(nolock)
left join bo_acc_account           b with(nolock) on a.acc_number = b.account_number
left join BO_CRD_INVOICE           c with(nolock) on c.account_id = b.id and a.invoice_month = left(c.invoice_date,7) 
where invoice_month >= '2023-01' 
group by invoice_month
		,acc_number
		,case when aging_2 like '%M1%'then 'M1'
			 when aging_2 like '%M2%'then 'M2'
			 when aging_2 like '%M3-M4%' then 'M3-M4' end 
		,case when aging_2 like '%M1%'then 'M1'
			 when aging_2 like '%M2%'then 'M2'
			 when aging_2 like '%M3-M4%' then 'M3-M4' end 
		,case when aging_2 like '%M1%'then '1a1'
			 when aging_2 like '%M2%'then '1a2'
			 when aging_2 like '%M3-M4%' then '1a3-1a4' end
		,c.total_amount_due 
		,(c.overdraft_balance + overdue_balance)
union all
-- select * from RISK2_ALLOCATION_CALL_MONTHLY

----------------------------CIC allocation
select  a.ALLOCATED_MONTH                                          process_month 
	   ,convert(varchar(10),a.allocated_date,120)                  allocation_date
	   ,convert(varchar(10),EOMONTH(a.allocated_date),120)         end_date
	   ,a.ACC_NUMBER                                               account_number
	   ,a.BOOK2                                                    book
	   ,a.BOOK2                                                    book_2
	   ,'1'                                                        allocation_aging
	   ,a.Bom_TAD                                                  allocation_TAD
	   ,a.Bom_pos                                                  allocation_MAD
from qlrr2.CIC_allocation2 a
where ALLOCATED_MONTH >= '2023-01'
;
-- select * from #allocation_base1
-- select * from qlrr2.CIC_allocation2  


Drop table if exists #add_M1_AR; ------- l?y M1 AR - no allocation (do stop call PD) , M1 AA
select  a.INVOICE_MONTH            Process_month
	   ,a.invoice_date             allocation_date 
	   ,a.END_CYCLE                end_date
	   ,a.account_number
	   ,a.AGING_GROUP              Book
	   ,case when GROUP_QLRR = 'AR_ALLOCATION'  then 'M1 AR'
	         when GROUP_QLRR = 'AUTO_PAID'  then 'M1 AA' 
			 when GROUP_QLRR = 'SKIP_MAD'  then 'M1 AA' 
			 end  Book_2
	   ,a.aging_bucket Allocation_aging
	   ,c.total_amount_due allocation_TAD
	   ,a.min_amount_due allocation_MAD
into #add_M1_AR
from qlrr2.datamart a
left join BO_ACC_ACCOUNT b on a.account_number = b.account_number
left join BO_CRD_INVOICE c on b.id = c.account_id and c.invoice_date = a.invoice_date
where a.INVOICE_MONTH >= '2023-01'
	  and a.AGING_GROUP = 'M1'
	  and a.GROUP_QLRR not in  ('A0')


Drop table if exists #allocation_base; ----------> Megre THN allocation with M1-Ar M1-AA

	select * 
	into #allocation_base -------> B?ng Full Allocation (Bao gôm M1 AA và M1 AR)
	from #allocation_base1
	union all
	select a.* 
	from #add_M1_AR A
	left join #allocation_base1 d on a.account_number = d.account_number and a.Process_month = d.Process_month
	where d.account_number is null
	-- select distinct Book from #allocation_base



	drop table if exists #Field_allocation; --------> tính t?ng s? l??ng allocation cho FIELD
	select a.Process_month
		  ,a.Book_2 book 
		  ,count(1) no_of_allocation 
	into #Field_allocation
	from #allocation_base a
	where Book = 'FIELD'
	group by a.Process_month
		  ,a.Book_2  

    drop table if exists #HC_allocation;  --------> tính t?ng s? l??ng allocation cho HARDCALL (EARLY + LATE)
	select a.Process_month
		  ,a.Book_2 book 
		  ,count(1) no_of_allocation 
	into #HC_allocation
	from #allocation_base a
	where Book_2 = 'HARD CALL'
	group by a.Process_month
		  ,a.Book_2  


	drop table if exists #team_allocation  -------> tinh allocation total theo t?ng book
	select a.Process_month
		  ,a.Book 
		  ,b.Total_no_of_allocation
		  ,count(1) Total_no_of_allocation_by_book ---->Total
		  ,sum(
				case when a.Book_2 not in ('M1 AR','M1 AA') then 1
					 else 0 end 
			  ) no_of_allocation ------> t?ng allocation không bao g?m AA và AR-no allocation
	      ,sum(
			    case when a.Book_2 = 'M1 AR' then 1
				     else 0 end 
			  ) no_of_allocation_AR ------> t?ng case AR-no allocation
	      ,sum(
				case when a.Book_2 = 'M1 AA' then 1
					 else 0 end 
			  ) no_of_allocation_AA -------> t?ng case M1 AA
into #team_allocation 
from #allocation_base a
left join (select Process_month, count(1) Total_no_of_allocation from #allocation_base group by Process_month) b on b.Process_month = a.Process_month
group by a.Process_month
		 ,a.book
		 ,b.Total_no_of_allocation
----
--SELECT distinct Book FROM  #team_allocation where Process_month='2024-10'
--select distinct Book,Book_2 from #allocation_base where Process_month='2024-10'



-----------------------------------Payment Base------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS #PAYMENT; ------- L?y thanh toán theo t?ng ngày 
SELECT DISTINCT A.account_number	
				, process_dt				
				, SUM(amount) AS Payment ----- T?ng thanh toán
				, SUM(a.overdraft_payment + overdue_payment + write_off_principal_payment) OS_Payment ----- Thanh toán g?c
				, SUM(A.amount - a.overdraft_payment - overdue_payment - write_off_principal_payment) int_fee_payment ------- Thanh toán lãi phí 
INTO #PAYMENT
FROM ODS_DW_CARD_LOAN_PAYMENT_FCT  A with(nolock)
WHERE 1=1
	And process_dt >= '2023-01-01' 
	AND is_successful_txn = 1
	AND merchant_name NOT IN ('ADJUST','HUYBH','KMMTRT','CSC','MGM','MEGASALE','RLPF','RILF','NAPAS','NAPASECOM','writeoff','BOITHUONGBH','CFCADJ','MRKMOBIFONE')
GROUP BY a.Account_number , process_dt;

-----------------------------------STAFF COST BASE---------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------


drop table if exists #total_employee ---------- tong so nhan vien THN cua TTQLTDKH---
select CONVERT(varchar(7),MONTH,120) Process_month
	  ,count(distinct name)  total_employee
into #total_employee
from #Bonus_by_collector a
where a.FLAG in ('COLLECTOR')
group by CONVERT(varchar(7),MONTH,120)

-- select * from #total_employee where Process_month = '2024-10'


drop table if exists #No_of_employeee;-----------Tong so nhan vien theo  tung book
select CONVERT(varchar(7),MONTH,120) Process_month
	   , BOOK
       ,case when a.BOOK = 'M1-M4' and convert(varchar(10),MONTH,120) <= '2023-11-01' then 'M1-M4'
		     when a.BOOK = 'M1-M4' and convert(varchar(10),MONTH,120) > '2023-11-01' then BOOK2    
			 When a.BOOK = 'LEGAL' then 'LEGAL'
			 When a.BOOK = 'FIELD' then 'Field'
			 when a.BOOK2 = 'CIC' then 'CIC' 
			 when a.book3 = 'EARLY CALL' then 'EARLY CALL'
			 when BOOk3 in ('LATE CALL','LATE CALL (N4 - 5)') then 'LATE CALL' 
			 when BOOK3 in ('HARD CALL TCC','TCC') then 'TCC'
			 end BOOK_sub
	   ,count(distinct name)  No_of_employee
into #No_of_employeee
from #Bonus_by_collector a
where a.FLAG in ('COLLECTOR')
group by CONVERT(varchar(7),MONTH,120) 
	   , BOOK
       ,case when a.BOOK = 'M1-M4' and convert(varchar(10),MONTH,120) <= '2023-11-01' then 'M1-M4'
		     when a.BOOK = 'M1-M4' and convert(varchar(10),MONTH,120) > '2023-11-01' then BOOK2    
			 When a.BOOK = 'LEGAL' then 'LEGAL'
			 When a.BOOK = 'FIELD' then 'Field'
			 when a.BOOK2 = 'CIC' then 'CIC' 
			 when a.book3 = 'EARLY CALL' then 'EARLY CALL'
			 when BOOk3 in ('LATE CALL','LATE CALL (N4 - 5)') then 'LATE CALL' 
			 when BOOK3 in ('HARD CALL TCC','TCC') then 'TCC'
			 end 

 -- select * from #No_of_employeee where Process_month='2024-10'


drop table if exists #QLTDKH_COST
select  [Month ] 
	   --,BOOK
	   ,case when BOOK ='TCC_CIC' then 'CIC'
	   else BOOK
	   end BOOK
	   ,SUM(Cost) cost
	   ,sum(case when Sub_cost_type in (N'Nhân viên',N'Chuyên viên',N'Chuyên gia') then f6 else 0 end)    no_of_employee ------b? sung Chuyên Gia
into #QLTDKH_COST 
from qlrr2.QLTDKH_COST
group by [Month ] 
	   ,BOOK

--select * from qlrr2.QLTDKH_COST where Month='2024-10'order by BOOK

--select * from #QLTDKH_COST where Month='2024-10'


drop table if exists #Staff_cost_per_employee; ------- tinh chi phi luong co ban trung binh tung nhan vien
select  b.[Month ] process_month
	   ,a.BOOK
	   ,b.BOOK BOOK_sub
	   ,a.No_of_employee No_of_employee_by_bonus 
	   ,b.no_of_employee no_of_employee_by_base
	   ,case when a.No_of_employee is not null then a.No_of_employee
			 when a.No_of_employee is null then b.No_of_employee
			 end No_of_employee
	   ,c.total_employee   -------- T?ng nhân viên
	   ,b.cost total_salary_by_book  --------- t?ng l??ng cb theo t?ng book nghi?p v?
	   ,d.cost total_BOM_oper_salary --------- tông l??ng cb c?a v?n hành và BOM
	   ,b.cost/(case when a.No_of_employee is not null then a.No_of_employee
					 when a.No_of_employee is null and b.No_of_employee > 0 then b.No_of_employee
					 end) average_salary_per_employee ------ trung bình l??ng 1 nv nghi?p v?
	   ,d.cost/c.total_employee staff_operation_cost_per_employee ------------ trung bình l??ng nv BO trên ??u 1 NV


	   ,(b.cost/((case when a.No_of_employee is not null then a.No_of_employee
					   when a.No_of_employee is null and b.No_of_employee > 0  then b.No_of_employee
					   end))) + (d.cost/c.total_employee) total_staff_cost_per_employee  ------ T?ng chi phí l??ng/ 1 nhân viên nghi?p v? (LCB + trung bình l??ng Back office)
	   ,(b.cost/((case when a.No_of_employee is not null then a.No_of_employee
					   when a.No_of_employee is null and b.No_of_employee > 0 then b.No_of_employee
					   end))) Team_average_staff_Cost -------- Chi phí l??ng trung bình c?a team 
	   ,(d.cost/c.total_employee) * ((case when a.No_of_employee is not null then a.No_of_employee
										   when a.No_of_employee is null and b.No_of_employee > 0 then b.No_of_employee
										   end)) vh_staff_cost ------ Chi phí l??ng v?n hành


into #Staff_cost_per_employee
from #QLTDKH_COST b
left join #No_of_employeee a  on a.Process_month = b.[Month ] and a.BOOK_sub = b.book	
left join #total_employee c on b.[Month ] = c.Process_month
left join (select a.[Month ], sum(cost) cost  
		   from qlrr2.qltdkh_cost a
		   where book in ('BOM','Operating','SKIPS','QC') ---- Ch? l?y back office 
		   group by a.[Month ]) d on b.[Month ] = d.[Month ] 
order by a.Process_month

-- select * from #Staff_cost_per_employee where process_month='2024-10'


drop table if exists #hc_manger_cost  ----- tính cost cho manager harcall sau ?ó chia op cost ? d??i --- 
select  h.Process_month
	   ,a.Cost/ h.no_of_allocation Hc_manager_op_cost
into #hc_manger_cost
from #QLTDKH_COST a 
left join (select Process_month, SUM(no_of_allocation) no_of_allocation 
 			from #team_allocation
			where Book in ('CIC','EARLY CALL','LATE CALL')
			group by Process_month) h on a.[Month ]= h.Process_month 
where a.BOOK = 'HC Manager'




drop table if exists #Staff_cost_sum ---------- Map cost ve theo nhom nghiep vu chi tiet------------
select b.process_month
	  ,case when book2 in ('AGENCY','LEGAL CALL', 'LITIGATION') then book2
			when BOOK2 like  '%CIC%' then 'CIC'
			when BOOK2 like '%M1%'then 'M1'
			when BOOK2 like '%M2%'then 'M2'
			when BOOK2 like '%M3-M4%' then 'M3-M4'
			when a.book2 is null then b.BOOK_sub
			else a.book end book
	  ,b.total_staff_cost_per_employee
	  ,b.Team_average_staff_Cost
	  ,b.vh_staff_cost
	  ,b.No_of_employee No_of_employee_2 
	  ,sum(cnt) no_of_allocation
	  ,sum(case when a.FLAG = 'COLLECTOR' then 1 else 0 end) No_of_employee
into #Staff_cost_sum
from #Staff_cost_per_employee b 
left join #Bonus_by_collector a on CONVERT(varchar(7),month,120) = b.Process_month and a.BOOk = b.BOOK_sub and a.FLAG = 'COLLECTOR'
left join (select MONTH,book, count(distinct book2) no_of_book2 from #Bonus_by_collector where FLAG = 'COLLECTOR' group by month, book) c on a.book = c.book and a.MONTH = c.MONTH
where (a.FLAG = 'COLLECTOR' or a.FLAG is null)
group by b.process_month
	  ,case when book2 in ('AGENCY','LEGAL CALL', 'LITIGATION') then book2
			when BOOK2 like  '%CIC%' then 'CIC'
			when BOOK2 like '%M1%'then 'M1'
			when BOOK2 like '%M2%'then 'M2'
			when BOOK2 like '%M3-M4%' then 'M3-M4'
			when a.book2 is null then b.BOOK_sub
			else a.book end
	  ,b.No_of_employee
	  ,b.total_staff_cost_per_employee
	  ,b.Team_average_staff_Cost
	  ,b.vh_staff_cost

-- select * from #Staff_cost_sum	 where process_month='2024-10'

-- select * from #Staff_cost_per_employee where process_month='2024-10'



drop table if exists #Staff_cost_by_account; -------------tinh staff cho account duoc allocation
select	a.process_month
	   ,a.book
	   ,a.No_of_employee No_of_employee_1 
	   ,a.No_of_employee_2
	   ,(a.total_staff_cost_per_employee*(case when a.book in ('M1','M2','M3-M4') and a.process_month < '2023-12' then  a.No_of_employee
			 when a.book in ('M1','M2','M3-M4') and a.process_month >= '2023-12' then  a.No_of_employee_2
			 else case when a.No_of_employee_2 = 0 or a.No_of_employee_2 is null then a.No_of_employee else a.No_of_employee_2 end end ))/b.no_of_allocation Staff_cost_by_account_by_book_allocation --- tinh theo cach lay total staff cost trung binh chia cho allocaton theo team 
	   ,((a.Team_average_staff_Cost*(case when a.book in ('M1','M2','M3-M4') and a.process_month < '2023-12' then  a.No_of_employee
			 when a.book in ('M1','M2','M3-M4') and a.process_month >= '2023-12' then  a.No_of_employee_2
			 else case when a.No_of_employee_2 = 0 or a.No_of_employee_2 is null then a.No_of_employee else a.No_of_employee_2 end end ))/b.no_of_allocation) + ((a.vh_staff_cost)/(b.Total_no_of_allocation)) distinct_staff_cost_by_account  ---- tinh rieng cost cua nghiep vu va van hanh 
	   ,(a.Team_average_staff_Cost*(case when a.book in ('M1','M2','M3-M4') and a.process_month < '2023-12' then  a.No_of_employee
			 when a.book in ('M1','M2','M3-M4') and a.process_month >= '2023-12' then  a.No_of_employee_2
			 else case when a.No_of_employee_2 = 0 or a.No_of_employee_2 is null then a.No_of_employee else a.No_of_employee_2 end end ))/b.no_of_allocation Staff_cost_by_account -- staff costt nghiep vu
	   ,a.vh_staff_cost	
	   ,a.Team_average_staff_Cost
	   ,case when a.book in ('M1','M2','M3-M4') and a.process_month < '2023-12' then  a.No_of_employee
			 when a.book in ('M1','M2','M3-M4') and a.process_month >= '2023-12' then  a.No_of_employee_2
			 else case when a.No_of_employee_2 = 0 or a.No_of_employee_2 is null then a.No_of_employee else a.No_of_employee_2 end end No_of_employee
	   ,b.no_of_allocation
	   ,case when a.book in ('M1','M2','M3-M4') then (a.vh_staff_cost)/(h.no_of_allocation)
			 when a.book in ('LITIGATION','LEGAL CALL','AGENCY') then (a.vh_staff_cost)/(c.no_of_allocation)
		     else (a.vh_staff_cost)/(b.no_of_allocation) end vh_staff_cost_by_account ---- staff cost cua van hanh chia deu all account
	    
into #Staff_cost_by_account
from #Staff_cost_sum a
left join #team_allocation b on a.process_month = b.Process_month and a.book = b.Book
left join (select Process_month, SUM(no_of_allocation) no_of_allocation 
 			from #team_allocation
			where Book in ('M1','M2','M3-M4')
			group by Process_month) h on a.Process_month = h.Process_month and a.Book in ('M1','M2','M3-M4')
left join (select Process_month, SUM(no_of_allocation) no_of_allocation 
			from #team_allocation
			where Book in ('LITIGATION','LEGAL CALL','AGENCY')
			group by Process_month) c on a.Process_month = c.Process_month and a.Book in ('LITIGATION','LEGAL CALL','AGENCY')
---SELECT * FROM #Staff_cost_by_account WHERE process_month='2024-10'



--------------------------------------OP COST BASE---------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------


drop table if exists #op_cost_base ----- Sum all operation code (khong bao gom phi sms tts cua M1)
select a.[Month ] Process_month 
	  ,case when a.cost_type in ('Early') then 'EARLY CALL'
			when a.cost_type in ('LATE','N4-5') then 'LATE CALL'
			when a.cost_type in ('CIC', 'CIC-TCC', 'TCC-CIC','DLC') then 'CIC'  ------------ update %CIC%
			when a.Cost_type = 'LITIGATION' then 'LITIGATION'
			when a.Cost_type = 'AGENCY' then 'AGENCY'
			when a.BOOK = 'LEGAL' and a.Cost_type <> 'LITIGATION' then 'LEGAL CALL'
			when a.BOOK = 'HARDCALL' then 'HARD CALL'
			else a.book end book
	  ,sum(cost) op_cost
into #OP_cost_base
from qlrr2.QLTDKH_OP_Cost a
where CONCAT(book,Sub_cost_type) not in ('M1SMS','M1TTS')
group by  a.[Month ]
		 ,case when a.cost_type in ('Early') then 'EARLY CALL'
			when a.cost_type in ('LATE','N4-5') then 'LATE CALL'
			when a.cost_type in ('CIC', 'CIC-TCC', 'TCC-CIC','DLC') then 'CIC'  ------------ update %CIC%
			when a.Cost_type = 'LITIGATION' then 'LITIGATION'
			when a.Cost_type = 'AGENCY' then 'AGENCY'
			when a.BOOK = 'LEGAL' and a.Cost_type <> 'LITIGATION' then 'LEGAL CALL'
			when a.BOOK = 'HARDCALL' then 'HARD CALL'
			else a.book end

-- select * from #OP_cost_base where Process_month='2024-10'
-- select * from qlrr2.QLTDKH_OP_Cost where month='2024-10'


drop table if exists #op_cost_base_field ----- Sum all operation code field theo vung
select a.[Month ] Process_month 
	  ,cost_type book 
	  ,SUM(cost) op_cost
into #OP_cost_base_field
from qlrr2.QLTDKH_OP_Cost a
where BOOK = 'FIELD'
group by  a.[Month ]
		 ,Cost_type

-- SELECT * FROM #op_cost_base_field

drop table if exists #op_cost_base_M1 ----- tach phi sms tts cua M1
select a.[Month ] Process_month 
	  ,book 
	  ,SUM(cost) op_cost
into #OP_cost_base_M1
from qlrr2.QLTDKH_OP_Cost a
where CONCAT(book,Sub_cost_type) in ('M1SMS','M1TTS')
group by  a.[Month ]
		 ,book


-- SELECT * FROM  #op_cost_base_M1


drop table if exists #Team_operation_cost ----- Map team operation code with team allocation (khong co field va phi legal khac)
Select a.Process_month
	  ,a.Book
	  ,a.no_of_allocation --- khong bao gom M1 aa va M1 ar
	  ,b.op_cost							team_operation_cost
into #Team_operation_cost
from #team_allocation a
left join #OP_cost_base b on a.Process_month = b.Process_month and a.Book = b.book  
where a.Book <> 'FIELD'

---SELECT * FROM #Team_operation_cost
---SELECT distinct Book FROM #team_allocation

---SELECT * FROM #Team_operation_cost order by Process_month desc


drop table if exists #op_cost_by_case --------- tính nh?ng op cost k chia theo nghi?p v? theo t?ng account
select distinct a.Process_month
	  ,a.book
	  ,a.op_cost
	  ,b.Total_no_of_allocation
	  ,a.op_cost operation_cost_by_case
into #op_cost_by_case
from #OP_cost_base a
left join #team_allocation b on a.Process_month = b.Process_month 
where a.book = 'Operation' 
--SELECT * FROM #OP_cost_base
order by process_month desc


drop table if exists #HC_op_cost --------- tính nh?ng op cost cua HC
select distinct a.Process_month
	  ,a.book
	  ,a.op_cost
	  ,b.no_of_allocation Total_no_of_allocation
	  ,a.op_cost operation_cost_by_case
into #HC_op_cost
from #OP_cost_base a
left join #hc_allocation b on a.Process_month = b.Process_month   
where a.book = 'HARD CALL' 

-- SELECT * FROM #HC_op_cost


drop table if exists #Team_operation_cost_Field -----  Tinh op cost cho Field/ chia theo vung
Select a.Process_month
	  ,'FIELD' Book
	  ,a.Book Book2
	  ,a.no_of_allocation --- khong bao gom M1 aa va M1 ar
	  ,b.op_cost team_operation_cost 
	  ,(c.operation_cost_by_case*D.No_of_employee)/(d.total_employee*a.no_of_allocation) other_op_cost_by_case
	  ,0 TTS_SMS_cost_M1
	  ,g.vh_staff_cost_by_account
	  ,(b.op_cost/a.no_of_allocation + (c.operation_cost_by_case*D.No_of_employee)/(d.total_employee*a.no_of_allocation)) + g.vh_staff_cost_by_account total_operation_cost_by_account
into #Team_operation_cost_field
from #Field_allocation a
left join #OP_cost_base_field b on a.Process_month = b.Process_month and a.Book = b.book  
left join #op_cost_by_case c on a.Process_month = c.Process_month
left join #Employee_base d on a.Process_month  = d.process_month and a.book = d.book2 AND D.employee_lvl = 'COLLECTOR'
left join (select distinct process_month,book, vh_staff_cost_by_account from #Staff_cost_by_account) g on a.Process_month = g.process_month and g.book = 'FIELD'
-- SELECT * FROM #Team_operation_cost_Field


drop table if exists #Total_op_cost_by_account; --- chia t?ng op cost theo account
select a.Process_month
	  ,a.Book
	  ,case when a.Book in ('LATE CALL', 'EARLY CALL') then 'HARD CALL' 
			else a.book end book2
	  ,a.no_of_allocation	
	  ,a.team_operation_cost
	  ,(b.operation_cost_by_case*f.No_of_employee)/(f.total_employee*e.no_of_allocation) other_op_cost_by_case
	  ,case when a.book = 'M1' then d.op_cost/e.Total_no_of_allocation
			else 0 end TTS_SMS_cost_M1
	  ,case when a.Book in ('CIC','EARLY CALL','LATE CALL') then g.vh_staff_cost_by_account + k.Hc_manager_op_cost
			else g.vh_staff_cost_by_account end vh_staff_cost_by_account
	  ,isnull((a.team_operation_cost/a.no_of_allocation),0) + isnull((b.operation_cost_by_case*f.No_of_employee)/(f.total_employee*e.no_of_allocation),0)  + isnull((case when a.book = 'M1' then d.op_cost/e.Total_no_of_allocation else 0 end),0) + isnull(case when a.Book in ('CIC','EARLY CALL','LATE CALL') then g.vh_staff_cost_by_account + k.Hc_manager_op_cost
			else g.vh_staff_cost_by_account end,0) +isnull((case when a.book in ('EARLY CALL','LATE CALL','CIC') then (h.op_cost/h.Total_no_of_allocation) else 0 end),0) total_operation_cost_by_account ----add cic

into #Total_op_cost_by_account
from #Team_operation_cost a
left join #op_cost_by_case b on a.Process_month = b.Process_month
left join #HC_op_cost h on a.Process_month = h.process_month
left join #OP_cost_base_M1 d on a.Process_month = d.Process_month
left join #team_allocation e on a.Process_month = e.Process_month and a.Book = e.Book
left join #Employee_base f on a.Process_month  = f.process_month and a.Book = f.book and f.employee_lvl  = 'COLLECTOR'
left join (select distinct process_month,book, vh_staff_cost_by_account from #Staff_cost_by_account) g on a.Process_month = g.process_month and a.Book = g.book
left join #hc_manger_cost k on a.Process_month = k.Process_month
union all
Select * from #Team_operation_cost_field

-- select * from #Total_op_cost_by_account where Process_month = '2024-10'



-----------------------------------Bonus Base--------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
drop table if exists #Bonus_base
select a.process_month
	  ,a.book
	  ,a.book2
	  ,a.bonus_deduction               Total_team_bonus
	  ,a.no_of_allocation              total_team_allocation
	  ,a.No_of_employee                total_employee
	  ,case when a.book like '%M1%'then '1'
			 when a.book like '%M2%'then '1'
			 when a.book like '%M3-M4%' then '1' 
			 when a.book = 'CIC' then '1'
			 else b.AGING_2 end Onus_debt_group
	  ,case when a.book like '%M1%'then a.bonus_deduction
			 when a.book like '%M2%'then a.bonus_deduction
			 when a.book like '%M3-M4%' then a.bonus_deduction
			 when a.book = 'CIC' then a.bonus_deduction
			 else B.BONUS_AMT end Bonus_by_aging
	  ,b.TOTAL_PAYMENT                 Ovd_payment_by_CSA
into #Bonus_base
from #sum_bonus_basse A
left join qlrr2.bonus_by_aging B on a.process_month = b.ALLOCATED_MONTH and a.book = b.book 
where a.employee_lvl = 'COLLECTOR'

--select * from #Bonus_base where process_month ='2024-10' 



-----------------------------------N?i phép màu x?y ra-----------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

								-----Cach tiep can 2------

drop table if exists #sum_by_book_and_aging   -------------------------- lay tong theo book va aging -------------------------------------------
Select a.Process_month
		,a.Book
		,a.Book_2
		,case when a.Allocation_aging = 'WO' then 'WO'  
			else LEFT(a.Allocation_aging,1) end Onus_debt_group
		,count(1) no_of_allocation_by_risk_By_BA
		,sum(a.allocation_TAD) allocation_TAD_By_BA
		,sum(b.Payment) Total_Payment_By_BA
into #sum_by_book_and_aging
from #allocation_base a
left join #PAYMENT b on a.account_number = b.account_number and (b.process_dt between a.allocation_date and a.end_date)
left join qlrr2.CIC_ALL c on a.account_number = c.account_number and a.Process_month = left(c.Next_CIC_Period,7)
Group by a.Process_month
		,a.Book
		,a.Book_2
		,LEFT(a.Allocation_aging,1) 
		,case when a.Allocation_aging = 'WO' then 'WO' else LEFT(a.Allocation_aging,1) end



drop table if exists #sum_by_book -------------------------------------- lay tong theo book-------------------------------------------------
Select a.Process_month
		,a.Book
		,a.Book_2
		,count(1) no_of_allocation_by_risk_By_Book
		,sum(a.allocation_TAD) allocation_TAD_By_Book
		,sum(b.Payment) Total_Payment_By_Book
into #sum_by_book
from #allocation_base a
left join #PAYMENT b on a.account_number = b.account_number and (b.process_dt between a.allocation_date and a.end_date)
left join qlrr2.CIC_ALL c on a.account_number = c.account_number and a.Process_month = left(c.Next_CIC_Period,7)
Group by a.Process_month
		,a.Book
		,a.Book_2


	--Merge allocation & Payment
	drop table if exists #allocation_base_payment
	Select a.account_number 
		  ,a.Process_month
		  ,a.Book
		  ,a.Book_2
		  ,a.Allocation_aging
		  ,case when a.Allocation_aging = 'WO' then 'WO'  
				when a.Allocation_aging = '_TCC' then '1'
				when a.Allocation_aging = 'ACSTWOFF' then 'WO'
				else LEFT(a.Allocation_aging,1) end Onus_debt_group
		  ,c.Last_CIC
		  ,case when d.card_type_group_name = 'ExpressCard' then '1. ExpressCard'
				when d.card_type_group_name = 'StateCard' then '2. StateCard'
				when d.card_type_group_name = 'Salary Card' then '3. SalaryCard'
				when d.card_type_group_name = 'StudentsCard' then '4. StudentsCard'
				when d.card_type_group_name = 'MortgageCard' then '5. MortgageCard'
				when d.card_type_group_name = 'FastCard' then '6. FastCard'
				when d.card_type_group_name = 'FarmerCard' then '7. FarmerCard'
				when d.card_type_group_name = 'AutoCard' then '8. AutoCard'
				else '9. Other' end card_type_group_name
		  ,sum(a.allocation_TAD) allocation_TAD
		  ,sum(a.allocation_MAD) allocation_OS
		  ,sum(b.Payment) Total_Payment
		  ,sum(b.OS_Payment) Total_OS_Payment
		  ,SUM(b.int_fee_payment) total_int_fee_payment
	into #allocation_base_payment
	from #allocation_base a
	left join #PAYMENT b on a.account_number = b.account_number and (b.process_dt between a.allocation_date and a.end_date)
	left join qlrr2.CIC_ALL c on a.account_number = c.account_number and a.Process_month = left(c.Next_CIC_Period,7)
	left join ODS_DW_APPLICATION_DIM d on a.account_number = d.account_number
	Group by a.account_number 
		  ,a.Process_month
		  ,a.Book
		  ,a.Book_2
		  ,LEFT(a.Allocation_aging,1) 
		  ,case when a.Allocation_aging = 'WO' then 'WO'  
				else LEFT(a.Allocation_aging,1) end
		  ,Last_CIC
		  ,case when d.card_type_group_name = 'ExpressCard' then '1. ExpressCard'
				when d.card_type_group_name = 'StateCard' then '2. StateCard'
				when d.card_type_group_name = 'Salary Card' then '3. SalaryCard'
				when d.card_type_group_name = 'StudentsCard' then '4. StudentsCard'
				when d.card_type_group_name = 'MortgageCard' then '5. MortgageCard'
				when d.card_type_group_name = 'FastCard' then '6. FastCard'
				when d.card_type_group_name = 'FarmerCard' then '7. FarmerCard'
				when d.card_type_group_name = 'AutoCard' then '8. AutoCard'
				else '9. Other' end
		  ,a.Allocation_aging

	--add infomation-------------------
	drop table if exists #account_base
	Select distinct a.account_number
		  ,a.Process_month
		  ,a.Book
		  ,a.Book_2
		  ,a.Allocation_aging
		  ,a.Onus_debt_group		
		  ,case when a.Onus_debt_group = 'WO' then 5
				else iif(a.Onus_debt_group  >= isnull(a.Last_CIC,0),a.Onus_debt_group,a.last_cic) end off_us
		  ,a.card_type_group_name
		  ,case when a.Book_2 = 'M1 AA' then 1
		        else isnull(h.RB,0) end RB
		  ,case when a.Book_2 = 'M1 AA' then a.allocation_OS
				when h.rb = 1 then a.allocation_OS
				else 0 end RB_OS
		  ,z.No_of_employee total_employee
		  ,1 acc
		  ,a.allocation_TAD
		  ,a.allocation_os
		  ,a.Total_Payment
		  ,a.Total_OS_Payment
		  ,a.total_int_fee_payment
		  ,d.Bonus_by_aging total_Bonus_by_aging_and_book
		  ,e.bonus_deduction total_team_bonus 
		  ,f.allocation_TAD_By_Book
		  ,g.allocation_TAD_By_BA

		  --bonus
		  ,case when a.Book_2 in ('M1 AR', 'M1 AA') then 0  
				else (a.Total_Payment*(d.Bonus_by_aging))/g.Total_Payment_By_BA  end bonus_by_case_on_aging
		  ,case when a.Book_2 in ('M1 AR', 'M1 AA') then 0  
				else (a.Total_Payment*(e.bonus_deduction))/f.Total_Payment_By_Book end bonus_by_case_on_book
		  ,case when a.Book_2 in ('M1 AR', 'M1 AA') then 0  
				else (a.Total_Payment*(b.bonus_deduction))/f.Total_Payment_By_Book end Manager_bonus_by_case_on_book
		  ,case when a.Book_2 in ('M1 AR', 'M1 AA') then 0  
				else (a.Total_Payment*(c.bonus_deduction))/f.Total_Payment_By_Book end Teamlead_bonus_by_case_on_book

		  --staff cost
		  ,case when a.Book_2 in ('M1 AR', 'M1 AA') then 0
				else k.Staff_cost_by_account end Staff_cost_by_account
		  ,case when a.Book_2 in ('M1 AR', 'M1 AA') then m.TTS_SMS_cost_M1
				else n.total_operation_cost_by_account end total_operation_cost_by_account

		  --extra infor-----
		  ,k.Staff_cost_by_account_by_book_allocation     Staff_no_aa_ar
		  ,n.other_op_cost_by_case                        vh_op_cost
		  ,case when a.Book_2 in ('M1 AR', 'M1 AA') then m.TTS_SMS_cost_M1
				else 0 end TTS_SMS_cost_M1

	into #account_base
	from #allocation_base_payment a
	--bonus
	left join #bonus_base d on a.Process_month = d.process_month and a.Book = d.book and a.Onus_debt_group = d.Onus_debt_group and a.Book_2 = d.book2
	left join #sum_bonus_basse e on a.Process_month = E.process_month and a.Book = E.book and a.Book_2 = e.book2 and e.employee_lvl = 'COLLECTOR'
	left join #sum_bonus_basse b on a.Process_month = b.process_month and a.Book = b.book and a.Book_2 = b.book2 and b.employee_lvl = 'MANAGER'
	left join #sum_bonus_basse c on a.Process_month = c.process_month and a.Book = c.book and a.Book_2 = c.book2 and c.employee_lvl in ( 'TEAM LEADER','TEAMLEADER')

	--add-in infromation
	left join #sum_by_book             F on a.Process_month = F.process_month and a.Book = f.book and a.Book_2 = f.Book_2
	left join #sum_by_book_and_aging   G on a.Process_month = g.process_month and a.Book = g.book and a.Book_2 = g.Book_2 and a.Onus_debt_group = g.Onus_debt_group 
	--add-in rb 
	left join qlrr2.DATAMART           h on a.Book in ('M1','M2','M3-M4') and a.account_number = h.account_number and a.Process_month = h.INVOICE_MONTH
	--cost
	left join #Staff_cost_by_account       k on a.Process_month = k.process_month and a.Book = k.book  
	left join #Total_op_cost_by_account    N on a.Process_month = n.Process_month and a.Book = n.Book and a.Book_2 = n.book2 
	left join #Total_op_cost_by_account    M on a.Process_month = M.Process_month and a.Book = M.Book 
	--Employee
	left join #Employee_base Z on a.Process_month = z.process_month and a.Book = z.book and a.Book_2 = z.book2 and z.employee_lvl = 'COLLECTOR'

	--------Import TO Power BI -------------
-----------------------------------------
drop table if exists qlrr2.Raw_Profit_model
select *
into qlrr2.Raw_Profit_model
from #account_base