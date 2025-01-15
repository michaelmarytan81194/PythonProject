USE [DP_Tan]
GO

/****** Object:  StoredProcedure [dbo].[COMMISION_CALCULATE_NEW]    Script Date: 01-Apr-2024 2:37:20 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[COMMISION_CALCULATE_NEW] (@varInputMonth nvarchar(50))
as 
begin
--I. Lấy bảng:
-- Cắt 2 ra kỳ 
with CUTOFF_1 AS (
SELECT DISTINCT CUTOFF
FROM [SQL_SV65].[PowerBI].[DPO].[DP_TAGENTCOMM_TEST_CUTOFF]
WHERE SUBSTRING(CUTOFF,5,2) =  SUBSTRING(FORMAT(CAST(@varInputMonth AS date), 'yyyyMM'),5,2) --SỬA
AND LEFT(CUTOFF,4) =  LEFT(FORMAT(CAST(@varInputMonth AS date), 'yyyyMM'),4)
)

, CUTOFF AS (SELECT *,
CONCAT(LEFT(CUTOFF,4),'-', SUBSTRING(CUTOFF,5,2),'-',RIGHT(CUTOFF,2)) DATE_CUTOFF,
ROW_NUMBER() OVER(ORDER BY CUTOFF) AS Pri_No
FROM CUTOFF_1)

--1. Lấy kỳ I:
, AGENT_INFO AS(
select Area_Name, Sales_Unit_Code, Sales_Unit, Agent_Number, Agent_Name, Grade, Agent_Status, Appointed_TAPSU as Appointed_Date,
Terminated_date, CUTOFF
from [SQL_SV65].[PowerBI].[DPO].[Main_AGENT_INFO_DA_CUTOFF]
WHERE CUTOFF = (SELECT CUTOFF FROM CUTOFF
				WHERE Pri_No  = 1) )

, COM_INFO AS(
SELECT * FROM [SQL_SV65].[PowerBI].[DPO].[DP_TAGENTCOMM_TEST_CUTOFF]
WHERE CUTOFF = (SELECT CUTOFF FROM CUTOFF
				WHERE Pri_No  = 1) 
)

, COM_FILTER_1 AS (
SELECT * FROM COM_INFO
WHERE [Policy Status] NOT IN ('SU', 'TR') 
and ([Applied Premium Date] BETWEEN @varInputMonth 
AND (select DATE_CUTOFF from CUTOFF where Pri_No = 1)) 
)

, COM_PERIOD_I AS (
select *,
(datediff(day, [Receive Policy date] ,(SELECT DATE_CUTOFF FROM CUTOFF WHERE Pri_No  =1)) +1) as Freelook_New
from COM_FILTER_1)

-- Tính PY2 kỳ I:
, PY2_I as (SELECT [AGENT NO], (SUM([Y2 ACTUAL PREM])/NULLIF(SUM([Y2 EXPECTED PREM]),0)) AS PY2 FROM [SQL_SV65].[PowerBI].[DPO].[DP_K2_CUTOFF]
where CUTOFF = (SELECT CUTOFF FROM CUTOFF WHERE Pri_No  = 1) 
GROUP BY [AGENT NO] )

-- Lấy ACK Day kỳ I:
, ACK_I as (select [Policy No], [POLICY ACKNOWLED], [Frequency of Payment], count(*) as Count_Temp	
from [SQL_SV65].[PowerBI].[DPO].[DP_TAGENTPREMIUM_TEST_CUTOFF]
where CUTOFF = (SELECT CUTOFF FROM CUTOFF WHERE Pri_No  = 1)
group by [Policy No], [POLICY ACKNOWLED], [Frequency of Payment]
)

--BƯỚC NÀY LÀ GROUP BY LẠI THEO PREMIUM, FYC, RYC
, SUM_COM_I AS (
select [Commission Agent], [Policy No], [Policy Status], [Issued Date], [Applied Premium Date], [Receive Policy date], Freelook_New,
sum([Premium Collected]) as Total_Premium,
sum(FYC) as Total_FYC,
sum(RYC) as Total_RYC,
sum(FYC) + SUM (RYC) AS Total_Com
from COM_PERIOD_I
group by [Commission Agent], [Policy No], [Policy Status], [Issued Date], [Applied Premium Date], [Receive Policy date], Freelook_New)

, SUM_COM_II AS(
SELECT a.Area_Name, a.Sales_Unit_Code, a.Sales_Unit, a.Agent_Number, a.Agent_Name, a.Grade, a.Agent_Status,
a.Appointed_Date, a.Terminated_date, 
b.[Policy No], b.[Policy Status], b.[Issued Date], b.[Receive Policy date], b.Freelook_New,
b.Total_Premium, b.Total_FYC, b.Total_RYC, b.Total_Com,
a.CUTOFF
FROM AGENT_INFO a
LEFT JOIN SUM_COM_I b
ON a.Agent_Number = b.[Commission Agent])

, SUM_COM_III AS(
SELECT a.*, b.*,
CASE
WHEN (Freelook_New > 21 and (Terminated_date IS NULL OR Terminated_date = '') and [Policy Status] <> 'FL'
and ([Receive Policy date] IS NOT NULL OR [Receive Policy date] <> '') and (c.[POLICY ACKNOWLED] IS NOT NULL OR c.[POLICY ACKNOWLED] <> ''))
OR -- VIẾT CH0 TH DL TRÊN 14 THÁNG và PY2 > 50%
( (DATEADD(MONTH, 14, Appointed_Date) < (select DATE_CUTOFF from CUTOFF where Pri_No = 1)) 
and (b.PY2 > 0.5) and (Terminated_date IS NULL OR Terminated_date = '') and [Policy Status] <> 'FL'
and (c.[POLICY ACKNOWLED] IS NOT NULL OR c.[POLICY ACKNOWLED] <> '')
)
THEN N'Thanh toán-A'
WHEN Freelook_New < 21 and (Terminated_date IS NULL OR Terminated_date = '') and [Policy Status] <> 'FL'
THEN N'Chưa qua 21 ngày cân nhắc-B'
WHEN ([Receive Policy date] IS NULL OR [Receive Policy date] = '') and (Terminated_date IS NULL OR Terminated_date = '') and
Freelook_New > 21 and [Policy Status] <> 'FL' and (c.[POLICY ACKNOWLED] IS NULL OR c.[POLICY ACKNOWLED] = '')
THEN N'Chưa trả ACK-C'
WHEN [Policy Status] = 'FL' and (Terminated_date IS NULL OR Terminated_date = '') THEN N'Hủy trong 21 ngày cân nhắc-E'
WHEN Freelook_New > 21 and NOT(Terminated_date IS NULL OR Terminated_date = '') THEN N'ĐL Ter trước khi phát sinh phí-D' 
END AS Note
FROM SUM_COM_II a 
LEFT JOIN PY2_I b
on a.Agent_Number = b.[AGENT NO]
left join ACK_I C
ON a.[Policy No] = c.[Policy No]
where Total_Com <> 0)

, SUM_COM_KY_I AS(
SELECT *,
RIGHT(Note,1) AS Mark,
CUTOFF AS Ti_To,
case 
when RIGHT(Note,1) = 'A' THEN CUTOFF 
when RIGHT(Note, 1) = 'E' AND Freelook_New > 21 THEN CUTOFF
END AS Th_To,
CUTOFF AS Kh_So
from SUM_COM_III)

--2. Lấy kỳ 2:
, AGENT_INFO_II AS(
select Area_Name, Sales_Unit_Code, Sales_Unit, Agent_Number, Agent_Name, Grade, Agent_Status, Appointed_TAPSU as Appointed_Date,
Terminated_date, CUTOFF
from [SQL_SV65].[PowerBI].[DPO].[Main_AGENT_INFO_DA_CUTOFF]
WHERE CUTOFF = (SELECT CUTOFF FROM CUTOFF
				WHERE Pri_No  = 2))
-- BẢNG HH KỲ 2:
, COM_FILTER_II AS (SELECT *,
(datediff(day, [Receive Policy date] ,(SELECT DATE_CUTOFF FROM CUTOFF WHERE Pri_No  =2)) +1) as Freelook_New
FROM [SQL_SV65].[PowerBI].[DPO].[DP_TAGENTCOMM_TEST_CUTOFF]
where [Policy Status] NOT IN ('SU', 'TR') 
and ([Applied Premium Date] BETWEEN @varInputMonth 
AND (select DATE_CUTOFF from CUTOFF where Pri_No = 2) )
and CUTOFF = (SELECT CUTOFF FROM CUTOFF
				WHERE Pri_No  = 2))

-- Tính PY2 kỳ I:
, PY2_II as (SELECT [AGENT NO], (SUM([Y2 ACTUAL PREM])/NULLIF(SUM([Y2 EXPECTED PREM]),0)) AS PY2 FROM [SQL_SV65].[PowerBI].[DPO].[DP_K2_CUTOFF]
where CUTOFF = (SELECT CUTOFF FROM CUTOFF WHERE Pri_No  = 2) 
GROUP BY [AGENT NO] )

--BƯỚC NÀY LÀ GROUP BY LẠI THEO PREMIUM, FYC, RYC
, KY2_COM_I AS (
select [Commission Agent], [Policy No], [Policy Status], [Issued Date], [Applied Premium Date], [Receive Policy date], Freelook_New,
sum([Premium Collected]) as Total_Premium,
sum(FYC) as Total_FYC,
sum(RYC) as Total_RYC,
sum(FYC) + SUM (RYC) AS Total_Com
from COM_FILTER_II
group by [Commission Agent], [Policy No], [Policy Status], [Issued Date], [Applied Premium Date], [Receive Policy date], Freelook_New)

, Ky2_COM_II AS(
SELECT a.Area_Name, a.Sales_Unit_Code, a.Sales_Unit, a.Agent_Number, a.Agent_Name, a.Grade, a.Agent_Status,
a.Appointed_Date, a.Terminated_date, 
b.[Policy No], b.[Policy Status], b.[Issued Date], b.[Receive Policy date], b.Freelook_New,
b.Total_Premium, b.Total_FYC, b.Total_RYC, b.Total_Com,
a.CUTOFF
FROM AGENT_INFO_II a
LEFT JOIN KY2_COM_I b
ON a.Agent_Number = b.[Commission Agent])

, ACK_II as (select [Policy No], [POLICY ACKNOWLED], [Frequency of Payment], count(*) as Count_Temp	
from [SQL_SV65].[PowerBI].[DPO].[DP_TAGENTPREMIUM_TEST_CUTOFF]
where CUTOFF = (SELECT CUTOFF FROM CUTOFF WHERE Pri_No  = 2)
group by [Policy No], [POLICY ACKNOWLED], [Frequency of Payment])

, Ky2_COM_III AS (SELECT a.*, b.*, c.[POLICY ACKNOWLED],
CASE
WHEN (Freelook_New > 21 and (Terminated_date IS NULL OR Terminated_date = '') and [Policy Status] <> 'FL'
and ([Receive Policy date] IS NOT NULL OR [Receive Policy date] <> '') and (c.[POLICY ACKNOWLED] IS NOT NULL OR c.[POLICY ACKNOWLED] <> ''))
OR -- VIẾT CH0 TH DL TRÊN 14 THÁNG và PY2 > 50%
( (DATEADD(MONTH, 14, Appointed_Date) < (select DATE_CUTOFF from CUTOFF where Pri_No = 1)) 
and (b.PY2 > 0.5) and (Terminated_date IS NULL OR Terminated_date = '') and [Policy Status] <> 'FL'
and (c.[POLICY ACKNOWLED] IS NOT NULL OR c.[POLICY ACKNOWLED] <> '')
)
THEN N'Thanh toán-A'
WHEN Freelook_New < 21 and (Terminated_date IS NULL OR Terminated_date = '') and [Policy Status] <> 'FL' 
THEN N'Chưa qua 21 ngày cân nhắc-B'
WHEN ([Receive Policy date] IS NULL OR [Receive Policy date] = '') and (Terminated_date IS NULL OR Terminated_date = '') and
Freelook_New > 21 and [Policy Status] <> 'FL' and (c.[POLICY ACKNOWLED] IS NULL OR c.[POLICY ACKNOWLED] = '')
THEN N'Chưa trả ACK-C'
WHEN [Policy Status] = 'FL' and (Terminated_date IS NULL OR Terminated_date = '') THEN N'Hủy trong 21 ngày cân nhắc-E'
WHEN Freelook_New > 21 and NOT(Terminated_date IS NULL OR Terminated_date = '') THEN N'ĐL Ter trước khi phát sinh phí-D' 
END AS Note
from Ky2_COM_II a
left join PY2_II b
on a.Agent_Number = b.[AGENT NO]
left join ACK_II c
on a.[Policy No] = c.[Policy No]
where Total_Com <> 0)

, SUM_COM_KY_II AS(
SELECT *,
RIGHT(Note,1) AS Mark,
CUTOFF AS Ti_To,
case 
when RIGHT(Note,1) = 'A' THEN CUTOFF 
when RIGHT(Note, 1) = 'E' AND Freelook_New > 21 THEN CUTOFF
END AS Th_To,
CUTOFF AS Kh_So
from Ky2_COM_III)

-- Bóc ra các HD chưa trả ACK, chưa tính của kỳ I
--Bước 1: Lấy ra các HD chưa pass 21 ngày, ACK của kỳ 1
, KY_1_CONTINUE AS (SELECT * FROM SUM_COM_KY_I
where Mark IN ('B','C') )

-- Bước 2: Lấy ra các HD đã pass A, D, E
, A_KY_II AS(
SELECT * FROM SUM_COM_KY_II
where [Policy No] NOT IN (SELECT [Policy No] FROM SUM_COM_KY_I where Mark IN('A', 'D', 'E') ) 
)

--Bước này là lấy lại các kỳ tính toán trước đó, VD: kỳ 1511 chưa xong, tính tiếp 3011
, B_KY_II AS(
SELECT a.*, b.Note as Note_1, b.Mark as Mark_1, b.Ti_To as Ti_To_1, b.Th_To as Th_To_1 , b.Kh_So as Kh_So_1 FROM A_KY_II a
left join KY_1_CONTINUE b
on a.[Policy No] = b.[Policy No])

, C_KY_II AS(
select *,
case 
when Mark <> Mark_1 THEN Ti_To_1 else Ti_To end as Ti_To_Final
from B_KY_II)

-- Lấy ra các cột cần thiết
, D_KY_II AS (SELECT Area_Name, Sales_Unit_Code, Sales_Unit, Agent_Number, Agent_Name, Grade, Agent_Status, Appointed_Date, Terminated_date,
[Policy No], [Policy Status], [Issued Date], [Receive Policy date], Freelook_New, Total_Premium, Total_FYC, Total_RYC, Total_Com,
CUTOFF, Note, Mark, Ti_To_Final AS Ti_To, Th_To, Kh_So
FROM C_KY_II)

, Result as (SELECT Area_Name, Sales_Unit_Code, Sales_Unit, Agent_Number, Agent_Name, Grade, Agent_Status, Appointed_Date, Terminated_date,
[Policy No], [Policy Status], [Issued Date], [Receive Policy date], Freelook_New, Total_Premium, Total_FYC, Total_RYC, Total_Com,
Note, Mark, Ti_To, Th_To, Kh_So
FROM SUM_COM_KY_I where Mark IN('A', 'D', 'E')
UNION
SELECT  Area_Name, Sales_Unit_Code, Sales_Unit, Agent_Number, Agent_Name, Grade, Agent_Status, Appointed_Date, Terminated_date,
[Policy No], [Policy Status], [Issued Date], [Receive Policy date], Freelook_New, Total_Premium, Total_FYC, Total_RYC, Total_Com,
Note, Mark, Ti_To, Th_To, Kh_So
FROM D_KY_II)


, Final_Result as (select *,
case 
when 
(Note IS NULL OR Note = '') AND ([Receive Policy date] IS NULL OR [Receive Policy date] = '') 
THEN N'Chưa trả ACK-C' ELSE Note
END AS Note_Final
from Result)

select 
Area_Name, Sales_Unit_Code, Sales_Unit, Agent_Number, Agent_Name, Grade, Agent_Status, Appointed_Date, Terminated_date,
[Policy No], [Policy Status], [Issued Date], [Receive Policy date], Freelook_New, Total_Premium, Total_FYC, Total_RYC, Total_Com,
Note_Final, Ti_To as N'Kỳ Tính Toán', Th_To as N'Kỳ Thanh Toán', Kh_So as N'Kỳ Khóa Sổ'
from Final_Result
end
GO


