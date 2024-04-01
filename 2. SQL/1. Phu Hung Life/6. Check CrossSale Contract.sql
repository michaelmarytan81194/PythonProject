USE [DP_Tan]
GO

/****** Object:  StoredProcedure [dbo].[CHECK_CROSS_SALES_CONTRACT]    Script Date: 01-Apr-2024 2:36:56 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[CHECK_CROSS_SALES_CONTRACT] (@varREPORT_MONTH nvarchar(100))
-- BIẾN SỐ: '2023-05-01'
AS
BEGIN
WITH
-- Lấy các bảng CTE cần thiết
-- Lấy ra Doanh số
DAILYSALES_FULL AS (
SELECT [Agent Code],
Policy_Number,
Issuing_Agent,
[Contract Type],
Component_Code,
Proposal_Receive_Date,
Policy_Issue_Date,
Sum_Assured,
Before_Discount_Premium,
Discount_Premium,
After_Discount_Premium,
Policy_Status,
Bill_Frequency,
Modal_Factor,
Lapsed_date,
AFYP,
RISK_COMMENCE_DATE,
CUTOFF,
ACK_DATE
FROM [dbo].[T_DP_TDAILYSALES_DA_CUTOFF]
WHERE CUTOFF = FORMAT(EOMONTH(@varREPORT_MONTH), 'yyyyMMdd') --VAR
UNION ALL
SELECT [Agent Code],
Policy_Number,
Issuing_Agent,
[Contract Type],
Component_Code,
Proposal_Receive_Date,
Policy_Issue_Date,
Sum_Assured,
Before_Discount_Premium,
Discount_Premium,
After_Discount_Premium,
Policy_Status,
Bill_Frequency,
Modal_Factor,
Lapsed_date,
AFYP,
RISK_COMMENCE_DATE,
CUTOFF,
ACK_DATE FROM [dbo].[T_DP_DA_Daily_DC_PO_WD_NT_CUTOFF]
WHERE CUTOFF = FORMAT(EOMONTH(@varREPORT_MONTH), 'yyyyMMdd') --VAR
)
, AGENT_INFO_CUTOFF AS
(
SELECT * FROM [dbo].[T_Main_AGENT_INFO_DA_CUTOFF]
WHERE CUTOFF = FORMAT(EOMONTH(@varREPORT_MONTH), 'yyyyMMdd') -- VAR
)
, CUSTOMER_INFO_CO AS (
SELECT * FROM [dbo].[T_Main_CUSTOMER_INFO_CUTOFF]
WHERE CUTOFF = FORMAT(EOMONTH(@varREPORT_MONTH), 'yyyyMMdd') -- VAR
)
, PREMIUM_CUTOFF AS 
(
SELECT *
FROM [dbo].[T_DP_TAGENTPREMIUM_TEST_CUTOFF]
WHERE CUTOFF = FORMAT(EOMONTH(@varREPORT_MONTH), 'yyyyMMdd') -- VAR
)

-- Tính IP
-- Lấy ra các cột, lọc các dòng cần thiết
, A AS (SELECT [Agent Code],
[Policy_Number],
[Contract Type],
[Component_Code],
[Proposal_Receive_Date],
Policy_Issue_Date,
After_Discount_Premium,
IIF(Component_Code = 'UL81', After_Discount_Premium *0.1, After_Discount_Premium) AS [After_Discount_Premium_REVISED],
Policy_Status,
ACK_DATE
FROM DAILYSALES_FULL
WHERE Policy_Issue_Date BETWEEN @varREPORT_MONTH AND EOMONTH(@varREPORT_MONTH) --VAR
AND [Contract Type] NOT IN ('UL3','UL4')
AND Policy_Status ='IF')
-- SUM cột After_Discount_Premium
, A1 AS(
SELECT [Agent Code],
[Policy_Number],
[Contract Type],
[Component_Code],
[Proposal_Receive_Date],
Policy_Issue_Date,
SUM(After_Discount_Premium) AS IP_PREMIUM,
SUM(After_Discount_Premium_REVISED) AS IP_PREMIUM_COUNT_BONUS,
Policy_Status,
ACK_DATE
FROM A
-- WHERE Policy_Number IN (80144554 , 80144556) -- NHỚ CHỈNH LẠI
GROUP BY [Agent Code], [Policy_Number], [Contract Type], [Component_Code],
[Proposal_Receive_Date], Policy_Issue_Date,  Policy_Status, ACK_DATE)
-- Mốc lấy thông tin của đại lý từ bảng Agent_INFO
, A2 AS (
SELECT A22.Area_Name,
A22.Sales_Unit_Code,
A21.[Agent Code],
A22.Agent_Name,
A22.Grade,
A22.Appointed_TAPSU,
A22.SFC,
A22.ID_Card AS SA_ID_Card,
A21.Policy_Number,
A21.[Contract Type],
A21.Component_Code,
A21.Proposal_Receive_Date,
A21.Policy_Issue_Date,
A21.IP_PREMIUM,
A21.IP_PREMIUM_COUNT_BONUS,
A21.Policy_Status,
A21.ACK_DATE
FROM A1 AS A21 LEFT JOIN AGENT_INFO_CUTOFF AS A22
ON A21.[Agent Code] = A22.Agent_Number)

-- KIỂM TRA CROSS SALE
-- Mốc thông tin từ bảng Agent_Info_Cutoff
, B as (
SELECT Agent_Number,
Agent_Name,
ID_Card,
Area_Name as [CUS_Area_Name],
Appointed_TAPSU AS Appointed_Date,
Terminated_date as [CUS_Terminated_Date]
FROM AGENT_INFO_CUTOFF)

-- LẤY RA DANH SÁCH HỢP ĐỒNG CROSS SALES
, B2 AS (select
B11.*,
B12.ID_NUMBER AS CUS_ID,
CASE
	WHEN B12.ID_NUMBER IN (SELECT ID_Card FROM B) --Nếu CCCD KH nằm trong CCCD của Đại lý
	AND B11.[Agent Code] NOT LIKE '6999%'
	AND B11.Agent_Name NOT LIKE 'DUMMY%'
	AND B13.ID_Card NOT LIKE 'DUMMY%'
	and B13.Agent_Number NOT LIKE '6999%' --Loại các TH DUMMY
	AND B13.CUS_Area_Name <> 'SEP' -- Loại văn phòng SEP -> Service Executive
	AND B13.Appointed_Date <= B11.Proposal_Receive_Date -- Ngày gia nhập trước ngày ký HD
	AND B13.CUS_Terminated_Date IS NULL --Đại lý chưa bị TER
THEN 'CROSS_SALES'
ELSE 'NOT_CROSS_SALES'
END AS CHECK_CROSS_SALES
from A2 AS B11 LEFT JOIN CUSTOMER_INFO_CO AS B12
on B11.Policy_Number = B12.POLICY_CODE
LEFT JOIN B AS B13
ON B12.ID_NUMBER = B13.ID_Card)

SELECT *,
REPORTMONTH = FORMAT(CAST(@varREPORT_MONTH AS DATE), 'yyyyMM')
FROM B2
WHERE CHECK_CROSS_SALES = 'CROSS_SALES'
END
GO


