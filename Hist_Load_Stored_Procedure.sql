USE [Power]
GO
/****** Object:  StoredProcedure [dbo].[LAT_Load]    Script Date: 2021-09-20 5:26:04 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Bledsoe, Ian
-- Create date: 2019-06-17, Latest Update: 2020-12-28
-- Description:	Load & Temperature data for use in Load Forecast
-- =============================================
ALTER PROCEDURE [dbo].[LAT_Load]
	-- Add the parameters for the stored procedure here

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

DECLARE @meter_math_table AS TABLE(
              [Timestamp] smalldatetime
              ,ResidentialSales float
              ,Systemsales float
              ,TotalSales float
              ,Wauna float
              ,CPBR_Main_Plant float
              ,CPBR_VCU float
              ,CPBR_DDG float
              ,Biorefinery float
              ,Halsey float
              ,Loki float
              ,WaunaCogen float
              ,WaunaCoGen_SS float
              ,Camas float
              ,CamasCogen float
              ,Conyers float
              ,Delena float
              ,Rainier float
              ,WaunaRes float                       
              ,Load_hour char(3)
			  ,Driscoll float
			  ,Stimson float)
INSERT INTO @meter_math_table  /*Blank table filled in by @meter_math_table stored procedure*/
EXECUTE meter_math

/*
No longer care about peaks. Peak forecast are created using energy forecast
multiplied by historic load factors
*/
SELECT Cast(MonthDay as Date) as Dy
		,nonWaunaSystem
		--,nonWaunaSystemPeak
		,Wauna
		--,WaunaPeak
		,Biorefinery
		--,BiorefineryPeak
		,Halsey
		--,HalseyPeak
		,Camas
		--,CamasPeak
		,Load_hour
		,NumHrs
		
FROM
(
-- Get month Average Megawatts for each Load
select DATEADD(month, DATEDIFF(month, 0, [Timestamp]), 0) as [MonthDay]
		,Load_hour
		,sum(isnull(ResidentialSales,0))  / count(Load_hour) as nonWaunaSystem
		,sum(isnull(Systemsales,0)) / count(Load_hour)as SystemSales
		,sum(isnull(Wauna,0)) / count(Load_hour)as Wauna
		,sum(isnull(Biorefinery,0)) / count(Load_hour)as Biorefinery
		,sum(isnull(Halsey,0))/ count(Load_hour) as Halsey
		,sum(isnull(Camas,0))/ count(Load_hour) as Camas
		,max(isnull(ResidentialSales,0))  as nonWaunaSystemPeak
		,max(isnull(Systemsales,0)) as SystemSalesPeak
		,max(isnull(Wauna,0)) as WaunaPeak
		,max(isnull(Biorefinery,0)) as BiorefineryPeak
		,max(isnull(Halsey,0)) as HalseyPeak
		,max(isnull(Camas,0)) as CamasPeak
		,count(load_hour) as NumHrs

		
from @meter_math_table
GROUP BY DATEADD(month, DATEDIFF(month, 0, [Timestamp]), 0)
		,Load_hour
)ATABLE
-- Pre-2004 load data has too many data errors to be useful to build a forecast
WHERE MonthDay >= '2004-01-01'
ORDER BY MONTHDAY
		,Load_hour

-- EXECUTE [LAT_Load]
	
END
