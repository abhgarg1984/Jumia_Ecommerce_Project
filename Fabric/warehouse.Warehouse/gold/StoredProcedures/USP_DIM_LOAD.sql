CREATE     PROC [gold].[USP_DIM_LOAD] (@filedate date)
AS
BEGIN

delete from gold.customer;

Insert INTO gold.customer  
select * from lakehouse.dbo.dim_customer ;


MERGE INTO gold.Dates tgt 
USING (select x.Date,DATEPART(YEAR,x.Date) YEAR, DATEPART(MONTH,x.Date) MONTH, datepart(QUARTER,x.Date) QUARTER, datepart(DAY,x.Date) DAY
 from 
(
select distinct try_convert(date, Order_Date) as Date
from lakehouse.dbo.bronze_sales where FileDate = @filedate
UNION 
select distinct try_convert(date, Shipping_Date) as Date
from lakehouse.dbo.bronze_sales where FileDate = @filedate
UNION 
select distinct try_convert(date, LoadTime) as Date
from lakehouse.dbo.bronze_sales where FileDate = @filedate
UNION 
select distinct try_convert(date, FileDate) as Date
from lakehouse.dbo.bronze_sales where FileDate = @filedate
) x) src on src.Date = tgt.Date
WHEN NOT MATCHED THEN INSERT (Date, YEAR, MONTH, QUARTER, DAY)
VALUES 
(
src.Date, src.YEAR, src.MONTH, src.QUARTER, src.DAY
);


UPDATE warehouse.gold.watermark
set LoadTime = (select MAX(LoadTime) from lakehouse.dbo.bronze_sales where FileDate = @filedate)
;


END;