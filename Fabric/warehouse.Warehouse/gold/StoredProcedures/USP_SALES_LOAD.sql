--drop PROC [gold].[USP_SALES_LOAD] 

CREATE   PROC [gold].[USP_SALES_LOAD] (@filedate varchar(10))
AS
BEGIN

DECLARE @file_date_type date;

select @file_date_type = 
CAST(
        SUBSTRING(@filedate, 5, 4) + '-' + -- Year (2023)
        SUBSTRING(@filedate, 3, 2) + '-' + -- Month (01)
        SUBSTRING(@filedate, 1, 2)         -- Day (01)
    AS DATE)

delete from gold.sales where FileDate = @file_date_type;

Insert INTO gold.sales (Order_ID,Order_Date,Shipping_Date,Aging,Ship_Mode,Product,Product_Category,Sales,Quantity,Discount,Profit,Shipping_Cost,Order_Priority,Customer_ID,LoadTime,FileDate,"Return")
select
s.Order_ID,s.Order_Date,s.Shipping_Date,s.Aging,s.Ship_Mode,s.Product,s.Product_Category,s.Sales,s.Quantity,s.Discount,s.Profit,s.Shipping_Cost,s.Order_Priority,s.Customer_ID,s.LoadTime, s.FileDate,r."Return"
from lakehouse.dbo.bronze_sales s 
left join lakehouse.dbo.bronze_returns r on s.FileDate = r.FileDate and s.Order_ID = r.Order_ID
where s.FileDate = @file_date_type;






execute gold.USP_DIM_LOAD @file_date_type ;

END;