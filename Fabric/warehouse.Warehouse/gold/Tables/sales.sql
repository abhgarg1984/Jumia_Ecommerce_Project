CREATE TABLE [gold].[sales] (

	[Order_ID] varchar(8000) NULL, 
	[Order_Date] datetime2(6) NULL, 
	[Shipping_Date] datetime2(6) NULL, 
	[Aging] bigint NULL, 
	[Ship_Mode] varchar(8000) NULL, 
	[Product] varchar(8000) NULL, 
	[Product_Category] varchar(8000) NULL, 
	[Sales] bigint NULL, 
	[Quantity] bigint NULL, 
	[Discount] float NULL, 
	[Profit] float NULL, 
	[Shipping_Cost] float NULL, 
	[Order_Priority] varchar(8000) NULL, 
	[Customer_ID] varchar(8000) NULL, 
	[LoadTime] datetime2(6) NULL, 
	[FileDate] date NULL, 
	[Return] varchar(8000) NULL
);