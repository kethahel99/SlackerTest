CREATE TABLE [dbo].[DevOpsDemo]
(
	[Id] INT NOT NULL ,
	FirstName varchar(50) NOT NULL,
	LastName varchar(50) NOT NULL,
	Email varchar(50) NOT NULL, 
    [Flag] NCHAR(1) NULL, 
    [Test] NCHAR(10) NULL
)

GO
CREATE CLUSTERED COLUMNSTORE INDEX [CStoreIX_DevOpsDemo] ON [dbo].[DevOpsDemo] WITH (DATA_COMPRESSION = COLUMNSTORE)
