






--Basic code for loading, populating and maintaining partition table  
CREATE PROCEDURE [dbo].[ResevoirPartitionSwitch]   
(  
  @TableName varchar(100)   --Name of table to be loaded  
 ,@PartitionCount smallint  --Number of partitions to keep  
 ,@Section varchar(50)   --Daily/Monthly/History  
 ,@StageIndex char(1)   --Y/N  
 ,@EmptyFile bit     --1 (allow empty file), 0 (disallow empty file)  
 ,@Result varchar(10) OUTPUT  --Success/Fail  
 ,@Reason varchar(300) OUTPUT --Result description  
 

)  WITH EXECUTE AS OWNER
AS  
BEGIN  
 BEGIN TRY  

  --Variable declarations  
  DECLARE @ObjCount AS smallint  
  DECLARE @RecCount AS int  
  DECLARE @sqlStr AS nvarchar(max)  
  DECLARE @sqlParamDef AS nvarchar(500)  
  DECLARE @StageDate AS datetime  
  declare @starttime datetime
  declare @endtime datetime
    
  SET @Result = 'Fail'  
  DECLARE @taskno smallint = 0
  --The following will be used to hold checkpoint durations
 
    
  --Check if the required objects exist  
  --Check for partition function  

 
select 
	@ObjCount = Count(1)
from sys.partition_functions pf 
where pf.name like '%' + @TableName  + '%'

  IF @ObjCount < 1  
  BEGIN  
   SET @Reason = 'Partition Function for table '+ @TableName + 'and section '+ @TableName + ' does not exist, please use <Create partition objects template.sql> as an example to create the object.'  
   RAISERROR(@Reason,16,1)  
  END   

  --Check for partition scheme  
 SELECT 
	@ObjCount = Count(1) 
  FROM sys.partition_schemes ps
  WHERE ps.name LIKE 'psc' + @TableName + '%'  

  IF @ObjCount < 1  
  BEGIN  
   SET @Reason = 'Partition Scheme for table '+ @TableName + 'and section '+ @TableName + ' does not exist, please use <Create partition objects template.sql> as an example to create the object.'  
   RAISERROR(@Reason,16,1)  
  END   

  --Check to ensure partition table and stage table exists  
  SELECT @ObjCount = Count(1) FROM sys.tables
  WHERE name LIKE '%' + @TableName + '%'  
  AND schema_id = schema_id(@Section)
    
  IF @ObjCount < 2
  BEGIN  
   SET @Reason = 'Partition and/or stage table '+@TableName+' does not exist for partition '+@Section+', please use <Create partition objects template.sql> as an example to create the object.'  
   RAISERROR(@Reason,16,1)  
  END

 

	
  --Check to ensure that there are data in the stage table,  
  --Make sure there is only a single day's data in the staging table  
  SET @sqlStr = 'SELECT @ObjCount = COUNT(DISTINCT CAST(RSVRBusinessdate AS date))'  
  SET @sqlStr = @sqlStr + ' FROM ' + @Section + '.' + @TableName + 'Stage'  
  SET @sqlParamDef = '@ObjCount smallint OUTPUT'  
  EXEC sp_executesql @sqlStr, @sqlParamDef, @ObjCount = @ObjCount OUTPUT  
   
 


  IF @ObjCount < 1 AND @EmptyFile = 0  
  BEGIN  
   SET @Reason = 'Stage table '+@Section+'.'+@TableName+'Stage is empty.'  
   RAISERROR(@Reason,16,1)   
  END  
  ELSE IF @ObjCount > 1  
  BEGIN  
   SET @Reason = 'Stage table '+@Section+'.'+@TableName+'Stage has more than one days data.'  
   RAISERROR(@Reason,16,1)   
  END   

 
  ---------------------Perform row level checksum
	set @sqlStr = 'Update ' + @Section + '.' + @TableName + 'Stage ' +char(13)+char(10) 
				+ 'set RSVRBinaryChecksum = binary_checksum(*)' 
	exec(@sqlStr)





 
 --Check to ensure that the staging index exists. If it does not exists, create it  If it exists, reindex, if option selected  
	SELECT @ObjCount = Count(1) FROM sys.indexes
	WHERE name = 'idx' + @TableName + 'StageDate' 
    
  IF (ISNULL(@ObjCount,0) = 0)  
  BEGIN  
   SET @sqlStr = 'CREATE NONCLUSTERED INDEX idx'+@TableName+'StageDate'+char(13)+char(10)  
   SET @sqlStr = @sqlStr+'ON '+@Section+'.'+@TableName+'Stage'+char(13)+char(10)  
   SET @sqlStr = @sqlStr+'(RSVRBusinessDate)'+char(13)+char(10)  
   SET @sqlStr = @sqlStr+'WITH (DATA_COMPRESSION = PAGE)'+char(13)+char(10)  
   SET @sqlStr = @sqlStr+'ON [PRIMARY]'  
     
   EXEC sp_executesql @sqlStr  
  

  END  
  --Gopolang: Why are we rebuilding an index? It won't help as we are never using the index at all to query a staging table. If it exists then we should leave it
  ELSE IF @StageIndex = 'Y'  
  BEGIN  
   SET @sqlStr = 'ALTER INDEX idx'+@TableName+'StageDate'+char(13)+char(10)  
   SET @sqlStr = @sqlStr+'ON '+@Section+'.'+@TableName+'Stage'+char(13)+char(10)  
   SET @sqlStr = @sqlStr+'REBUILD WITH (DATA_COMPRESSION = PAGE)'  
  
   EXEC sp_executesql @sqlStr  
  END  
    
  --Check to ensure that the partition table index exists  
	SELECT @ObjCount = Count(1) FROM sys.indexes
	WHERE name = 'idx' + @TableName +  'Date'
    
  IF (ISNULL(@ObjCount,0) = 0)  
  BEGIN  
   SET @Reason = 'idx'+@TableName+'Date index does not exist.'  
   RAISERROR(@Reason,16,1)   
  END  
    
  --Expire records if @EmptyFile parameter is set to 1 - Manual data  
  --The comment above is incorrect as this does not use @EmptyFile to expire records  
 
  SET @sqlStr = 'UPDATE '+@Section+'.'+@TableName+char(13)+char(10)  
  SET @sqlStr = @sqlStr + ' SET RSVRStatusFlag = 0 WHERE RSVRStatusFlag = 1'   
  EXEC sp_executesql @sqlStr  
    
	
  --Check if the partition for the current stage data exists  
  --If it does, switch it out, before loading again  

  
   --Check No of records in stage table, set @RecCount  
  SET @sqlStr = 'SELECT @RecCount = COUNT(1)'  
  SET @sqlStr = @sqlStr + ' FROM ' + @Section + '.' + @TableName + 'Stage'  
  SET @sqlParamDef = '@RecCount int OUTPUT'  
  EXEC sp_executesql @sqlStr, @sqlParamDef, @RecCount = @RecCount OUTPUT  

  
  IF (ISNULL(@RecCount,0) > 0)  
  BEGIN  
   --Get the date from the stage table  
   SET @sqlStr = 'SET @StageDate = (SELECT top 1 RSVRBusinessDate'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'FROM '+@Section+'.'+@TableName+'Stage)'  
   SET @sqlParamDef = '@StageDate datetime OUTPUT'  
     
   EXEC sp_executesql @sqlStr, @sqlParamDef, @StageDate = @StageDate OUTPUT 
  
   --Now check if a partition for the date exists on the partition table  
   SELECT @ObjCount = count(1) 
   FROM sys.partition_range_values prv
   INNER JOIN sys.partition_functions pf ON pf.function_id = prv.function_id AND pf.name = 'pfn'+@TableName+'DateRight'
   WHERE CAST(prv.value AS varchar) = cast(@StageDate as varchar)  

  END  

 



  IF (ISNULL(@ObjCount,0) > 0 AND ISNULL(@RecCount,0) > 0)  
  --The partition exists  
  BEGIN  
   --Create a temp table to switch out the existing data (drop and re-create)  
   --First drop the table if it exists  
  
  SET @sqlStr = 'DROP TABLE IF EXISTS '+@Section+'.'+@TableName+'Stagetmp'  
  EXEC(@sqlStr)


   --Get the number of columns in the partition table to build the temp table to switch out  
   SELECT @ObjCount = MAX(col.column_id)
	FROM sys.columns col
	INNER JOIN sys.tables tab ON (tab.object_id = col.object_id AND tab.name = @TableName AND tab.schema_id = schema_id(@Section))
	INNER JOIN sys.types typ ON (typ.system_type_id = col.system_type_id)
  
   --Now Build the create statement  
   --Create the temp table  
   DECLARE @LoopCount smallint  
   DECLARE @FieldAdd nvarchar(1000)  
   DECLARE @FieldAddPrev nvarchar(1000)  
   DECLARE @sqlStr2 nvarchar(max)  
   DECLARE @sqlParamDef2 AS nvarchar(500)  
   DECLARE @sqlStrBegin nvarchar(1000)  
     
   SET @sqlStr = 'CREATE TABLE '+@Section+'.'+@TableName+'Stagetmp'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + '('+char(13)+char(10)  
   SET @FieldAddPrev = ''  
   SET @FieldAdd = ''  
   SET @sqlStrBegin = @sqlStr  
     
   --Loop through to create field definitions  
   SET @LoopCount = 1  
   WHILE (@LoopCount <= @ObjCount)  
   BEGIN  
		SET @FieldAddPrev = @FieldAdd  
		SELECT @FieldAdd = 
		((CASE (LEFT(RIGHT(@sqlStrBegin,3),1)) WHEN '(' THEN '' ELSE ',' END)
		+ '  [' + col.name + ']'  
		+(CASE WHEN col.is_computed = 0 THEN typ.name
		+(CASE typ.name  WHEN 'varchar' THEN '('+CASE col.max_length WHEN -1 THEN 'MAX' ELSE CAST(col.max_length as varchar) END +') '
						WHEN 'nvarchar' THEN '('+CASE col.max_length WHEN -1 THEN 'MAX' ELSE CAST((col.max_length/2) as varchar) END +') '
						WHEN 'char' THEN '(' +CAST(col.max_length as varchar)+') '
						WHEN 'numeric' THEN '('+CAST(col.precision as varchar)+','+CAST(col.scale AS varchar)+') '
						WHEN 'decimal' THEN '('+CAST(col.precision as varchar)+','+CAST(col.scale AS varchar)+') '
						ELSE ' '
			END)
		+(CASE col.is_nullable WHEN 1 THEN 'NULL' ELSE 'NOT NULL' END) ELSE 'AS '+com.definition+'' END)
		)
		FROM sys.columns col
		INNER JOIN sys.tables tab ON (tab.object_id = col.object_id AND tab.name = @TableName AND tab.schema_id = schema_id(@Section))
		INNER JOIN sys.types typ ON (typ.user_type_id = col.user_type_id)
		LEFT OUTER JOIN sys.computed_columns com ON (com.object_id = col.object_id) AND (com.column_id = col.column_id)
		WHERE col.column_id = CAST(@LoopCount as varchar)
    
    --Quite possible that some of the fields have been removed  
		IF NOT (@FieldAdd = @FieldAddPrev)  
			SET @sqlStr = @sqlStr + @FieldAdd  
		IF (LEN(@sqlStrBegin) < 500)   
			SET @sqlStrBegin = @sqlStr  
          
		SET @LoopCount = @LoopCount + 1  
   END  
   
   SET @sqlStr = @sqlStr + ') ON [PRIMARY]'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'WITH (DATA_COMPRESSION = PAGE)'+char(13)+char(10)  
     
   EXEC sp_executesql @sqlStr  
   --Switch out the old data  

   

   SET @sqlStr = 'ALTER TABLE '+@Section+'.'+@TableName+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'SWITCH PARTITION $PARTITION.pfn'+@TableName+'DateRight'  
   SET @sqlStr = @sqlStr + '('''+CAST(@StageDate AS varchar)+''')'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + ' TO '+@Section+'.'+@TableName+'Stagetmp'  
  
   EXEC sp_executesql @sqlStr
     
  
   --Drop the temp table  
   SET @sqlStr = 'DROP TABLE '+@Section+'.'+@TableName+'Stagetmp'  
   EXEC sp_executesql @sqlStr   
   
  
     
  END  
  ELSE IF (ISNULL(@RecCount,0) > 0)  
  --The partition does not exists  
  BEGIN  
	
	--Have to do this. Even though you using only a single filegroup....  
	SET @sqlStr = 'ALTER PARTITION SCHEME psc'+@TableName+'DateRight'+char(13)+char(10)  
	SET @sqlStr = @sqlStr + 'NEXT USED [PRIMARY]'  
  
	EXEC sp_executesql @sqlStr  
     
	--create the new partition  
	SET @sqlStr = 'ALTER PARTITION FUNCTION pfn'+@TableName+'DateRight()'+char(13)+char(10)  
	SET @sqlStr = @sqlStr + 'SPLIT RANGE ('''+CAST(@StageDate AS varchar)+''')'  
  
	EXEC sp_executesql @sqlStr  

	
  END  
    

  IF (ISNULL(@RecCount,0) > 0)  
  BEGIN  
	
   --Switch in the new data  
   --Check and/or create constraint on stage table  
    SELECT @ObjCount = COUNT(1) FROM sys.check_constraints
   WHERE name = 'con'+@TableName+'Stage'  
     
   IF (ISNULL(@ObjCount,0) <> 0)  
   --The constraint does not exist  
   BEGIN  
    --drop the constraint  
    --Have to do this to switch out data.    
    SET @sqlStr = 'ALTER TABLE '+@Section+'.'+@TableName+'Stage'+char(13)+char(10)  
    SET @sqlStr = @sqlStr + 'DROP CONSTRAINT  con'+@TableName+'Stage'  
  
    EXEC sp_executesql @sqlStr  
  
   END  
     
   --Add the constraint on the stage table  
   SET @sqlStr = 'ALTER TABLE '+@Section+'.'+@TableName+'Stage'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'ADD CONSTRAINT  con'+@TableName+'Stage'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'CHECK (RSVRBusinessdate IS NOT NULL AND RSVRBusinessdate = '''+CAST(@StageDate AS varchar)+''')'  
    
   EXEC sp_executesql @sqlStr  
     
   
   --switch in the new stage data with the empty partition  
   SET @sqlStr = 'ALTER TABLE '+@Section+'.'+@TableName+'Stage'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'SWITCH TO '+@Section+'.'+@TableName  
   SET @sqlStr = @sqlStr + ' PARTITION $PARTITION.pfn'+@TableName+'DateRight'  
   SET @sqlStr = @sqlStr + '('''+CAST(@StageDate AS varchar)+''')'+char(13)+char(10)  
  
   EXEC sp_executesql @sqlStr  
   
  

   
   --drop the constraint  
   --Have to do this to switch out data.    
   SET @sqlStr = 'ALTER TABLE '+@Section+'.'+@TableName+'Stage'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'DROP CONSTRAINT  con'+@TableName+'Stage'  
  
   EXEC sp_executesql @sqlStr  
  END  
     
  --Check if the oldest partition should be switched out  
  --Get number of active partitions  
  SET @sqlStr = 'SELECT @ObjCount = COUNT(DISTINCT RSVRBusinessDate)'+char(13)+char(10)  
  SET @sqlStr = @sqlStr + 'FROM '+@Section+'.'+@TableName  
  SET @sqlParamDef = '@ObjCount smallint OUTPUT'  
    
  EXEC sp_executesql @sqlStr, @sqlParamDef, @ObjCount = @ObjCount OUTPUT  
    
  --Partion count may not be less than 1  
  IF (@PartitionCount < 1)  
   SET @PartitionCount = 1  
     
  --Need another date, want to switch out put not what you just put in if it is the min  
  DECLARE @CurrentSwitchDate as datetime  
    
  --Set the current switch date equal to Stage Date  
  SET @CurrentSwitchDate = ISNULL(@StageDate, CAST('9999-01-01' AS datetime))  
  --Loop through parition until you have the max, or less than max active partitions  
  WHILE (@ObjCount > @PartitionCount)  
  BEGIN  
	
   --if it does switch it out  
   SET @sqlStr = 'SET @StageDate = (SELECT MIN(RSVRBusinessDate)'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'FROM '+@Section+'.'+@TableName+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'WHERE RSVRBusinessDate <> '''+CAST(@CurrentSwitchDate AS varchar)+''')'  
   SET @sqlParamDef = '@StageDate datetime OUTPUT'  
    
   EXEC sp_executesql @sqlStr, @sqlParamDef, @StageDate = @StageDate OUTPUT  
     
   SET @sqlStr = 'ALTER TABLE '+@Section+'.'+@TableName+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'SWITCH PARTITION $PARTITION.pfn'+@TableName+'DateRight'  
   SET @sqlStr = @sqlStr + '('''+CAST(@StageDate AS varchar)+''')'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + ' TO '+@Section+'.'+@TableName+'Stage'  
  
   EXEC sp_executesql @sqlStr  
  
   --Merge the partition in to remove empty partition  
   SET @sqlStr = 'ALTER PARTITION FUNCTION pfn'+@TableName+'DateRight()'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'MERGE RANGE ('''+CAST(@StageDate AS varchar)+''')'  
  
   EXEC sp_executesql @sqlStr  
        
   --Truncate the stage table    
   SET @sqlStr = 'TRUNCATE TABLE '+@Section+'.'+@TableName+'Stage'  
  
   EXEC sp_executesql @sqlStr  
  
   --Get no of active partitions  
   SET @sqlStr = 'SELECT @ObjCount = COUNT(DISTINCT RSVRBusinessDate)'+char(13)+char(10)  
   SET @sqlStr = @sqlStr + 'FROM '+@Section+'.'+@TableName  
   SET @sqlParamDef = '@ObjCount smallint OUTPUT'  
     
   EXEC sp_executesql @sqlStr, @sqlParamDef, @ObjCount = @ObjCount OUTPUT  
    
  END  
    
  SET @Result = 'Success'  
  SET @Reason = 'Table '+@TableName+' for '+@Section+' section successfully loaded.'  
 END TRY  
 BEGIN CATCH  
  SET @Result = 'Fail'  
  SET @Reason = ERROR_MESSAGE()  
   
 END CATCH  

 
  

END  
  

GO
GRANT EXECUTE
    ON OBJECT::[dbo].[ResevoirPartitionSwitch] TO dbo
    AS [dbo];

