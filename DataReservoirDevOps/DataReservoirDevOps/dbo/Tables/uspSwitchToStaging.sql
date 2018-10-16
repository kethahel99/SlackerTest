


CREATE procedure [dbo].[uspSwitchToStaging] 

@SchemaName varchar(100), 
@Tablename varchar(100),
@FileBusinessDate datetime
as
begin

	declare  @sqlParamDef nvarchar(100), @RSVRBusinessDate datetime
	declare @sql nvarchar(max) = ''
	 
	set @sql = 'select top 1 @RSVRBusinessDate = RSVRBusinessDate from ' + @SchemaName + '.' + @TableName
			+ ' where RSVRStatusFlag = 1'
	
	SET @sqlParamDef = '@RSVRBusinessDate DateTime OUTPUT' 
    EXEC sp_executesql @sql, @sqlParamDef, @RSVRBusinessDate = @RSVRBusinessDate OUTPUT
     
	 if @FileBusinessDate = @RSVRBusinessDate
	 begin

		SET @sql = 'ALTER TABLE '+@SchemaName+'.'+@TableName+char(13)+char(10)  
		SET @sql = @sql + 'SWITCH PARTITION $PARTITION.pfn'+@TableName+'DateRight'  
		SET @sql = @sql + '('''+CAST(@RSVRBusinessDate AS varchar)+''')'+char(13)+char(10)  
		SET @sql = @sql + ' TO '+@SchemaName+'.'+@TableName+'Stage'
    
		EXEC sp_executesql @sql
	end
end

GO
GRANT EXECUTE
    ON OBJECT::[dbo].[uspSwitchToStaging] TO dbo
    AS [dbo];

