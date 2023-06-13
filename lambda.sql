/*
###### Lamba checking #####
checking#####
# When lambda checks the status of tables in log table,
# it needs to execute sql queries,
# pack these frequently used queries into SP to reduce duplication in coding.
*/

DELIMITER &&  
DROP PROCEDURE IF EXISTS exampleschema.sp_lambda_loading_check_status;
CREATE PROCEDURE exampleschema.sp_lambda_loading_check_status (
	IN schema_name varchar (50),
	IN table_name varchar(50),
    In time_interval_in_seconds int,
    OUT result_for_sp_loading_check_status tinyint
    )    
BEGIN   
	-- 1. Declare variables to hold diagnostics area information
	DECLARE exit handler for SQLEXCEPTION
		BEGIN
			GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE, 
			@errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;
			SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);
			SELECT @full_error;
            set result_for_sp_loading_check_status=0;
            call exampleschema.sp_init_logtable_for_loading (schema_name, '99.lambda checking status before loading', table_name,0, 0,@full_error);
				
        END;

	-- 2. the body SP
    START TRANSACTION;
		set @sql= concat('
		select status into @status_for_sp_loading_check_status
        from (
			select 
				EventSource, 
				coalesce(Status,0) as status, 
				timediff(now(),Time_stamp) as time_interval 
			from `', schema_name, '`.`log_for_loading` 
			where locate(''permanent table loading from temp table '',EventSource)>0
				and  TIMESTAMPDIFF(SECOND, now(),Time_stamp)<', time_interval_in_seconds,
				' and status=1
			order by time_interval asc
			)as t 
			limit 1;');
			
    commit;
    IF @status_for_sp_loading_check_status =1 THEN
		/*
		-- means there was a successful loading not long ago
        */
		set result_for_sp_loading_check_status=0;
        /*
        -- means status not ok, there might be duplicate loading
        -- don't load and notify person in charge 
        -- (depends on how frequently your project updates data)
        */
	ELSE 
		set result_for_sp_loading_check_status=1;
	END IF; 
    /*
    select @result_for_sp_loading_check_status;
    */
END &&  
DELIMITER ; 
