#### Data Loading ####
#### This file builds procedures that can be used to load and split data.####
#### The data is can be used for reporting in the following file of "reporting.sql".####
-- ----------------------------------------------------------
-- 0. create stored procedure (sp) for logging
	-- this part is included in the file of 'init.sql'
-- 1. create stored procedure (sp) for load data from s3(or other datasource) to a temp table 
	-- 1.1 create temp table 
    -- 1.2 count in temp table
    -- 1.3 load from data file to temp table
-- 2. create stored procedure (sp) for upsert data from temp table to the permanent table (general table in this project)
-- 3. create stored procedure (sp) for upsert data from permanent table to sub tables based on their category
	-- 3.1 update general table with status column according to categories
    -- 3.2 load data into sub tables according to status
-- 4. create stored procedure (sp) to Check if there are data not split into their category table yet
	-- return the check result as OUT parameter 
-- 5. creare a parent SP to include all the above child SPs
-- 6. just call the parent SP, all child SPs will be called automatically
	-- data will be loaded, log will be written if error
-- ----------------------------------------------------------

-- call webvoir.sp_loading_PriceIndex("webvoir","temp","0.PriceIndex", "priceindex.csv", " ", 18516,5, @result)
-- select @result
-- 1. to load data from datasource (s3) into general table in mysql
DROP PROCEDURE IF EXISTS webvoir.sp_loading_PriceIndex;
DELIMITER &&  
CREATE PROCEDURE webvoir.sp_loading_PriceIndex (
	IN schema_name varchar (50),
    IN temp_tablename varchar(50),
	IN table_name varchar(50),
    IN file_name varchar (50),
    IN copy_command varchar (800),
    IN total_rows bigint, maxerrors_allowed tinyint,
    OUT result tinyint
    )    
BEGIN   
	-- 1. to create temp table 
    call webvoir.sp_loading_create_temp_table (schema_name, temp_tablename, table_name);
    -- 2. to copy data from s3 to temp table in mysql
    -- the command is like : 
    -- LOAD DATA FROM S3 “s3://sample_bucket/sample_file.csv” 
    -- INTO TABLE dbname.dbtable(field1, field2, ...);
    -- this command string will be done by lambda
    set @sql= copy_command;
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
  
    -- 3. to count how many rows were loaded into temp table
    call webvoir.sp_loading_count_table(schema_name, temp_tablename, @result_rows,"");


    -- 4. to compare the numbers 
    IF @result_rows >= (total_rows -maxerrors_allowed) THEN
		-- to try loading
		call webvoir.sp_loading_upsert_general_table (schema_name, temp_tablename, table_name);
		-- to write log for successful loading
        call webvoir.sp_init_logtable_for_loading (schema_name, file_name, table_name,1, @result_rows,concat((total_rows - @result_rows),"loading to general table successfully"));
		
        -- after loading to general table successfully,
        -- start loading for sub tables
		-- 5. to decide if split general table into sub tables
		-- for some reports, sub tables make report building easier
		IF locate ('priceindex',table_name)>0 THEN
			-- update general table with status and load data to sub tables
			call webvoir.sp_loading_priceindex_split (schema_name, table_name);
            -- check if the loading is complete
            
			call webvoir.sp_loading_check_remaining_after_split (schema_name, table_name, @check_remain);
            
			IF @check_remain =0 THEN
				-- complete loading
                -- write log
				call webvoir.sp_init_logtable_for_loading (schema_name, file_name, table_name,1, @result_rows,concat((total_rows - @result_rows),"loading to general table successfully")); 
                set result =1;
			ELSE 
				set result =0;
			END IF;
		-- ELSE
			-- loading process for other data files other than priceindex
		END IF;
    ELSE 
		-- don't load data from temp_table to permanent table
        -- just to write log for unsuccessful loading
        call webvoir.sp_init_logtable_for_loading (schema_name, file_name, table_name,0, 0,'failed to load all data from the file');
        set result =0;
	END IF;
    
    -- use session variable to carry the value
    select @result;

END &&  
DELIMITER ;

-- ----------------------------------------------------------------
-- in order to let the above sp work, below child SPs are created:
-- 1.1 to create sp to build temp table
-- the tempt table is used to store data from .csv file in s3
-- the columns and their order in mysql table must be the same as .csv file 
-- the CREATE TABLE statement can be generated dynamically by lambda or EC2 or ECS
-- or can be pre-defined here in sp, of course, when original file changes
-- the sp here needs changing accordingly.
-- to reduce the management overhead, I use lamda to do some of the ETL
-- make the original data file in s3 only contains the columns we need for the report
-- these columns donot change frequently, if there is any change in the data source,
-- lambda will detect and make sure the .csv file is still loadable for mysql
DROP PROCEDURE IF EXISTS webvoir.sp_loading_create_temp_table;
DELIMITER &&  
CREATE PROCEDURE webvoir.sp_loading_create_temp_table (
	IN schema_name varchar (50),
    IN temp_tablename varchar(50),
	IN table_name varchar(50)
    )    
BEGIN   
	set @sql =concat('
    DROP temporary TABLE IF EXISTS `',schema_name, '`.`', temp_tablename, '` ;'
    );
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;

	-- to copy the table structure from permanent table
    -- temporary
	set @sql =concat ('
    CREATE temporary TABLE  `',schema_name, '`.`', temp_tablename, '` LIKE  `',schema_name, '`.`', table_name, '`;'
                                    
    );
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;

END &&  
DELIMITER ;   

-- call sp_loading_create_temp_table ("webvoir", "temp","0.priceindex");
-- -------------------------------------------------------------------------
-- 1.2 to count rows in temptable
-- if the number is the same as lambda counts in the .csv file in s3
-- means all rows were loaded to mysql from s3
-- Note :
-- In order to make this sp can't be used by all other SPs in this project
-- a parameter of 'WHRE clause' can be added, so that the sp can count the rows on some conditions
-- later , when we check the remaining rows in general table after splitting, 
-- use "where status is null" to get the row number

DROP PROCEDURE IF EXISTS webvoir.sp_loading_count_table;
DELIMITER &&  
CREATE PROCEDURE webvoir.sp_loading_count_table (
	IN schema_name varchar(50),
	IN table_name varchar(50),
    OUT total bigint,
    IN where_clause varchar (255)
    )    
BEGIN    
	-- in order to get a value from sp
    -- we need a 'OUT' parameter
    -- the steps are :
    -- a) to reset @aaaa before using
    -- b) to get the count value into a variable @aaaa
    -- c) to set the out parameter equals @aaaa
    -- d) to select the session variable @aaaa in the end of procedure
    
    IF where_clause is null THEN
		set @sql = concat ('
		select COUNT(*) into @row_no from `',schema_name, '`.`', table_name, '`;'     
		);
	ELSE 
		set @sql = concat ('
		select COUNT(*) into @row_no from `',schema_name, '`.`', table_name, '` ',
         where_clause, ' ;'     
		);
    END IF;
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    
    set total = @row_no;
    
	select @total;
END &&  
DELIMITER ;   

-- to get the out paramter: 
-- call sp_loading_count_table ("webvoir", "temp",@result_row);
-- select @result_row;
-- -------------------------------------------------------------------------
-- 2. to create SP for upsert from template to permanent table (general table in this project)
-- If there is PK or unique index in the table, any duplicate loading would cause error of 'key violation'
-- in order to update only new records into permanent table and avoid error of key violation,
-- UPSERT is applied here
-- there are 3 ways to upsert, i chose method of 'REPLACE' for this project
-- because one report from dashboard is based on this table, we have to make data in this table complete, up to date, and no duplicates
DROP PROCEDURE IF EXISTS webvoir.sp_loading_upsert_general_table;
DELIMITER &&  
CREATE PROCEDURE webvoir.sp_loading_upsert_general_table (
	IN schema_name varchar(50),
	IN temp_tablename varchar(50),
	IN table_name varchar(50)
    )    
BEGIN  
	-- make sure all status is null in the temporary table
    -- later the status column will be updated according to sub category
    set @sql=concat ('
	UPDATE  `', temp_tablename, '`',
		' set status = null;');
    SET SQL_SAFE_UPDATES = 0;    
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    SET SQL_SAFE_UPDATES = 1;
    
	-- load for 0.PriceIndex
    set @sql=concat ('
	REPLACE INTO  `', schema_name, '`.`', table_name, '` ',
		'select 
			*
		from `', temp_tablename,'`');
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
END &&  
DELIMITER ; 
-- -------------------------------------------------------------------------
-- call webvoir.sp_loading_upsert_general_table ("webvoir", "temp", "0.priceindex")
-- call webvoir.sp_loading_priceindex_split ("webvoir", "0.priceindex");
-- 3. to create SP for loading --> Food, Energy, Cosmetics, Tobacco
DROP PROCEDURE IF EXISTS webvoir.sp_loading_priceindex_split;
DELIMITER &&  
CREATE PROCEDURE webvoir.sp_loading_priceindex_split (
	IN schema_name varchar(50),
	IN table_name varchar(50)
    )    
BEGIN    
	-- the original data files contains various products 
    -- the data will be split into sub tables according to their categories
    -- @tablename=webvoir.`0.PriceIndex`;
    
    -- use comma number to get how many kinds of product there are.
	set @food_list ="'steak', 'roast', 'beef', 'chicken', 'pork', 'bacon', 'wiener', 'salmon','milk','butter','cheese','egg','bread', 'cracker','macaroni','flour','corn','apple','banana','grape','orange','juice','cabbage','carrot','celery','mushroom','onion','potato','fried','bake','bean','canned','ketchup','sugar','coffee','tea','cooking','salad','oil','soup','food','peanut','fruit','drink','cola','lemon'";
    
    set @cosmetics_list="'laundry','detergent','facial','towel','tissue','bathroom','shampoo','deodorant','toothpaste'";
    
    set @energy_list="'gasoline'";
    
    set @tobacco_list="'cigarette'";

	SET @sql=concat('update
		`', schema_name, '`.`',table_name ,
		'` set STATUS =1
		WHERE products REGEXP "steak|roast|beef|chicken|pork|bacon|wiener|salmon|milk|butter|cheese|egg|bread|cracker|macaroni|flour|corn|apple|banana|grape|orange|juice|cabbage|carrot|celery|mushroom|onion|potato|fried|bake|bean|canned|ketchup|sugar|coffee|tea|cooking|salad|oil|soup|food|peanut|fruit|drink|cola|lemon"
         and STATUS is null;'
        );
    SET SQL_SAFE_UPDATES = 0;
	 PREPARE dynamic_statement FROM @sql;
	 EXECUTE dynamic_statement;
	 DEALLOCATE PREPARE dynamic_statement;
	SET SQL_SAFE_UPDATES = 1; 

    -- after we update STATUS for all food products,
    -- now to update for the second category: cosmetics

	SET @sql=concat('update
		`', schema_name, '`.`',table_name ,
		'` set STATUS =2
		WHERE products REGEXP "laundry|detergent|facial|towel|tissue|bathroom|shampoo|deodorant|toothpaste"
		 and STATUS is null;'
        );
		-- update save mode will stop below from running
	SET SQL_SAFE_UPDATES = 0;
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
	SET SQL_SAFE_UPDATES = 1;

    -- next is to udpate according to the third category
 
	SET @sql=concat('
		update `', schema_name, '`.`',table_name ,
			'` set STATUS=3 
		where products REGEXP "gasoline"
			and STATUS is null;' );
	SET SQL_SAFE_UPDATES = 0;
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
	SET SQL_SAFE_UPDATES = 1;

	-- next is to udpate according to the fourth category
	SET @sql=concat('
		update `', schema_name, '`.`',table_name ,
			'` set STATUS=4 
		where products REGEXP "cigarette"
			and STATUS is null;' );
			
	SET SQL_SAFE_UPDATES = 0;
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
	SET SQL_SAFE_UPDATES = 1;

    -- after we got original data file updated
    -- to start loading for 1st category
    -- in this project, the data will be continuously got from online
    -- the new data might be duplicate with existing data in database
    -- therefore , a UPSERT will be applied here. 
	-- UPSERT CAN BE ACHIEVED IN 3 WAYS:
    -- https://www.javatpoint.com/mysql-upsert#:~:text=UPSERT%20is%20one%20of%20the,words%20named%20UPDATE%20and%20INSERT.
    -- for TESTING purpose, all these 3 methods will be used
    -- 1. INSERT IGNORE INTO...
    
	-- load for food 
		set @sql=concat ('
		insert IGNORE into `', schema_name, '`.`1.Food`  (GEO,Date, Year, Month, Products, Measurement,`Products Details`, Price, Status)
			select 
				GEO,
				Date, 
				LEFT(date,4) as Year, 
				RIGHT(date,2) as Month,
				SUBSTRING_INDEX(Products,",",(LENGTH(Products)-LENGTH(REPLACE(Products,",","")))) as Products,
                substring_index(Products,",",-1) as Measurement,
				Products as `Products Details`,
				Value as Price,
				STATUS
			from `', schema_name , '`.`', table_name ,
			'` where status =1;');
	
	SET SQL_SAFE_UPDATES = 0;
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    SET SQL_SAFE_UPDATES = 1;    
	-- 2. UPSERT using Replace:
    -- load for cosmetics
    set @sql=concat('
	REPLACE INTO `', schema_name , '`.`2.Cosmetics` (GEO,Date, Year, Month, Products,Measurement, `Products Details`, Price, Status)
		select 
			GEO,
			Date, 
            LEFT(date,4) as Year, 
            RIGHT(date,2) as Month,
            SUBSTRING_INDEX(Products,",",(LENGTH(Products)-LENGTH(REPLACE(Products,",","")))) as Products,
            substring_index(Products ,",",-1) as Measurement,
            Products as `Products Details`,
            Value as Price,
            Status
		from `', schema_name , '`.`', table_name ,
		'` where status =2;');
	SET SQL_SAFE_UPDATES = 0;
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    SET SQL_SAFE_UPDATES = 1;    
	-- 3. UPSERT with ON DUPLICATE KEY UPDATE clause:
    -- load for energy:
    set @sql=concat('
	INSERT INTO `', schema_name , '`.`3.Energy` (GEO,Date, Year, Month, Products, Measurement, `Products Details`, Price, Status)
		select 
			GEO,
			Date, 
            LEFT(date,4) as Year, 
            RIGHT(date,2) as Month,
            SUBSTRING_INDEX(Products,",",(LENGTH(Products)-LENGTH(REPLACE(Products,",","")))) as Products,
            substring_index(Products,",",-1) as Measurement,
            Products as `Products Details`,
            Value as Price,
            Status
		from `', schema_name , '`.`', table_name ,
		'` where status =3
	ON DUPLICATE KEY UPDATE Status=0;');
    SET SQL_SAFE_UPDATES = 0;
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
	SET SQL_SAFE_UPDATES = 1;
    # 1 update using insert ignore into, a duplicate record is 
    # just ignored and won't be loaded at all
    # 2 update using Replace Into, the existing record is just 
    # deleted and new record overrides
    #3 update using ON Duplicate key update clause, its a little bit flexible 
    # for data loading. once duplicate, the whole row of data won't be deleted,
    # only specific values are to be updated according to the clause. 
    # But if there are more than one unique index in the table,
    # and multiple rows are found duplicate by one new row of data, only one of the existing row is updated according to 
    # the ON Duplicate key update clause. 
  
	# for this project, a overwrite (REPLACE INTO) would be better. The original data file has no pk
    # so that all records will be kept safe though duplicate may exist.
    
    -- load for tobacco
    -- use REPLACE INTO
	set @sql=concat('
	REPLACE INTO  `', schema_name , '`.`4.Tobacco` (GEO,Date, Year, Month, Products, Measurement,`Products Details`, Price, Status)
		select 
			GEO,
			Date, 
            LEFT(date,4) as Year, 
            RIGHT(date,2) as Month,
            SUBSTRING_INDEX(Products,",",(LENGTH(Products)-LENGTH(REPLACE(Products,",","")))) as Products,
            substring_index(Products,",",-1) as Measurement,
            Products as `Products Details`,
            Value as Price,
            Status
		from `', schema_name , '`.`', table_name ,
		'` where status =4;');
	SET SQL_SAFE_UPDATES = 0;
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    SET SQL_SAFE_UPDATES = 1;
END &&  
DELIMITER ;      

	-- 4.
	-- after data were splited in 4 sub tables
    -- now is to check if there are products which have not been marked by a category
    -- in the column of status
    -- create a table to carry data of this kind and send a message back to lambda (or ec2 or ecs) 
    -- a message or email will be sent by lambda using SNS (or SES) to the person in charge
    
    -- call webvoir.sp_loading_check_remaining_after_split ("webvoir","0.PriceIndex",@remain);
    -- select @remain
    
DROP PROCEDURE IF EXISTS webvoir.sp_loading_check_remaining_after_split;
DELIMITER &&  
CREATE PROCEDURE webvoir.sp_loading_check_remaining_after_split (
	IN schema_name varchar(50),
	IN table_name varchar(50),
    OUT remain boolean
    )    
BEGIN  
	-- the table structure of remaining_data is the same as general table except it has a time_stamp column
    -- it will help DE to find the rows that need to be worked on today
	set @sql =concat ('
    CREATE TABLE  IF NOT EXISTS `',schema_name, '`.`remaining_data` LIKE  `',schema_name, '`.`', table_name, '`' 
    );
    
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    -- if it is not for db init, the table of remaining data has already existed.
    -- the column of 'Time_stamp' has already been added to the table as well.
    -- unfortunately, there is no "add column if not exist" in mysql.
    -- need to check before adding

    set @table_name = table_name;
    set @schema_name =schema_name;
    set @column_name="Time_stamp";
    IF NOT EXISTS( SELECT NULL
		FROM INFORMATION_SCHEMA.COLUMNS
	    WHERE table_name = @table_name
		AND table_schema = @schema_name
		AND column_name = @column_name)  THEN
             
		set @sql =concat ('
		ALTER TABLE `',schema_name, '`.`remaining_data` ADD COLUMN Time_stamp TIMESTAMP not null DEFAULT CURRENT_TIMESTAMP ;' 
		);
		PREPARE dynamic_statement FROM @sql;
		EXECUTE dynamic_statement;
		DEALLOCATE PREPARE dynamic_statement;
	END IF;
    
    call webvoir.sp_loading_count_table (schema_name, table_name, @remain_rows," where status is null ");
    IF @remain_rows >0 THEN 
		-- Someone needs to work today...
        -- the product can be a new one to the database, just add the product name in the list
        -- execute the modified sp, rerun the parent SP, hopefully, all rows are splitted to their category table.
		set remain = 1;
        -- insert the remaining rows to table of 'remaining_data'
		set @sql = concat('
			INSERT INTO `', schema_name , '`.`remaining_data` 
			select * from `', schema_name , '`.`', table_name , 
			'` where status is null;');
			PREPARE dynamic_statement FROM @sql;
			EXECUTE dynamic_statement;
			DEALLOCATE PREPARE dynamic_statement;
    ELSE
		-- hooray! 
		set remain =0;
	END IF;
    
    select @remain;
END &&  
DELIMITER ;   
   

-- Note:
-- if you wish to use mysql locally for testing purpose:
-- you can load data from local file (.csv or .txt) to localhost mysql
-- the sql command is :
-- LOAD DATA LOCAL INFILE '/path/to/priceindex.csv' INTO TABLE `0.PriceIndex` FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;  
-- however, there might be two errors (3948 and 2068)
-- there are posts online which only suggest the solution for either of the two, finally, i figured out the solution to solve the both problems.
-- no worries. the newer version of mysql disables import and export by default.
-- if you use mac, $ sudo nano /etc/my.cnf 
-- input below:
/* 
	[mysqld]
	secure_file_priv=''
	local_infile    = 1

	[client]
	local_infile    = 1
*/
-- in your mysql client:
-- execute below sql query:
-- set global local_infile=1
-- then, you can use the load data command locally. you might need to set global local_infile=1 everytime you reboot. 
-- so i usually run this command before the load data command.
-- loading files from local to localhost is just for testing. so i don't mind the repetition.




