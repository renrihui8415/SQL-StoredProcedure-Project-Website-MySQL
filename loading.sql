/*
#### Data Loading ####
#### This file builds procedures that can be used to load and split data.####
#### The data is can be used for reporting in the following file of "reporting.sql".####
-- ----------------------------------------------------------
-- 0. create stored procedure (sp) for logging
	-- this part is included in the file of 'init.sql'
-- 1. create stored procedure (sp) for load data from s3(or other datasource) to a temp table 
	-- 1.1 create temp table 
    -- here, we need to communicate with Lambda for data loading 
    -- because RDS Mysql doesnot support 'load data from s3' command
    -- like aurora mysql does.
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
-- things we should know about AWS RDS MYSQL~~~
-- Please note, I didn't mean Aurora MySQL.
-- aaa) mysql in aws doesnot support to use one command to import/export data from/to s3
-- for other db engines like aurora, postgres, sql server, oracle, even including redshift
-- they are easier to do the data transfer, the db engines can access s3 for direct import/export
-- but for RDS MySQL, we have no way but to apply a 3rd party AWS service to do the ETL~
-- AWS recommands data pipeline in Glue, but it is not free....
-- To reduce cost, I figure out the best way is to apply Lambda, my favorite, to load data with python. 
-- If you wish to spend less and don't mind coding, Lambda is a great choice.
-- If you don't want any coding, try and pay for GLUE.
-- (I will share Lambda function in python in another repository. )

-- bbb) using lambda to do ETL, be aware it only has a payload of 6mib
-- but we can learn from online examples to overcome this limitation
-- or we can use other computation in aws as well, EC2 or ECS (of course, these more powerful computation
-- systems are more expensive). 

-- ----------------------------------------------------------
-- 1. lambda checks the original data file and count the ROW NUMBER
-- 2. lambda call the sp in mysql to create a temp table
-- 3. after checking the log table for temp table creation
	-- lambda load data from datasource (s3) into temp table in mysql (using cursor)
	-- this will be achieved by Lambda from outside of mysql
-- 4. after loading to temp table, lambda will call parent SP and Mysql will proceed from here 
	-- until it finishes the loading completely
-- ----------------------------------------------------------------
-- 1 to create sp to build temp table
-- the tempt table is used to store data from .csv file in s3
-- the columns and their order in mysql table must be the same as .csv file 
-- the CREATE TABLE statement can be generated dynamically by lambda or EC2 or ECS
-- or can be pre-defined here in sp, of course, when original file changes
-- the sp here needs changing accordingly.
-- to reduce the management overhead, I use lamda to do some of the ETL
-- make the original data file in s3 only contains the columns we need for the report
-- these columns donot change frequently, if there is any change in the data source,
-- lambda will detect and make sure the .csv file is still loadable for mysql
*/

DELIMITER &&  
DROP PROCEDURE IF EXISTS exampleschema.sp_loading_create_temp_table;
CREATE PROCEDURE exampleschema.sp_loading_create_temp_table (
	IN schema_name varchar (50),
    IN temp_tablename varchar(50),
	IN table_name varchar(50),
    OUT result_for_sp_loading_create_temp_table tinyint
    )    
BEGIN   
	-- 1. Declare variables to hold diagnostics area information
	DECLARE exit handler for SQLEXCEPTION
		BEGIN
			GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE, 
			@errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;
			SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);
			SELECT @full_error;
            set result_for_sp_loading_create_temp_table=0;
            call exampleschema.sp_init_logtable_for_loading (schema_name, '1. temp table creation', temp_tablename,0, 0,@full_error);
				
        END;

	-- 2. the body SP
    START TRANSACTION;
		set @sql =concat('
		DROP TABLE IF EXISTS `',schema_name, '`.`', temp_tablename, '` ;'
		);
		PREPARE dynamic_statement FROM @sql;
		EXECUTE dynamic_statement;
		DEALLOCATE PREPARE dynamic_statement;

		-- to copy the table structure from permanent table
		-- temporary
		set @sql =concat ('
		CREATE TABLE  `',schema_name, '`.`', temp_tablename, '` LIKE  `',schema_name, '`.`', table_name, '`;'
										
		);
		PREPARE dynamic_statement FROM @sql;
		EXECUTE dynamic_statement;
		DEALLOCATE PREPARE dynamic_statement;
		set result_for_sp_loading_create_temp_table=1;
		-- after creating the temp table
		-- write log in the log table
		call exampleschema.sp_init_logtable_for_loading (schema_name, '1. temp table creation', temp_tablename,1, 0, concat("created from `",schema_name,"`.`",table_name,"`"));
		
    COMMIT;	
    select @result_for_sp_loading_create_temp_table;
END &&  
DELIMITER ;  
/*
-- ----------------------------------------------------------------
-- 2. after lambda checks log table for a eventsource of 'temp table creation' and find its status is 1
-- (or, to get the returned value @temp_table_created from sp)
-- it will load the data first and then call the following SP
*/

DELIMITER && 
DROP PROCEDURE IF EXISTS exampleschema.sp_loading_PriceIndex; 
CREATE PROCEDURE exampleschema.sp_loading_PriceIndex (
	IN schema_name varchar (50),
    IN temp_tablename varchar(50),
	IN table_name varchar(50),
    IN file_name varchar (50),
    IN total_rows bigint, maxerrors_allowed tinyint,
    OUT result_for_sp_loading_PriceIndex tinyint
    )    
BEGIN 
	-- 1. Declare variables to hold diagnostics area information
	DECLARE exit handler for SQLEXCEPTION
		BEGIN
			GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE, 
			@errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;
			SET @full_error = CONCAT("ERROR ", @errno, " (", @sqlstate, "): ", @text);
			SELECT @full_error;
            set result_for_sp_loading_PriceIndex=0;
            call exampleschema.sp_init_logtable_for_loading (schema_name, '2. data loading and splitting', temp_tablename,0, 0,@full_error);				
        END;
	-- 2. the body SP
    START TRANSACTION;  
		-- 1. MySQL is to count how many rows were loaded into temp table by lambda
		call exampleschema.sp_loading_count_table(schema_name, temp_tablename, @result_rows,"");

		-- 2. to compare the numbers 
		IF @result_rows >= (total_rows -maxerrors_allowed) THEN
			-- to load
			call exampleschema.sp_loading_upsert_general_table (schema_name, temp_tablename, table_name);
			-- to write log for successful loading
			call exampleschema.sp_init_logtable_for_loading (schema_name, concat("2.1 permanent table loading from temp table : ",temp_tablename), table_name,1, @result_rows,concat("loading from temptable to general table successfully. ","Rows skipped: ",(total_rows - @result_rows)));
			
			-- after loading to general table successfully,
			-- start loading for sub tables
			-- 3. to decide if split general table into sub tables
			-- for some reports, sub tables make report building easier
			-- in my website, I need subcategory tables 
			IF locate ('priceindex',table_name)>0 THEN
				-- update general table with status and load data to sub tables
				call exampleschema.sp_loading_priceindex_split (schema_name, table_name);
				-- check if the loading is complete
				call exampleschema.sp_loading_check_remaining_after_split (schema_name, table_name, @check_remain);
				
				IF @check_remain =0 THEN
					-- complete loading
					-- write log
					set result_for_sp_loading_PriceIndex =1;
					call exampleschema.sp_init_logtable_for_loading (schema_name, "2.2 general table splitting", table_name,1, @result_rows,"loading to subcategory tables successfully"); 
					
				ELSE 
					set result_for_sp_loading_PriceIndex =0;
					call exampleschema.sp_init_logtable_for_loading (schema_name, "2.2 general table splitting", table_name,0, 0,"loading to subcategory tables failed");
				END IF;
			-- ELSE
				-- loading process for other data files other than priceindex
			END IF;
		ELSE 
			-- don't load data from temp_table to permanent table
			-- just to write log for unsuccessful loading
            set result_for_sp_loading_PriceIndex =0;
			call exampleschema.sp_init_logtable_for_loading (schema_name, file_name, "2.1 permanent table loading failed",0, 0,'temp table contains incomplete data from original data file');
			
		END IF;
    commit;
    -- use session variable to carry the value
    select @result_for_sp_loading_PriceIndex;

END &&  
DELIMITER ;
/*
-- -------------------------------------------------------------------------
-- 3. below is another child SP to count rows in temptable
-- if the number is the same as lambda counts in the .csv file in s3
-- means all rows were loaded to mysql from s3
-- Note :
-- In order to make this sp more versatile in this project
-- a parameter of 'WHRE clause' can be added, so that the sp can count the rows on some conditions
-- later , when we check the remaining rows in general table after splitting, 
-- use "where status is null" to get the row number
*/

DELIMITER &&  
DROP PROCEDURE IF EXISTS exampleschema.sp_loading_count_table;
CREATE PROCEDURE exampleschema.sp_loading_count_table (
	IN schema_name varchar(50),
	IN table_name varchar(50),
    OUT total_for_sp_loading_count_table bigint,
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
    
    set total_for_sp_loading_count_table = @row_no;
    
	select @total_for_sp_loading_count_table;
END &&  
DELIMITER ;   
/*
-- to get the out paramter: 
-- -------------------------------------------------------------------------
-- 4. below is child sp for upsert from template to permanent table (general table in this project)
-- If there is PK or unique index in the table, any duplicate loading would cause error of 'key violation'
-- in order to update only new records into permanent table and avoid error of key violation,
-- UPSERT is applied here
-- there are 3 ways to upsert, i chose method of 'REPLACE' for this project
-- because one report from dashboard is based on this table, we have to make data in this table complete,accurate, up to date, and no duplicates
*/
DELIMITER &&  
DROP PROCEDURE IF EXISTS exampleschema.sp_loading_upsert_general_table;
CREATE PROCEDURE exampleschema.sp_loading_upsert_general_table (
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
/*
-- -------------------------------------------------------------------------
-- 5. to create SP for loading --> Food, Energy, Cosmetics, Tobacco
*/
DELIMITER &&  
DROP PROCEDURE IF EXISTS exampleschema.sp_loading_priceindex_split;
CREATE PROCEDURE exampleschema.sp_loading_priceindex_split (
	IN schema_name varchar(50),
	IN table_name varchar(50)
    )    
BEGIN    
	-- the original data files contains various products 
    -- the data will be split into sub tables according to their categories
    -- @tablename=exampleschema.`0.PriceIndex`;
    
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
	/*
    -- after we got original data file updated
    -- to start loading for 1st category
    -- in this project, the data will be continuously got from online
    -- the new data might be duplicate with existing data in database
    -- therefore , a UPSERT will be applied here. 
	-- UPSERT CAN BE ACHIEVED IN 3 WAYS:
    -- https://www.javatpoint.com/mysql-upsert#:~:text=UPSERT%20is%20one%20of%20the,words%20named%20UPDATE%20and%20INSERT.
    -- for TESTING purpose, all these 3 methods will be used
    -- 1. INSERT IGNORE INTO...
    */
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
	/*  
	-- 2. UPSERT using Replace:
    -- load for cosmetics
	*/
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
/*
-- 6.
-- after data were splited in 4 sub tables
-- now is to check if there are products which have not been marked by a category
-- in the column of status
-- create a table to carry data of this kind and send a message back to lambda (or ec2 or ecs) 
-- a message or email will be sent by lambda using SNS (or SES) to the person in charge
*/    

DELIMITER &&  
DROP PROCEDURE IF EXISTS exampleschema.sp_loading_check_remaining_after_split;
CREATE PROCEDURE exampleschema.sp_loading_check_remaining_after_split (
	IN schema_name varchar(50),
	IN table_name varchar(50),
    OUT result_for_sp_loading_check_remaining_after_split boolean
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
    
    call exampleschema.sp_loading_count_table (schema_name, table_name, @remain_rows," where status is null ");
    IF @remain_rows >0 THEN 
		-- Someone needs to work today...
        -- the product can be a new one to the database, just add the product name in the list
        -- execute the modified sp, rerun the parent SP, hopefully, all rows are splitted to their category table.
		set result_for_sp_loading_check_remaining_after_split = 1;
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
		set result_for_sp_loading_check_remaining_after_split =0;
	END IF;
    
    select @result_for_sp_loading_check_remaining_after_split;
END &&  
DELIMITER ;   
   
/*
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

	[mysqld]
	secure_file_priv=''
	local_infile    = 1

	[client]
	local_infile    = 1

-- in your mysql client:
-- execute below sql query:
-- set global local_infile=1
-- restart MySQL (this is important)
-- then, you can use the load data command locally. you might need to set global local_infile=1 everytime you reboot. 
-- so i usually run this command before the load data command.
-- loading files from local to localhost is just for testing. so i don't mind the repetition.

-- finally , to call the parent sp
*/

