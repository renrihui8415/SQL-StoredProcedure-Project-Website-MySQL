/*
#### Database Init ####
#### This file builds procedures which can be used to create all the tables that needed for data loading.####
#### The tables can be used to store data in the following file of 'loading.sql'.
-- --------------------------------------------------------
-- 0. schema building
-- 1. create stored procedure (sp) for log table for loading process
-- Note: creating sp doesnot mean to create log table. we need to use sp to create table in the end.
-- 2. create stored procedure (sp) for general table for original data (Price Index)
	--  create stored procedure (sp) smaller tables according to their category (food, cosmetics, energy, tobacco)
-- 3. creare a parent SP to include all the above child SPs
-- 4. just call the parent SP, all child SPs will be called automatically
	-- tables will be created and database is ready for data loading in the next step
-- ----------------------------------------------------------
-- 0. for schema building
-- data for this website is about price index for 'Food', 'Cosmetics', 'Energy' and 'tobacco'.
-- tables are under one schema. One general table for original data. One general table for reporting.
-- Four subtables for four category. 
-- Procedure are mainly used for database init/data loading, analyzing and reporting.
-- A user will be created for database maintanance.
-- 0.1 create user 

-- MySQL allows to create user with sha256 password
-- drop user 'your_username'@'%';
-- create user 'your_username'@'%' Identified by 'your_password';
-- the .sql files which are uploaded by Terraform automatically will have hashed password
-- instead of plain text.
-- the password in plain text, keep it safe
-- also, we can use AWS secret manager to store the username and password
-- options vary.

-- login RDS (MySQL) with username and plain password on MySQL Workbench
-- build new connections with cloud MySQL using its endpoint

-- 0.2 grant priviledges to users
-- to grant priviledges as admin
-- GRANT ALL PRIVILEGES ON exampleschema.* TO 'your_username'@'%';
-- more can be found :https://www.strongdm.com/blog/mysql-create-user-manage-access-privileges-how-to#:~:text=To%20create%20a%20user%20with,their%20privileges%20to%20other%20users.
-- REMEMBER to always grant the least priviledge for users

-- 1. to create sp for for log table(for loading and reporting)
-- the log table i created only shows the information i need for error handling. 
-- select CURRENT_TIMESTAMP;  the result is precision of 3.
-- select now(6) 
-- to make time precision be 6. the performance of db is very fast. 
-- a precision of 3 can't be enough to differenciate all log events
*/
DELIMITER && 
DROP PROCEDURE IF EXISTS exampleschema.sp_init_logtable_for_loading ;
CREATE PROCEDURE exampleschema.sp_init_logtable_for_loading (
	IN schema_name varchar(50),
    IN event_source varchar(50),
    IN table_name varchar (50),
    IN event_status tinyint,
    IN total_rows bigint,
    IN note_s varchar(800)
)   
BEGIN    
	-- to create log table if not exists
    set @sql =concat('
    create table IF NOT EXISTS `',schema_name, '`.`log_for_loading`  (
		Time_stamp datetime not null,
        EventSource varchar (50) NOT NULL,
        Tablename varchar (50) NOT NULL,
        Status tinyint not null,
        TotalRowsAffected bigint null,
        Notes varchar (800) null
	);');
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    
    -- to insert log into log table
    set @sql=concat ('
    insert into `',schema_name, '`.`log_for_loading`  Values (''', now(6),''',''', replace(event_source,"'","''"),''',''',table_name,''',''',event_status,''',''',total_rows,''',''', replace(note_s,"'","''"),''');');
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;

END &&  
DELIMITER ;   
/*
-- there will be log table for reporting as well:
*/
DELIMITER && 
DROP PROCEDURE IF EXISTS exampleschema.sp_init_logtable_for_reporting ;  
CREATE PROCEDURE exampleschema.sp_init_logtable_for_reporting (
	IN schema_name varchar(50),
    IN procedure_name varchar(50),
    IN note_s varchar(800)
)   
BEGIN    
	-- to create log table if not exists
    set @sql =concat('
    create table IF NOT EXISTS `',schema_name, '`.`log_for_reporting`  (
		Time_stamp datetime not null,
        ProcedureName varchar(50),
        Notes varchar (800) null

	);');
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    
    -- to insert log into log table
    set @sql=concat ('
    insert into `',schema_name, '`.`log_for_reporting`  Values (''', now(6),''',''', procedure_name,''',''',replace(note_s,"'","''"),''');');
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;

END &&  
DELIMITER ;  
-- 2. to create sp for general table --> Price Index
	-- to create smaller subcategory tables as well
DELIMITER && 
DROP PROCEDURE IF EXISTS exampleschema.sp_init_create_tables_for_loading ;  
CREATE PROCEDURE exampleschema.sp_init_create_tables_for_loading (
	IN schema_name varchar(50), 
    IN today_date varchar(50)
)   
BEGIN    
	/*
	-- create original data file, define all columns as string 
    -- allowing all data loaded first, later to split it into smaller tables based on
    -- their categories and check duplication in smaller tables as well
    -- mysql doesnot allow table name to be passed in as parameter,
    -- the solution is to use set @sql = here is the sql string
    -- then, execute sql later
    -- within sql string, + (plus sign) can't be used
    -- instead, we need to use concat to combine and get a complete string
    */
    set @sql =concat('
    create table IF NOT EXISTS `',schema_name, '`.`0.PriceIndex`  (
		Date varchar(255),
        GEO varchar(255),
        DGUID varchar(255),
        Products varchar(255),
        UOM varchar(255),
        UOM_ID varchar(255),
        SCALAR_FACTOR varchar(255),
        SCALAR_ID  varchar(255),
        VECTOR varchar(255),
        COORDINATE varchar(255),
        VALUE varchar(255),
        STATUS varchar(255),
        SYMBOL varchar(255),
        `TERMINATED` varchar(255),
        DECIMALS varchar(255),
        Primary Key (GEO,Date,Products)
        
	);');
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    /*
    -- the original data file should be created by date
    -- as we won't wish everytime new data loads/inserts into one table 
    -- and database has to do a lot of work on checking/excluding duplicates
    -- only today's new data will be processed
        
        
    ### while ####
    -- for each category, only one table will be created
	-- in the original data file, the composite pk is Date and Products
    -- the divided tables will inherit this pk
    -- but the process is a little bit different
    -- load data into temp first,
    -- if everything goes well, UPSERT info into persistant table 
	
	-- create table for food
    */
    set @sql=concat('
    create table IF NOT EXISTS `', schema_name , '`.`1.Food` (
		GEO char(10),
		Date char(10),
		Year int,
		Month tinyint, 
		Products varchar(100),
        Measurement varchar(100),
		`Products Details` varchar(255),
		Price decimal(10,2),
        Status tinyint DEFAULT NULL,
        Primary Key (GEO,Date,`Products Details`)
	); ');
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    -- create table for Gas
    set @sql = concat('
    create table IF NOT EXISTS `', schema_name , '`.`3.Energy` (
		GEO char(10),
		Date char(10),
		Year int,
		Month tinyint, 
		Products varchar(100),
        Measurement varchar(100),
		`Products Details` varchar(255),
		Price decimal(10,2),
        Status tinyint DEFAULT NULL,
        Primary Key (GEO,Date,`Products Details`)
	); ');
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    -- create table for Cosmetics
    set @sql =concat('
    create table IF NOT EXISTS `', schema_name , '`.`2.Cosmetics` (
    	GEO char(10),
		Date char(10),
		Year int,
		Month tinyint, 
		Products varchar(100),
        Measurement varchar(100),
		`Products Details` varchar(255),
		Price decimal(10,2),
        Status tinyint DEFAULT NULL,
        Primary Key (GEO,Date,`Products Details`)
	); ');
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    -- create table for Tobacco
    set @sql=concat('
    create table IF NOT EXISTS `', schema_name , '`.`4.Tobacco` (
    	GEO char(10),
		Date char(10),
		Year int,
		Month tinyint, 
		Products varchar(100),
        Measurement varchar(100),
		`Products Details` varchar(255),
		Price decimal(10,2),
        Status tinyint DEFAULT NULL,
        Primary Key (GEO,Date,`Products Details`)
	); ');
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    /*
    -- there are other tables to be created as well
    -- 1. log table for loading
    -- 2. log table for reporting 
    */
    
END &&  
DELIMITER ;   
/*
-- 4. to create parent SP for database init 
*/
DELIMITER && 
DROP PROCEDURE IF EXISTS exampleschema.sp_init_database ;  
CREATE PROCEDURE exampleschema.sp_init_database (
	IN schema_name varchar(50), 
    IN today_date varchar(50),
    IN db_event_source varchar(50),
    IN table_name varchar (50),
    IN db_event_status tinyint,
    IN total_rows_affected bigint,
    IN note_s varchar(800)
    )    
BEGIN  
	call exampleschema.sp_init_logtable_for_loading (schema_name,db_event_source,table_name,db_event_status,total_rows_affected,note_s);
    call exampleschema.sp_init_logtable_for_reporting (schema_name,db_event_source,note_s);
	call exampleschema.sp_init_create_tables_for_loading (schema_name,today_date);
    call exampleschema.sp_init_calendar_month (schema_name);

END &&  
DELIMITER ;   
/*
-- ----------------------------------------------------------
-- THIS ONE SINGLE SQL QUERY WILL GET MYSQL READY TO USE

-- AAA) for testing, we can use below sp to clear all tables and sp created by one owner
-- Note:
-- In order to make it more flexible, i add a parameter of 'excluding_tables'\
-- this allows the sp to delete all tables except for the tables stated in this parameter
-- SELECT * FROM information_schema.tables; 
-- this command will help you find all tables in the db
*/
DELIMITER && 
DROP PROCEDURE IF EXISTS exampleschema.sp_init_drop_all_tables ;  
CREATE PROCEDURE exampleschema.sp_init_drop_all_tables (
	IN schema_name varchar(50),
    IN excluding_tables varchar(255) 
    )    
BEGIN  
	-- the query will get us a column with multiple rows
    -- use CURSOR to get all rows into a string
	DECLARE var_tables varchar (255);
    DECLARE finished INT DEFAULT 0;
	DECLARE cursor_table_list CURSOR for SELECT table_name FROM information_schema.tables 
		WHERE table_schema = schema_name;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished =1;
    OPEN cursor_table_list;	

	table_loop: LOOP
		IF finished =1 THEN
			LEAVE table_loop;
        END IF;
		FETCH cursor_table_list INTO var_tables;
        IF excluding_tables is not null AND locate(excluding_tables,var_tables)>0 THEN
			-- DONT DELETE
            SET @sql= concat('select ',1);
		ELSE 
			SET @sql=CONCAT('DROP TABLE IF EXISTS `',schema_name,'`.`', var_tables, '`;');
		END IF;
		PREPARE dynamic_statement FROM @sql;
		EXECUTE dynamic_statement;
		DEALLOCATE PREPARE dynamic_statement;
	END LOOP table_loop;

END &&  
DELIMITER ;   
/*

-- BBB) i found most reports require info month, weekday, date
-- to create a sp or user defined function for lookup sometimes better than build-in functions
-- below SP accepts numeric or none-numeric input
-- it will help check if the input is valid month info and find the coresponding month number 
*/
DELIMITER && 
DROP PROCEDURE IF EXISTS exampleschema.sp_init_calendar_month ;  
CREATE PROCEDURE exampleschema.sp_init_calendar_month (
	IN schema_name varchar(50)
    )    
BEGIN  
	/*
	-- note : 2 digit month number can be achieved by LPAD(MONTHNUMBER,2,0)
    -- set @schema_name ='exampleschema';
    */
	set @sql= concat('
	create table IF NOT EXISTS `', schema_name, '`.`99.month` (
		month_number_1_digit tinyint,
        month_name_short char(3),
        month_name_long varchar(20) ,
        month_name varchar(50),
        PRIMARY KEY (month_number_1_digit)
        
        );'
	);
    
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
	set @sql= concat('
	TRUNCATE table  `', schema_name, '`.`99.month` ;'
	);
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    -- insert values
	set @sql= concat('
	insert ignore into `', schema_name, '`.`99.month` values 
		("1","jan","january","jan, january"),
        ("2","feb","february","feb, February"),
		("3","mar","march","mar, march"),
        ("4","apr","april","apr, april"),
        ("5","may","may","may, may"),
        ("6","jun","june","jun, june"),
        ("7","jul","july","jul, july"),
        ("8","aug","august","aug, august"),
        ("9","sep","september","sep, september"),
        ("10","oct","october","oct, october"),
        ("11","nov","november","nov, november"),
        ("12","dec","december","dec, december");'
	);
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    
END &&  
DELIMITER ;  
/*
-- below SP is check single_month_string, the result be either 0 or a correct month number
*/
DELIMITER && 
DROP PROCEDURE IF EXISTS exampleschema.sp_init_calendar_month_checking;  
CREATE PROCEDURE exampleschema.sp_init_calendar_month_checking (
	IN schema_name varchar(50),
    IN month_str varchar(50),
    OUT month_num tinyint
    )    
BEGIN  
	/*
	-- note: 
    -- 2 digit month number * 1 = 1 digit month number
    -- character month name * 1 =0
    */
	set @result='';
	set @month_str=lower(month_str);
	set @sql= concat('
	select month_number_1_digit into @result from `', schema_name, '`.`99.month` 
		WHERE lower(month_name_short)= "', @month_str, '"
		or lower(month_name_short)= "', @month_str, '"
		or month_number_1_digit= "', @month_str, '"*1 ;'
	);
	-- select @sql;
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;

	IF @result='' THEN
		/*
		-- the month_string is not an invalid month name or month number
        -- there is no 'isnumeric' function in mysql to determine
        -- if a value is number or not
        -- the solution is to use REGEXP '^[0-9]+$' to check for integer
        -- REGEXP '^[0-9]+\\.?[0-9]*$' for float
        */
        IF @month_str REGEXP '^[0-9]+$' THEN
			IF @month_str>12 THEN
				set month_num=12;
			ELSEIF @month_str <1 THEN 
				set month_num=1;
			END IF;
		ELSE
			/*
            -- it is an invalid string
            */
			set month_num=0;
		END IF;
	ELSE
		set month_num=@result;
	END IF;    
    -- select @month_num;    
END &&
DELIMITER ;  
