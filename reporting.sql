#### MySQL Reporting ####
#### This file builds procedures which can be used to obtain all the report data that needed for my website.####
#### The report data will be sent to AMAZON Simple Storage Service (S3) to serve as the static content of the website.####
-- --------------------------------------------------------
-- This time, i create parent SP before child SPs for report building.
-- 1. to create SP for a general report (which contains info of year, month, geo, product category and its sub category as well)
	-- 6 parameters are allowed for the SP
    -- in order to increase fault tolerance and user experience, 
    -- there will be no restriction on input's data type
	-- anyone who use the SP, if he inputs invalid values for parameters,
    -- SP will try to return a reasonable result 
    -- 1.1 to create a general table for reporting
    -- 1.2 to create Stored Procedure (SP) for general report 
		-- 1.2.1 to process 6 parameters respectively
			-- 1.2.1.1 i tested different ways to process the parameters 
				-- for year and month, i will test if the values are int or not
                -- for geo_limit, ie., the values for regions like 'Canada, USA,etc', 
					-- i will test if they can be found in the data table
				-- for parameter of category, I use loop to filter out those input for main category and those for sub category
					-- put them into 2 lists respectively
        -- 1.2.2 to get correct condition statement for these 6 parameters
			-- 1.2.2.1 use loop to find which statements are null and which are not null
            -- 1.2.2.2 to add 'where' and 'and' at correct place to make the where_statement work
        -- 1.2.3 to construct the nested query using the where_statement
        -- 1.2.4 to export the result so that my website can use
-- 2. to create child SPs for the parent SP in the above step
-- the queries which are executed often can be isolated from parent SP and make it to be child SP
-- the parent SP becomes not that comlicated and long. 
-- 2.1 to create SP for month number checking, 
	-- to varify a string can be coverted into a month number and put the number in the where-clause
-- 2.2 to create SP for max values in a table
	-- to get the max(year) or min(year) from a table , for example
-- 2.3 to create SP to check if a value exists in a table
	-- to check if the values in the geo_limit exists in the GEO column in the data table
    -- meanwhile construct condition_statement for GEO parameter

-- ----------------------------------------------------------------------------------
-- 1.1 below is to create a general table for reporting
-- it contains all the information we need for reports
-- this table is specially for general report. 
-- there are smaller tables based on detailed category created in the file of 'loading.sql' as well,
-- they are specially for detailed reports, and they can make nested queries less complicated.
-- so , different reports base on different tables

DROP PROCEDURE IF EXISTS webvoir.sp_reporting_50_general_table_for_report_building;
DELIMITER &&  
CREATE PROCEDURE webvoir.sp_reporting_50_general_table_for_report_building (
	IN schema_name varchar(50),
	IN table_name varchar(50),
    IN reporting_table_name varchar(50)
    )    
BEGIN  
	set @schema_name=schema_name;
    set @table_name=table_name;
    -- a). copy data from another table
    -- modify and generate new table for reporting

    set @sql= concat_ws('',
    'create table IF NOT EXISTS `',@schema_name,'`.`',reporting_table_name,'` 
    select *, ',
		' LEFT(date,4) as Year, 
		RIGHT(date,2) as Month, '
		'CASE WHEN locate(",",Products)>0
			THEN SUBSTRING_INDEX(Products,",",(LENGTH(Products)-LENGTH(REPLACE(Products,",",""))))
		WHEN locate("(", Products)>0 
			THEN SUBSTRING(Products, 1, locate("(",Products)-1)  
		END Product, ',
        'CASE WHEN locate(",",Products)>0
			THEN substring_index(Products,",",-1) 
		WHEN locate("(", Products)>0 
			THEN  SUBSTRING(Products, locate("(",Products),(locate(")",Products)-locate("(",Products)))  
		END Measurement ',
	 ' from ', '`',@schema_name, '`.`',@table_name,'`');
    /* to pack a sql query in Concat function
    , the easier way is to write the hard code query that runs successfully
    , then translate the hard code query into dynamic one
    create table IF NOT EXISTS `webvoir`.`1.report` 
    select *, CASE WHEN locate(",",Products)>0
			THEN SUBSTRING_INDEX(Products,",",(LENGTH(Products)-LENGTH(REPLACE(Products,",",""))))
		WHEN locate("(", Products)>0 
			THEN SUBSTRING(Products, 1, locate("(",Products)-1)  
		END Product, 
        CASE WHEN locate(",",Products)>0
			THEN substring_index(Products,",",-1) 
		WHEN locate("(", Products)>0 
			THEN  SUBSTRING(Products, locate("(",Products),(locate(")",Products)-locate("(",Products)) ) 
		END Measurement  from `webvoir`.`0.priceindex`
	*/

    PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
	
    -- b). to point to the right table 
    set @table_name=reporting_table_name;
	select @table_name;
    -- c). to tailor the table to reports
    -- before modify the table, check if column already exists 
    -- just need to check "products details" 
    -- if this column exists, it means the table already created and modified previously
    set @column_name='Products Details';
    set @schema_name=schema_name; 
    set @sql = concat_ws('','
	SELECT count(*) into  @column_exists
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE table_name = "',@table_name,
	'" AND table_schema = "', @schema_name,
	'" AND column_name = "', @column_name, '";');
    -- here,within the sql string, we only need to quote the table name, etc
    -- no need to use ``, as mysql will consider `` as part of the table name
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    /*
    SELECT count(*) into @column_exists
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE table_name = '1.report'
	AND table_schema ='webvoir'
	AND column_name = 'Products Details';
    select @column_exists;
    */
    select @column_exists;
	IF @column_exists <1 THEN
		set @sql = concat('
			ALTER TABLE `',@schema_name, '`.`', @table_name ,
			'` RENAME COLUMN Products TO `Products Details`',';');
			PREPARE dynamic_statement FROM @sql;
			EXECUTE dynamic_statement;
			DEALLOCATE PREPARE dynamic_statement;
		set @sql = concat('
			ALTER TABLE `',@schema_name, '`.`', @table_name ,
			'` RENAME COLUMN Product TO `Products`',';');
			PREPARE dynamic_statement FROM @sql;
			EXECUTE dynamic_statement;
			DEALLOCATE PREPARE dynamic_statement;
		set @sql = concat('
			ALTER TABLE `',@schema_name, '`.`', @table_name ,
			'` RENAME COLUMN Value TO `Price`',';');
			PREPARE dynamic_statement FROM @sql;
			EXECUTE dynamic_statement;
			DEALLOCATE PREPARE dynamic_statement;
	END IF;
END &&  
DELIMITER ;
call webvoir.sp_reporting_50_general_table_for_report_building('webvoir','0.PriceIndex','1.report');
-- ----------------------------------------------------------------------------------
-- 1.2 to create SP for the general report 
-- within this stored procedure, child procedures will be called. 
-- 6 parameters can be passed in at the same time


DROP PROCEDURE IF EXISTS webvoir.sp_reporting_1_price_by_year_month_geo_category;
DELIMITER &&  
CREATE PROCEDURE webvoir.sp_reporting_1_price_by_year_month_geo_category (
	IN schema_name varchar(50),
	IN table_name varchar(50),
    IN delimiter_reporting_1 varchar(10),
    IN year_no varchar(50),
    IN month_no varchar(50),
    IN geo_limit varchar(50),
    IN category varchar(50),
    OUT test varchar(255)
    )    
BEGIN 
	-- to work on the parameters
    -- the more parameters are added into sp, the more complex the sp is.
    -- that does not necessarily mean the sp will run slowly 
    -- it all depends on how we construct the nested query
    -- 1 for parameter of year_no
	call webvoir.sp_reporting_99_aggregation(schema_name,table_name,"max","year","",@latest_year);
    call webvoir.sp_reporting_99_aggregation(schema_name,table_name,"min","year","",@earliest_year);
    -- Note:@latest_year in data file might not be the current year
	-- find the max and min year number using one of the child procedure
    -- if year_no that users input exceeds the year range in data file,
    -- SP will return the @latest_year or @earliest_year
    -- year_no also accepts null and any characters to increase fault tolerance
    -- if a string (null or characters) is not covertable to int, 
    -- cast() function will return a '0' and SP will get all years' record returned
    -- if users input multiple years when calling SP,
    -- each single year will be taken out of the string and be varified. if there are 
    -- inputs that are not years, SP will give a result as well. 
    -- that doesn't mean the SP can't give the accurate result. if the input all correct,
    -- the SP will give the correct result accordingly. if the input, like typo by users,
    -- the SP won't return null, it will give a closest match anyway.
    -- no matter it is my project or not, to make SP accurate, flexible, user-friendly is always my goal.
    set @year_string=year_no;
    -- add a comma to the users' input, 
    -- this make the comma number equals the value number
    -- no matter single year or multiple years, now both situation 
    -- can be applied to the same loop
    set @year_string =concat(@year_string,delimiter_reporting_1);
    select @year_string;
    set @count_year=LENGTH(@year_string)-LENGTH(REPLACE(@year_string, delimiter_reporting_1,''));
    -- get how many year values in the year string
    -- below is to loop through the year string to varify every value
    set @count =1; -- for each value in the string
    set @start_position=1; -- start searching from the beginning of the string
    set @year_number = ''; -- collect both valid and modified values in the end
    -- if we set @year_number = null, concat (null, anystring) will get null
    year_loop: LOOP
		WHILE @count <= @count_year DO
			
			-- @start_position marks the point where locate function starts
			set @single_year_string=substring(@year_string,@start_position,locate(delimiter_reporting_1,@year_string,@start_position)-@start_position);
			-- the content between two commas are one single year value
			set @start_position_new=locate(delimiter_reporting_1,@year_string,@start_position)+1 ; 
			set @start_position=@start_position_new; -- move the staring position to the next comma
			set @single_year_string=trim(@single_year_string);
            -- always trim first
			-- after we get one single year , 
			-- use cast() to convert string to int (signed)
			/* below IF Clause is not working ,change to use IF function instead*/
			IF @single_year_string REGEXP '^-?[0-9]+$' THEN
				set @single_year_string=cast(@single_year_string as signed);
                select @single_year_string;
				IF @single_year_string > @latest_year THEN
					SET @single_year_string = @latest_year;
				ELSEIF @single_year_string<@earliest_year THEN
					SET @single_year_string =@earliest_year;
				END IF;
				SET @year_number=concat(@year_number,@single_year_string,delimiter_reporting_1);
             
                -- accumulating value one by one into the find year string --> @year_number
			ELSE 
				set @year_number = 'all';
				-- once a character string is found
				-- let sp to find all years data
				LEAVE year_loop;
				-- which means the sp will return records for all years
	
			END IF;
			set @count=@count+1;
		END WHILE;
        LEAVE year_loop;
    END LOOP year_loop;
    -- up until now, @year_no can either be 'all' or string of number(s)
    -- Note:
    -- the latter contains a comma in the end, need to eliminate later in nested sql query
	-- to build where-clause for parameter of 'year_no'
    -- here , we only build part of the where-clause
    -- 1) we don't know if all input are null or not , if yes, there will be no where-clause at all
    -- 2) we can't put 'where' this word into every where-clause, there\s only one "where" in one query
    -- 3) the operator like 'and' and 'or' are also decided in the end in the query, as we don't know 
		-- which parameters are null and which are not.
	-- 4) if we have more after the where-clause , like order by clause, we need to decide for ',', the comma
		-- it won't necessarily be after the last parameters, as the last para can be null.
	IF @year_number<>'all' THEN
		set @year_number = left(@year_number,length(@year_number)-1); 
	END IF;
    -- delete the trailing comma
    -- if we put quote around each value, we need to make sure there is no unnecessary space in the value
    -- like '1995' and '1995 ' is totally different if they are put in where-clause and be quoted
    -- that's why in the above loop, all value are trimmed.
    set @delimiter_with_quotes_reporting_1=concat('"',delimiter_reporting_1,'"');
	set @where_year= IF(@year_number='all',' ',concat(' year in ("', replace(@year_number,delimiter_reporting_1,@delimiter_with_quotes_reporting_1),'")'));
	-- IF @year_no ='all' THEN 
	-- all years are required, there will be no where-clause for year parameter

    -- 2.2 for parameter of month_no
	#### ATTENTION ####
    -- when we define user parameter( @aaaa), this is a parameter out of sp
    -- after sp executed, @aaaa won't be reallocated to null or ''
    -- if two SPs share the same @aaaa, it would cause error 
    -- try name @aaaa uniquely.
    set @month_string = month_no;
    set @month_string =concat(@month_string,delimiter_reporting_1);
	-- attention: there will be no space between concat and '('
    -- otherwise: mysql can't run the concat function and @month_string become ''.
    set @count_month=LENGTH(@month_string)-LENGTH(REPLACE(@month_string, delimiter_reporting_1,''));
    set @count =1;
    set @start_position=1;
    set @month_number = ''; 
    -- this variable is to collect all numeric string
    -- attention: if we set @month_number =NUll, the result will null after concat any string
    -- we need to set it '', or ','
    month_loop: LOOP
		WHILE @count <= @count_month DO
			-- @start_position marks the point where locate function starts
			set @single_month_string=substring(@month_string,@start_position,locate(delimiter_reporting_1,@month_string,@start_position)-@start_position);
			set @start_position_new=locate(delimiter_reporting_1,@month_string,@start_position)+1 ;
			set @start_position=@start_position_new;
			-- after we get one single month string
			-- use child sp in the file of 'init.sql' to check its validation
            -- with the help of this sp, the result will be either 0 or a correct month number
            -- when directly calling sp within sp, to use local variables, not user variables

            set @schema_name =schema_name;      
            set @single_month_string=trim(@single_month_string);
            call webvoir.sp_init_calendar_month_checking(schema_name,@single_month_string, @check_result);	
            select @check_result;
			IF @check_result =0 THEN 
				-- the month input is not valid 
                -- sp will return all data 
                set @month_number ='all';
                LEAVE month_loop;
			ELSE
				-- continue to varify and collect valid month values
                set @month_number=concat(@month_number,@check_result,delimiter_reporting_1);
                -- attention: there is a trailing and begining comma in the string
            END IF;
			set @count=@count+1;
		END WHILE;
        LEAVE month_loop;
	END LOOP month_loop;
    IF @month_number<>'all' THEN
		set @month_number = left(@month_number,length(@month_number)-1); 
	END IF;
    -- delete the trailing comma
	set @where_month= IF(@month_number='all',' ',concat(' month in ("', replace(@month_number,delimiter_reporting_1,@delimiter_with_quotes_reporting_1),'")'));
	-- IF @year_no ='all' THEN 
	-- all years are required, there will be no where-clause for year parameter
    select @where_month;
    
    -- 2.3 for parameter of geo-limit
    -- although my website only contains price info of one country and her provinces;
    -- i still make the sp accept parameter of multiple value. 
	
    set @geo_string =geo_limit;
	-- check the values by searching in the data table
	call webvoir.sp_reporting_99_value_exists(schema_name,table_name,"GEO",@geo_string,delimiter_reporting_1,0,@geo_exists,@where_clause_for_geo);
	select @geo_exists;    
	set @where_geo= IF(@geo_exists=0,' ',@where_clause_for_geo);
	-- Note: 
	-- when we call this SP defining geo_limit parameter like 'Canada, USA', 
	-- all countries as together be quoted,
	-- while, in the @sql string, each country should be quoted respectively
	-- like GEO in ('Canada', 'USA')
	-- the solution is to replace every comma in the parameter to be surrounded by quotes
    -- this is extremely important!!!!! otherwise, there is no way, my website can call the SP using multi-value string using java.
    -- the query will be constructed at the end of the SP with other parameters
    select @where_geo;


    -- 2.4 for parameter of category
    -- this parameter accepts large category and sub category
    -- if the user input 'food' or 'energy' (large category)
    -- SP will search and return results based on the column of status 
    -- if the user input 'milk' or 'orange'
    -- SP will search and return results based on the column of products
    
    -- to check for large category first, add it in the where-clause
    -- to check for sub category, add it in the where-clause also
    -- to select instinct values as there might be overlapping for the above two where-clause
    set @category_string=category;
    
    -- for inner loop (main):
	set @main_category_string=concat_ws(delimiter_reporting_1,'food', 'energy', 'cosmetics','tobacco');
    set @count_main_category=LENGTH(@main_category_string)-LENGTH(REPLACE(@main_category_string, delimiter_reporting_1,''));
    set @count_inner_loop =1;
    set @start_position_inner_loop=1;
    set @main_category_list = ''; -- use it to search in column status
    
    -- for outer loop (sub):
    set @sub_category_string=lower(@category_string);
    set @sub_category_string=concat(@sub_category_string,delimiter_reporting_1);
    set @count_sub_category=LENGTH(@sub_category_string)-LENGTH(REPLACE(@sub_category_string, delimiter_reporting_1,''));
    set @count_outer_loop =1;
    set @start_position_outer_loop=1;
    set @sub_category_list = ''; -- use it to search in column products
    
    select @sub_category_string ;
    select @sub_category_list ;
    select @single_sub_category_string ;
    select @main_category_list;

    -- attention: if we set @value_list =NUll, the result will null after concat any string
    -- we need to set it '', or ','
    -- ---------------------------------------------------------------------------------------------------------
    category_loop: LOOP
		WHILE @count_outer_loop <= @count_sub_category DO
			-- @start_position marks the point where locate function starts
			set @single_sub_category_string=substring(@sub_category_string,@start_position_outer_loop,locate(delimiter_reporting_1,@sub_category_string,@start_position_outer_loop)-@start_position_outer_loop);
			set @start_position_outer_loop_new=locate(delimiter_reporting_1,@sub_category_string,@start_position_outer_loop)+1 ;
			set @start_position_outer_loop=@start_position_outer_loop_new;
            set @single_sub_category_string=trim(@single_sub_category_string);
			-- --------------------------------------------------------------------------------------------------
            -- after we get one single_sub_category_string, checking within main category
			IF LOCATE(@single_sub_category_string,@main_category_string)>0 THEN 
				-- if the users' input values belong to main categories,
				-- set the value to main category list, later to search in column status
				-- attention: there is a trailing delimiter_value (comma) in the string
				set @main_category_list = concat(@main_category_list,@single_sub_category_string,delimiter_reporting_1);
			ELSE -- set the value to sub category list, later to search in column products
				set @sub_category_list= concat(@sub_category_list,@single_sub_category_string,delimiter_reporting_1);
			END IF;
            -- --------------------------------------------------------------------------------------------------
			set @count_outer_loop=@count_outer_loop+1;
		END WHILE;
        LEAVE category_loop;
	END LOOP category_loop;
	-- ---------------------------------------------------------------------------------------------------------
	-- there is one more step for main category, in data table
    -- main category is represented by numbers 
    set @main_category_list= replace(@main_category_list, 'food', 1);
    set @main_category_list= replace(@main_category_list, 'cosmetics', 2);
    set @main_category_list= replace(@main_category_list, 'energy', 3);
    set @main_category_list= replace(@main_category_list, 'tobacco', 4);
    -- delete the trailing ',' from both list
    IF trim(@main_category_list) <>'' THEN
		set @main_category_list= left(@main_category_list,length(@main_category_list)-length(delimiter_reporting_1));
	END IF;
    IF trim(@sub_category_list) <>'' THEN
		set @sub_category_list= left(@sub_category_list,length(@sub_category_list)-length(delimiter_reporting_1));
    END IF;
    -- now is to create where-clause
    -- in below IF function, we need to use if a list ='', we can't use a list is null, as in mysql, ='' is different from is null
    set @where_status= IF(@main_category_list='', ' ', concat(' status in ("', replace(@main_category_list,delimiter_reporting_1,@delimiter_with_quotes_reporting_1),'")'));
    set @where_products= IF(@sub_category_list='', ' ', concat(' status in ("', replace(@sub_category_list,delimiter_reporting_1,@delimiter_with_quotes_reporting_1),'")'));
    select @where_products;
 
	
    -- up until now, we have created the parts of where-clause
    -- now is to build the complete and complex nested query
	-- the where-clause is most tricky part
    -- we have: @where_year, @where_month, @where_geo, @where_status,@where_products
    -- every part has two possibilities, null or not-null
    -- the where statement contains: where condition 1 (and condition2) (and condition3) (....)
    -- the word of 'where' won't be used unless at least one of conditions exist
    -- the word of 'and' won't be used unless more than one conditions exist
    set @delimiter_where_condition ='|';
    set @condition_string = concat_ws(@delimiter_where_condition,@where_year, @where_month, @where_geo, @where_status,@where_products);
    set @condition_string=concat(@condition_string,@delimiter_where_condition);
    set @count_condition=LENGTH(@condition_string)-LENGTH(REPLACE(@condition_string, @delimiter_where_condition,''));
    set @count_condition_loop =1;
    set @start_position_condition_loop=1;
    set @condition_list = ''; -- use it to collect all those not-null conditions,
    -- later this list will be put into where_statement
    
    select @condition_string ;
    select @condition_list ;
    select @single_sub_category_string ;
    select @main_category_list;

    -- attention: if we set @value_list =NUll, the result will null after concat any string
    -- we need to set it '', or ','
    -- ---------------------------------------------------------------------------------------------------------
    condition_loop: LOOP
		WHILE @count_condition_loop <= @count_condition DO
			-- @start_position marks the point where locate function starts
			set @single_condition_string=substring(@condition_string,@start_position_condition_loop,locate(@delimiter_where_condition,@condition_string,@start_position_condition_loop)-@start_position_condition_loop);
			set @start_position_condition_loop_new=locate(@delimiter_where_condition,@condition_string,@start_position_condition_loop)+1 ;
			set @start_position_condition_loop=@start_position_condition_loop_new;
			-- --------------------------------------------------------------------------------------------------
            -- after we get one single_condition_string, checking if null;
            -- only conditions that are not null can be written to where_statement
			IF trim(@single_condition_string)<>'' THEN 
				set @condition_list= concat(@condition_list,@single_condition_string,@delimiter_where_condition);
			END IF;
            -- --------------------------------------------------------------------------------------------------
			set @count_condition_loop=@count_condition_loop+1;
		END WHILE;
        LEAVE condition_loop;
	END LOOP condition_loop;
    
	-- now we have @condition_list based on users' input
    -- aaa) the list is empty, users input nothing for these parameters they want to know all
    -- bbb) the list is not empty, compose the list to be a complete where_statement
    
    IF trim(@condition_list)='' THEN
		set @where_statement=' ';
	ELSE
		-- the conditions in the list are separated by '|', with a trailing '|' as well
		-- 1st , delete that trailing delimiter_where_condition
		set @condition_list=left(@condition_list,length(@condition_list)-length(@delimiter_where_condition));
		-- 2nd, replace all '|' to be word of 'and'
		set @condition_list=replace(@condition_list,@delimiter_where_condition,' and ');
		-- 3rd, add 'WHERE' in the front
        set @where_statement=concat(' WHERE ',@condition_list);
    END IF;
    
    -- FINALLY, to create the sql query~
	set @sql =concat ('
		Select GEO, Year, Month, Products, Measurement, Price, Status
		from `',schema_name, '`.`', table_name, '` ',
		@where_statement,';'
		);   

	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
	set test=@sql;
    select @test;
END &&  
DELIMITER ;   

-- ----------------------------------------------------------------------------------
-- lets test the SP:
-- call webvoir.sp_reporting_1_price_by_year_month_geo_category ('webvoir', '1.report',',',' 1995 ,543,5009','12,7','canada','food',@test_result);
-- I added space around year number 1995
-- there was a number of 543 for year parameter
-- the month parameter are multiple values
-- the country name has a typo, the initial letter is not in upper case
-- there is only main category of 'food' is stated 
-- but, sp still construct a workable and correct sql query as below:
/*

		Select GEO, Year, Month, Products, Measurement, Price, Status
		from `webvoir`.`1.report`  WHERE  year in ("1995","1995","2022") and  month in ("12","7") and  lower(GEO) in ("canada") and  status in ("1");

*/
-- lets test if no parameter except for schema/table name is given;
-- call webvoir.sp_reporting_1_price_by_year_month_geo_category ('webvoir', '1.report',',','','','','',@test_result);
-- again , the sp gives the correct result
/*
		Select GEO, Year, Month, Products, Measurement, Price, Status
		from `webvoir`.`1.report`  ;
*/
-- select @test_result;
#### this is not finished #####
-- after we build the report and get the result, we could 
-- export the report to s3 where the static content of my website resides



-- ----------------------------------------------------------------------------------
-- 2. to create child SP
-- 2.1 as the reports will use aggregation a lot
-- create a specific SP for this purpose

-- call webvoir.sp_reporting_99_aggregation("webvoir","1.report","max","year"," ",@latest_year);
-- call webvoir.sp_reporting_99_aggregation("webvoir","1.report","min","year","",@earliest_year);
-- select @latest_year;
DROP PROCEDURE IF EXISTS webvoir.sp_reporting_99_aggregation;
DELIMITER &&  
CREATE PROCEDURE webvoir.sp_reporting_99_aggregation (
	IN schema_name varchar(50),
	IN table_name varchar(50),
    IN aggregation varchar(50),
    IN aggr_column varchar(50),
    IN where_clause varchar(255),
    out result_aggregation varchar(100)
    )    
BEGIN 
	set @result_aggre='';
	IF where_clause is null THEN		
		set @sql = concat(' 
		select ', aggregation, '(', aggr_column, ') into @result_aggre 
		from `',schema_name, '`.`',table_name,'`;');
		PREPARE dynamic_statement FROM @sql;
		EXECUTE dynamic_statement;
		DEALLOCATE PREPARE dynamic_statement;
	ELSE 
		set @sql = concat(' 
		select ', aggregation, '(', aggr_column, ') into @result_aggre 
		from `',schema_name, '`.`',table_name,'` ',
        where_clause,';');
		PREPARE dynamic_statement FROM @sql;
		EXECUTE dynamic_statement;
		DEALLOCATE PREPARE dynamic_statement;
    END IF;
    
	set result_aggregation=@result_aggre;
    select @result_aggregation;
END &&  
DELIMITER ; 
-- ----------------------------------------------------------------------------------
-- 2.3 to create sp for geo_limit
-- in order to make this SP more flexible 
-- multiple values in a string is accepted for the parameter of 'value_s';
-- also, the SP is working as the delimiter in a string, we set it ',' by default
-- there might be other possibilities, if you d like to make the sp more versatile
-- can try make every sp be acceptable for delimiter parameter

-- call webvoir.sp_reporting_99_value_exists("webvoir","1.report","geo","CANADA",",",0,@value_exists,@where_clause_for_geo);
-- select @value_exists; select @where_clause_for_geo;
DROP PROCEDURE IF EXISTS webvoir.sp_reporting_99_value_exists;
DELIMITER &&  
CREATE PROCEDURE webvoir.sp_reporting_99_value_exists (
	IN schema_name varchar(50),
	IN table_name varchar(50),
    IN column_name varchar(50),
    IN value_s varchar(255),
    IN delimiter_value varchar(10),
    IN case_sensitive boolean,
    out result_value_exists varchar(100),
    OUT where_clause_ready_to_use varchar(255)
    )    
BEGIN 
	
	-- get each value not-trimmed, in lower case, using loop
	set @value_s =value_s;
    set @value_s =concat(@value_s,delimiter_value); 
    IF case_sensitive = 0 THEN
		-- make all values to be compared in lower case
        set @value_s=lower(@value_s);
        -- remember to lower the values in the table as well
	END IF;
    select @value_s;
	-- attention: there will be no space between concat and '('
    -- otherwise: mysql can't run the concat function and @var become ''.
    set @count_value=LENGTH(@value_s)-LENGTH(REPLACE(@value_s, delimiter_value,''));
    set @count =1;
    set @start_position=1;
    set @value_list = ''; 
    -- this variable is to collect all value into a string
    -- attention: if we set @value_list =NUll, the result will null after concat any string
    -- we need to set it '', or ','
    value_loop: LOOP
		WHILE @count <= @count_value DO
			-- @start_position marks the point where locate function starts
			set @single_value_string=substring(@value_s,@start_position,locate(delimiter_value,@value_s,@start_position)-@start_position);
			set @start_position_new=locate(delimiter_value,@value_s,@start_position)+1 ;
			set @start_position=@start_position_new;
			-- after we get one single value string
            set @single_value_string=trim(@single_value_string);
            select @single_value_string;
			set @value_list=concat(@value_list,@single_value_string,delimiter_value);
            select @value_list;
			-- attention: there is a trailing delimiter_value (comma) in the string
			set @value_list= left(@value_list,length(@value_list)-length(delimiter_value));
			set @count=@count+1;
		END WHILE;
        LEAVE value_loop;
	END LOOP value_loop;
    
    set @result_value_exist='';
    set @delimiter_with_quotes=concat('"',delimiter_value,'"');
    IF case_sensitive = 0 THEN
		set @sql = concat(' 
		select count(*) into @result_value_exist 
		from `',schema_name, '`.`',table_name,'` 
		WHERE lower(', column_name, ') in ("', replace(@value_list,delimiter_value,@delimiter_with_quotes),'")');
        set where_clause_ready_to_use=concat (' lower(', column_name, ') in ("', replace(@value_list,delimiter_value,@delimiter_with_quotes),'")');
	ELSE 
		set @sql = concat(' 
		select count(*) into @result_value_exist 
		from `',schema_name, '`.`',table_name,'` 
		WHERE `', column_name, '` in ("', replace(@value_list,delimiter_value,@delimiter_with_quotes),'")');	
		set where_clause_ready_to_use=concat (' `',column_name, '` in ("', replace(@value_list,delimiter_value,@delimiter_with_quotes),'")');

	END IF;
	select @sql;
	PREPARE dynamic_statement FROM @sql;
	EXECUTE dynamic_statement;
	DEALLOCATE PREPARE dynamic_statement;
    
    IF @result_value_exist = 0 THEN
		-- no such value
        set result_value_exists=0;
	ELSE 
		set result_value_exists=1;
	END IF;
	select @where_clause_ready_to_use;
    select @result_value_exists;
END &&  
DELIMITER ; 
-- ----------------------------------------------------------------------------------



