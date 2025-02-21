Select* From ushouseholdincome limit 20;

CREATE TABLE `US_Household_Income` (
  `row_id` int DEFAULT NULL,
  `id` int DEFAULT NULL,
  `State_Code` int DEFAULT NULL,
  `State_Name` text,
  `State_ab` text,
  `County` text,
  `City` text,
  `Place` text,
  `Type` text,
  `Primary` text,
  `Zip_Code` int DEFAULT NULL,
  `Area_Code` int DEFAULT NULL,
  `ALand` int DEFAULT NULL,
  `AWater` int DEFAULT NULL,
  `Lat` double DEFAULT NULL,
  `Lon` double DEFAULT NULL,
  `TimeStamp` TIMESTAMP DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DELIMITER $$
CREATE PROCEDURE copy_and_clean_data ()
BEGIN
-- Creating Table
	CREATE TABLE `US_Household_Income` (
	  `row_id` int DEFAULT NULL,
	  `id` int DEFAULT NULL,
	  `State_Code` int DEFAULT NULL,
	  `State_Name` text,
	  `State_ab` text,
	  `County` text,
	  `City` text,
	  `Place` text,
	  `Type` text,
	  `Primary` text,
	  `Zip_Code` int DEFAULT NULL,
	  `Area_Code` int DEFAULT NULL,
	  `ALand` int DEFAULT NULL,
	  `AWater` int DEFAULT NULL,
	  `Lat` double DEFAULT NULL,
	  `Lon` double DEFAULT NULL,
	  `TimeStamp` TIMESTAMP DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Copying Data From The Old Table
	INSERT INTO US_Household_Income
    SELECT *, current_timestamp
    FROM ushouseholdincome ;
    
-- Data Cleaning Steps
	-- 1. Removing Duplicates
    DELETE FROM US_Household_Income
    WHERE row_id IN (
		SELECT row_id FROM (
			SELECT row_id, id, ROW_NUMBER () OVER (PARTITION BY id,`TimeStamp`) AS row_num
            FROM US_Household_Income) duplicate
            WHERE row_num > 1);
	-- 2. Standarization
    UPDATE US_Household_Income
    SET State_Name = 'Georgia'
    WHERE State_Name = 'georia' ;
	
    UPDATE US_Household_Income
    SET `Type` = 'CDP'
    WHERE `Type` = 'CPD';
    
    UPDATE US_Household_Income
    SET `Type` = 'Borough'
    WHERE `Type` = 'Borughs';
    
    UPDATE US_Household_Income
    SET Country = UPPER (Country);
    
    UPDATE US_Household_Income
    SET City = UPPER (City);
    
    UPDATE US_Household_Income
    SET Place = UPPER (Place);
    
    UPDATE US_Household_Income
    SET State_Name = UPPER (State_Name);
    
END $$
DELIMITER ;

-- Creating an Event

CREATE EVENT run_data_cleaning
ON SCHEDULE EVERY 30 DAY
DO CALL copy_and_clean_data ();

-- Creating Trigger
DELIMITER $$
CREATE TRIGGER transfer_clean_data 
AFTER INSERT ON US_Household_Income
FOR EACH ROW
BEGIN
CALL copy_and_clean_data ();
END $$
DELIMITER ;

