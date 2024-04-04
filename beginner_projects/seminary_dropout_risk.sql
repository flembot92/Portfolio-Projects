/*
Name: Dropout Risk Project (Seminary)
Author: Brian Fleming
Date: 4/10/2023
Description: The Dropout Risk Project seeks to identify attendance and demographic variables that may influence a student's risk  of dropping out.
This script will produce the desired first trial dataset for seminary students enrolled in the US the past four years.

Working definition of a dropout (Dropout_YN_Flag): the student was enrolled anytime in the past four years (and was supposed to be enrolled according to their grade),
but is not enrolled in the current term (and should be currently enrolled).

*/

USE EducationDW;
-- This first CTE is getting all the student info and conditions we need
WITH main AS(
	SELECT
		 f.STUDENT_KEY
		,s.GENDER								AS Student_Gender
		,st.MEMBERSHIP_YN_FLAG
		,st.AGE_IN_YEARS						AS Student_Age
		,s.PRIMARY_LANGUAGE
		,st.YEAR_IN_SEMINARY
		,s.SEMINARY_GRADUATION_YEAR -- STUDENT_TERM_FACT.GRADUATED_COUNT seems risky, some students have it showing up more than once
		,c.CLASS_KEY
		,c.COURSE_TITLE
		,c.TIME_OF_DAY_DESCRIPTION
		,e.GENDER								AS Teacher_Gender
		,YEAR(GETDATE()) - YEAR(e.BIRTH_DATE)	AS Teacher_Age
		,t.TERM_KEY
		,t.TERM_NUMBER
		,t.SCHOOL_YEAR_NAME
		,p.PROGRAM_NAME
		,CASE
			WHEN PROGRAM_NAME LIKE '%release%' THEN 'Y'
			ELSE 'N'
		END AS Released_Time_YN_Flag
		,p.POLITICAL_PLACE_NAME				AS State
		,p.POLITICAL_PLACE_COUNTRY			AS County
		,p.POLITICAL_PLACE_CONTINENT		AS Continent
		,f.ABSENT_COUNT
		,c.CLASS_SESSION_COUNT
		,f.FIRST_ATTENDED_CLASS_COUNT
		,f.ATTENDED_COUNT
		,f.PHYSICALLY_PRESENT_COUNT
		,f.RECEIVED_CREDIT_COUNT
		,f.DROPPED_COUNT
		,ROW_NUMBER() OVER (PARTITION BY f.STUDENT_KEY, f.TERM_KEY ORDER BY f.STUDENT_KEY) AS rownumber -- anything above '1' tells us the student was registered for multiple classes within a term
		,CASE
			WHEN t.SCHOOL_YEAR_NAME = '2019 (19-20)' AND st.YEAR_IN_SEMINARY = 1 THEN 'Y'
			WHEN t.SCHOOL_YEAR_NAME = '2020 (20-21)' AND st.YEAR_IN_SEMINARY BETWEEN 1 AND 2 THEN 'Y'
			WHEN t.SCHOOL_YEAR_NAME = '2021 (21-22)' AND st.YEAR_IN_SEMINARY BETWEEN 1 AND 3 THEN 'Y' 
			WHEN t.SCHOOL_YEAR_NAME = '2022 (22-23)' AND st.YEAR_IN_SEMINARY BETWEEN 1 AND 4 THEN 'Y' 
			ELSE 'N'
		 END AS Relevant_YN_Flag -- tells us the student should be enrolled in seminary during 2019-2023; per line 107, ultimately we only want records that are relevant
	FROM STUDENT_CLASS_FACT f
		INNER JOIN STUDENT_DIM s						ON s.STUDENT_KEY = f.STUDENT_KEY
		INNER JOIN STUDENT_TYPE_DIM st					ON st.STUDENT_TYPE_KEY = f.BUSINESS_STUDENT_TYPE_KEY
		INNER JOIN CLASS_DIM c							ON c.CLASS_KEY = f.CLASS_KEY
		INNER JOIN TERM_DIM t							ON t.TERM_KEY = f.TERM_KEY
		INNER JOIN PROGRAM_DIM p						ON p.PROGRAM_KEY = f.PROGRAM_KEY
		INNER JOIN STATUS_DIM x							ON x.STATUS_KEY = f.STATUS_KEY and x.DELETED_YN_FLAG = 'N'
		INNER JOIN EMPLOYEE_DIM e						ON e.EMPLOYEE_ID = c.INSTRUCTOR_EMPLOYEE_ID
	WHERE 1=1
		AND x.ENROLLED_YN_FLAG = 'Y'
		AND p.PROGRAM_TYPE_GROUP_CODE = 'SEM'
		AND SCHOOL_YEAR_NAME BETWEEN '2019 (19-20)' AND '2022 (22-23)'
		AND p.POLITICAL_PLACE_COUNTRY = 'United States' -- in other iterations, can use a different country or continent, or just remove this condition to use globally
		AND s.SEMINARY_GRADUATION_YEAR >= 2023
	)
-- The second CTE should simply be grabbing the students who are currently enrolled
,perm AS(
	SELECT f.STUDENT_KEY
	FROM STUDENT_TERM_FACT f
		INNER JOIN TERM_DIM t							ON t.TERM_KEY=f.TERM_KEY
		INNER JOIN STATUS_DIM x							ON x.STATUS_KEY = f.STATUS_KEY and x.DELETED_YN_FLAG = 'N'
		INNER JOIN STUDENT_TYPE_DIM st					ON st.STUDENT_TYPE_KEY = f.BUSINESS_STUDENT_TYPE_KEY
		INNER JOIN PROGRAM_DIM p						ON p.PROGRAM_KEY = f.PROGRAM_KEY
	WHERE 1=1
		AND t.CURRENT_TERM_YN_FLAG = 'Y'
		AND x.ENROLLED_YN_FLAG = 'Y'
		AND p.POLITICAL_PLACE_COUNTRY = 'United States'
	)
-- Now the goal is to get all students from the "main" CTE, and flag them if they are not currently enrolled according to the "perm" CTE (i.e. they have dropped out)
SELECT --TOP (100)
	*
	,SUM(ABSENT_COUNT) OVER (PARTITION BY main.STUDENT_KEY, TERM_KEY ORDER BY TERM_KEY)			AS Absences_per_Term -- totals all absences within a term
	,SUM(ATTENDED_COUNT) OVER (PARTITION BY main.STUDENT_KEY, TERM_KEY ORDER BY TERM_KEY)		AS Attended_per_Term -- totals all attended sessions within a term
--	,(ATTENDED_COUNT/CLASS_SESSION_COUNT)*100													AS Attendance_Percentage -- only producing 0 or 100. Also problematic for dropped classes
	,CASE
		WHEN main.STUDENT_KEY NOT IN (SELECT STUDENT_KEY FROM perm) THEN 'Y'
		ELSE 'N'
	 END AS Dropout_YN_Flag
	,CASE
		WHEN perm.STUDENT_KEY IS NULL AND MAX(YEAR_IN_SEMINARY) OVER (PARTITION BY main.STUDENT_KEY)=1 AND MAX(TERM_NUMBER) OVER (PARTITION BY main.STUDENT_KEY,SCHOOL_YEAR_NAME)=1 THEN 'Mid-Freshman'
		WHEN perm.STUDENT_KEY IS NULL AND MAX(YEAR_IN_SEMINARY) OVER (PARTITION BY main.STUDENT_KEY)=1 AND MAX(TERM_NUMBER) OVER (PARTITION BY main.STUDENT_KEY,SCHOOL_YEAR_NAME)=2 THEN 'Post-Freshman'
		WHEN perm.STUDENT_KEY IS NULL AND MAX(YEAR_IN_SEMINARY) OVER (PARTITION BY main.STUDENT_KEY)=2 AND MAX(TERM_NUMBER) OVER (PARTITION BY main.STUDENT_KEY,SCHOOL_YEAR_NAME)=1 THEN 'Mid-Sophomore'
		WHEN perm.STUDENT_KEY IS NULL AND MAX(YEAR_IN_SEMINARY) OVER (PARTITION BY main.STUDENT_KEY)=2 AND MAX(TERM_NUMBER) OVER (PARTITION BY main.STUDENT_KEY,SCHOOL_YEAR_NAME)=2 THEN 'Post-Sophomore'
		WHEN perm.STUDENT_KEY IS NULL AND MAX(YEAR_IN_SEMINARY) OVER (PARTITION BY main.STUDENT_KEY)=3 AND MAX(TERM_NUMBER) OVER (PARTITION BY main.STUDENT_KEY,SCHOOL_YEAR_NAME)=1 THEN 'Mid-Junior'
		WHEN perm.STUDENT_KEY IS NULL AND MAX(YEAR_IN_SEMINARY) OVER (PARTITION BY main.STUDENT_KEY)=3 AND MAX(TERM_NUMBER) OVER (PARTITION BY main.STUDENT_KEY,SCHOOL_YEAR_NAME)=2 THEN 'Post-Junior'
		WHEN perm.STUDENT_KEY IS NULL AND MAX(YEAR_IN_SEMINARY) OVER (PARTITION BY main.STUDENT_KEY)=4 AND MAX(TERM_NUMBER) OVER (PARTITION BY main.STUDENT_KEY,SCHOOL_YEAR_NAME)=1 THEN 'Mid-Senior'
		ELSE 'None'
	END AS Point_of_Dropout -- this tells us at what point in their high school tenure they dropped out (or didn't enrolled after having been enrolled previously)
	-- using perm.STUDENT_KEY IS NULL above is just another way of saying Dropout_YN_Flag='Y' in this case statement, because it can't reference the field Dropout_YN_Flag
FROM main
LEFT JOIN perm				ON perm.STUDENT_KEY = main.STUDENT_KEY
WHERE main.Relevant_YN_Flag = 'Y'
--AND main.student_key IN (3006630, 3803639) -- these students keys are useful to test out assumptions
ORDER BY main.STUDENT_KEY, SCHOOL_YEAR_NAME, TERM_NUMBER, CLASS_KEY -- this can also be useful to test out assumptions


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/*

 NOTES FOR THE FUTURE
-- If running this during the 2023-2024 academic year, move the years up by one in lines 64 and 66
-- Change line 65 to another country or continent, or just remove to analyze globally. And make sure line 79 matches the location you're using in line 65

NOTES FOR POWER BI
-- Attendance_Percentage has never calculated correctly in SQL for me, so I would create it as a new column in Power BI when Transforming Data
-- Rename/alias any column titles in whatever way makes sense
-- Relevant_YN_Flag and the second STUDENT_KEY column can be deleted
-- I would also change the number values in YEAR_IN_SEMINARY to Freshman, Sophomore, etc.
-- I would also change the number values to Y/N in RECEIVED_CREDIT_COUNT and DROPPED COUNT

-- Use this DAX formula to calculate Dropout Percentage Rate in Power BI (apply it to states, gender, etc.):

Dropout Percentage Rate = 
VAR X = CALCULATE(DISTINCTCOUNT('Dropout Risk Trial (US 19-23)'[STUDENT_KEY]), 'Dropout Risk Trial (US 19-23)'[Dropout_YN_Flag]="Y")
VAR Y = CALCULATE(DISTINCTCOUNT('Dropout Risk Trial (US 19-23)'[STUDENT_KEY]))
RETURN X/Y

CURRENT CONCERNS
-- Track point of dropout throughout high school tenure
-- Why are there so few Seniors in Year_in_Seminary?
-- Dropout_YN_Flag has a low level granularity. Each student has either all Y or all N. That field does not allow for analysis of higher granularity, such as time of day or teacher's gender
-- Another solution might be to order each student's rows chronologically and us LAG or LEAD to tell when they drop out

*/
