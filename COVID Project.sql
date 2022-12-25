-- Data downloaded from https://ourworldindata.org/covid-deaths on 12-23-2022

SELECT *
FROM CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 3,4

--SELECT *
--FROM CovidVaccinations
--ORDER BY 3,4

SELECT location, date, total_cases, new_cases, total_deaths, population
FROM CovidDeaths
ORDER BY 1,2

-- Total Cases vs Total Deaths with Death Rate by Country (Running)
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS DeathRate
FROM CovidDeaths
--WHERE location LIKE '%states%'
ORDER BY 1,2

-- Total Cases vs Population with Infection Rate by Country (Running)
SELECT location, date, population, total_cases, (total_cases/population)*100 AS InfectionRate
FROM CovidDeaths
ORDER BY 1,2

-- Infection Rates by Country
SELECT location, population, MAX(total_cases) AS HighestInfectionCount, MAX((total_cases/population))*100 AS InfectionRate
FROM CovidDeaths
--WHERE location = 'United States'
GROUP BY location, population
ORDER BY InfectionRate DESC

-- Death Counts by Country
-- cast converts the nvarchar total_deaths into an integer
SELECT location, MAX(CAST(total_deaths as int)) AS TotalDeathCount
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY TotalDeathCount DESC

-- Death Counts by Continent
SELECT continent, MAX(CAST(total_deaths as int)) AS TotalDeathCount
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY TotalDeathCount DESC

-- Global Death Rate (Daily, Running)
SELECT date, SUM(new_cases) AS TotalCases, SUM(cast(new_deaths as int)) AS TotalDeaths, (SUM(cast(new_deaths as int))/SUM(new_cases))*100 AS GlobalDeathRate
FROM CovidDeaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1,2

-- Global Stats
SELECT SUM(new_cases) AS TotalCases, SUM(cast(new_deaths as int)) AS TotalDeaths, (SUM(cast(new_deaths as int))/SUM(new_cases))*100 AS GlobalDeathRate
FROM CovidDeaths
WHERE continent IS NOT NULL
--GROUP BY date
ORDER BY 1,2

-- Total Population vs Vaccinations
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
,SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingVaccinations
--, (RollingVaccinations/population)*100 AS PercentPopVaccinated
FROM CovidDeaths dea
JOIN CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
ORDER BY 2,3

-- USE CTE

WITH PopVsVacc (continent,location,date,population,new_vaccinations,RollingVaccinations)
AS
(
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
,SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingVaccinations
--, (RollingVaccinations/population)*100 AS PercentPopVaccinated
FROM CovidDeaths dea
JOIN CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
--ORDER BY 2,3
)
SELECT *, (RollingVaccinations/population)*100
FROM PopVsVacc

-- Use temp table

DROP TABLE IF EXISTS #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population int,
New_Vaccinations bigint,
Rolling_Vaccinations bigint
)

INSERT INTO #PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
,SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingVaccinations
--, (RollingVaccinations/population)*100 AS PercentPopVaccinated
FROM CovidDeaths dea
JOIN CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
--ORDER BY 2,3

SELECT *, (Rolling_Vaccinations/population)*100 AS PercentPopVaccinated
FROM #PercentPopulationVaccinated

-- Creating View to store data for later viz

CREATE VIEW PercentPopulationVaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
,SUM(CONVERT(bigint,vac.new_vaccinations)) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS RollingVaccinations
--, (RollingVaccinations/population)*100 AS PercentPopVaccinated
FROM CovidDeaths dea
JOIN CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
--ORDER BY 2,3

-- Investigating NULL values for new_vaccinations after receiving NULL conversion error message
--SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
--,SUM(CAST(vac.new_vaccinations as int)) OVER (PARTITION BY dea.location)
--FROM CovidDeaths dea
--JOIN CovidVaccinations vac
--	ON dea.location = vac.location
--	AND dea.date = vac.date
--WHERE dea.continent IS NOT NULL
--AND dea.date >= '2021-10-16'
--ORDER BY 2,3

-- Setting NULLs in new_vaccinations to zero
--UPDATE CovidVaccinations
--SET new_vaccinations = 0
--WHERE new_vaccinations IS NULL

-- Still got error msg 8115, researched and decided to increase width of the variable
--SELECT MAX(new_vaccinations)
--FROM CovidVaccinations

--SELECT CONVERT(bigint,new_vaccinations)
--FROM CovidVaccinations
-- found out new_vaccinations needs to be converted to big_int (line 65)

-- Confirming that a location's population remains static throughout the dataset
--SELECT location, population, AVG(population) AS avgpop
--FROM CovidDeaths
--GROUP BY population, location

--SELECT DISTINCT population, location
--FROM CovidDeaths
--ORDER BY 2
