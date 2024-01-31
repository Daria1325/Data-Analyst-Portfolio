/*
Covid 19 Data Exploration 
*/

-- Create tables and import data from csv files

CREATE TABLE IF NOT EXISTS public.covid_death
(
    iso_code text COLLATE pg_catalog."default",
    continent text COLLATE pg_catalog."default",
    location text COLLATE pg_catalog."default",
    date date,
    population real,
    total_cases numeric,
    new_cases numeric,
    new_cases_smoothed numeric,
    total_deaths numeric,
    new_deaths numeric,
    new_deaths_smoothed numeric,
    total_cases_per_million numeric,
    new_cases_per_million numeric,
    new_cases_smoothed_per_million numeric,
    total_deaths_per_million numeric,
    new_deaths_per_million numeric,
    new_deaths_smoothed_per_million numeric,
    reproduction_rate numeric,
    icu_patients numeric,
    icu_patients_per_million text COLLATE pg_catalog."default",
    hosp_patients numeric,
    hosp_patients_per_million numeric,
    weekly_icu_admissions numeric,
    weekly_icu_admissions_per_million numeric,
    weekly_hosp_admissions numeric,
    weekly_hosp_admissions_per_million numeric
)
COPY covid_vaccination FROM 'D:\Work\portfolio\Data exploration\covidDeaths.csv' DELIMITER ';' HEADER CSV;

CREATE TABLE IF NOT EXISTS public.covid_vaccination
(
    iso_code text COLLATE pg_catalog."default",
    continent text COLLATE pg_catalog."default",
    location text COLLATE pg_catalog."default",
    date date,
    total_tests numeric,
    new_tests numeric,
    total_tests_per_thousand numeric,
    new_tests_per_thousand numeric,
    new_tests_smoothed numeric,
    new_tests_smoothed_per_thousand numeric,
    positive_rate numeric,
    tests_per_case numeric,
    tests_units text COLLATE pg_catalog."default",
    total_vaccinations numeric,
    people_vaccinated numeric,
    people_fully_vaccinated numeric,
    total_boosters numeric,
    new_vaccinations numeric,
    new_vaccinations_smoothed numeric,
    total_vaccinations_per_hundred numeric,
    people_vaccinated_per_hundred numeric,
    people_fully_vaccinated_per_hundred numeric,
    total_boosters_per_hundred numeric,
    new_vaccinations_smoothed_per_million numeric,
    new_people_vaccinated_smoothed numeric,
    new_people_vaccinated_smoothed_per_hundred numeric,
    stringency_index numeric,
    population_density numeric,
    median_age text COLLATE pg_catalog."default",
    aged_65_older numeric,
    aged_70_older numeric,
    gdp_per_capita numeric,
    extreme_poverty numeric,
    cardiovasc_death_rate numeric,
    diabetes_prevalence numeric,
    female_smokers numeric,
    male_smokers numeric,
    handwashing_facilities numeric,
    hospital_beds_per_thousand numeric,
    life_expectancy numeric,
    human_development_index numeric,
    excess_mortality_cumulative_absolute numeric,
    excess_mortality_cumulative numeric,
    excess_mortality numeric,
    excess_mortality_cumulative_per_million numeric
)

COPY covid_vaccination FROM 'D:\Work\portfolio\Data exploration\covidVacinations.csv' DELIMITER ';' HEADER CSV;

-- Select Data that we are going to be starting with

SELECT location, date, total_cases, new_cases, total_deaths, population
FROM covid_death
ORDER BY location, date


-- Total Cases vs Total Deaths in Ukraine
    
SELECT location, date, total_cases, total_deaths, 
(total_deaths/total_cases)*100 as death_percentage
FROM covid_death
WHERE location = 'Ukraine' AND total_cases IS NOT NULL
ORDER BY location, date

    
-- Total Cases vs Population

SELECT location, date, total_cases, population, (total_cases/population)*100 as contracted_percentage
FROM covid_death
-- WHERE location = 'Ukraine' AND total_cases IS NOT NULL
ORDER BY date


-- Countries with Highest Infection Rate compared to Population
    
SELECT 
	location, 
	MAX(total_cases) AS highest_infection_count, 
	population, 
	MAX((total_cases/population))*100 as contracted_percentage
FROM covid_death
GROUP BY location, population
HAVING MAX(total_cases) IS NOT NULL
ORDER BY contracted_percentage DESC

    
-- Countries with Highest Death Count per Population
    
SELECT 
	location, 
	MAX(total_deaths) AS total_deaths_count
FROM covid_death
WHERE continent IS NOT NULL
GROUP BY location
HAVING MAX(total_deaths) IS NOT NULL
ORDER BY total_deaths_count DESC


-- Showing contintents with the highest death count per population
    
SELECT 
	location, 
	MAX(total_deaths) AS total_deaths_count
FROM covid_death
WHERE continent IS NULL AND location NOT LIKE '%income'
GROUP BY location
HAVING MAX(total_deaths) IS NOT NULL
ORDER BY total_deaths_count DESC


-- GLOBAL NUMBERS
    
SELECT  
	SUM(new_cases) as total_cases,
	SUM(new_deaths) as total_deaths,
	(SUM(new_deaths)/SUM(new_cases))*100 as death_percentage
FROM covid_death
WHERE continent IS NOT NULL


-- Total Population vs Vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine

SELECT 
	covid_death.continent,
	covid_death.location,
	covid_death.date,
	covid_death.population,
	covid_vaccination.new_vaccinations,
	SUM(covid_vaccination.new_vaccinations) 
	OVER (PARTITION BY covid_death.location ORDER BY covid_death.location, covid_death.date ) AS rolling_people_vaccinated
FROM covid_death
INNER JOIN covid_vaccination
ON covid_death.location = covid_vaccination.location AND covid_death.date =covid_vaccination.date
WHERE covid_death.continent IS NOT NULL
ORDER BY covid_death.location, covid_death.date


-- Using CTE to perform Calculation on Partition By in previous query
    
WITH pop_vs_vac(continent, location, date, population,new_vaccinations, rolling_people_vaccinated)
AS
(
	SELECT 
	covid_death.continent,
	covid_death.location,
	covid_death.date,
	covid_death.population,
	covid_vaccination.new_vaccinations,
	SUM(covid_vaccination.new_vaccinations) 
	OVER (PARTITION BY covid_death.location ORDER BY covid_death.location, covid_death.date ) AS rolling_people_vaccinated
FROM covid_death
INNER JOIN covid_vaccination
ON covid_death.location = covid_vaccination.location AND covid_death.date =covid_vaccination.date
WHERE covid_death.continent IS NOT NULL
--ORDER BY covid_death.location, covid_death.date
)
SELECT *, (rolling_people_vaccinated/population)*100 FROM pop_vs_vac


-- Using Temp Table to perform Calculation on Partition By in previous query
    
DROP TABLE IF EXISTS PercentPopulationVaccinated;

CREATE TEMPORARY TABLE PercentPopulationVaccinated
(
continent text,
location text,
date date,
population numeric,
new_vaccinations numeric,
rolling_people_vaccinated numeric
);


INSERT INTO PercentPopulationVaccinated
SELECT 
	covid_death.continent,
	covid_death.location,
	covid_death.date,
	covid_death.population,
	covid_vaccination.new_vaccinations,
	SUM(covid_vaccination.new_vaccinations) 
	OVER (PARTITION BY covid_death.location ORDER BY covid_death.location, covid_death.date ) AS rolling_people_vaccinated
FROM covid_death
INNER JOIN covid_vaccination
ON covid_death.location = covid_vaccination.location AND covid_death.date =covid_vaccination.date;
-- WHERE covid_death.continent IS NOT NULL
--ORDER BY covid_death.location, covid_death.date

SELECT *, (rolling_people_vaccinated/population)*100 
FROM PercentPopulationVaccinated


-- Creating View to store data for later visualizations
    
CREATE VIEW PercentPopulationVaccinated AS
SELECT 
	covid_death.continent,
	covid_death.location,
	covid_death.date,
	covid_death.population,
	covid_vaccination.new_vaccinations,
	SUM(covid_vaccination.new_vaccinations) 
	OVER (PARTITION BY covid_death.location ORDER BY covid_death.location, covid_death.date ) AS rolling_people_vaccinated
FROM covid_death
INNER JOIN covid_vaccination
ON covid_death.location = covid_vaccination.location AND covid_death.date =covid_vaccination.date
WHERE covid_death.continent IS NOT NULL
