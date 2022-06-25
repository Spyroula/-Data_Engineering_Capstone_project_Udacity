-- STAGING TABLES

CREATE TABLE IF NOT EXISTS public.immigration (
	i94addr varchar,
	cicid int4,
  	i94visa int4,
  	i94res int4,
  	arrdate varchar,
  	depdate varchar,
  	airline varchar,
  	fltno varchar,
  	i94mode int4,
  	i94port varchar,
  	visatype varchar,
  	gender varchar,
  	i94cit int4,
  	i94bir int4,
  	stay int4,
	CONSTRAINT immigration_pkey PRIMARY KEY ("cicid")
);

                                                      
CREATE TABLE IF NOT EXISTS public.country (
	Code int4,
	Country varchar,
	Temperature float,
	Latitude varchar,
	Longitude varchar,
	CONSTRAINT country_pkey PRIMARY KEY ("Code")
);

CREATE TABLE IF NOT EXISTS public.states (
	Code varchar,
	State varchar,
	BlackOrAfricanAmerican int8,
	TotalPopulation int8,
	White int8,
	ForeignBorn int8,
	AmericanIndianAndAlaskaNative int8,
	HispanicOrLatino int8,
	NumberVeterans int8,
	Asian int8,
	FemalePopulation int8,
	MalePopulation int8,
	CONSTRAINT state_pkey PRIMARY KEY ("Code")
);

CREATE TABLE IF NOT EXISTS public."date" (
	"date" varchar,
	"year" int4,
	"month" int4,
	"day" int4,
	weekofyear int4,
	dayofweek int4,
	CONSTRAINT date_pkey PRIMARY KEY ("date")
) ;
