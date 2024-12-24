CREATE TABLE public.dolomiti_superski_piste (
	nr varchar(50) NULL,
	"name" varchar(500) NULL,
	altitude_from integer NULL,
	altitude_to integer NULL,
	length integer NULL,
	lift_cat varchar(50) NULL,
	lift_nr varchar(50) NULL,
	lift_name varchar(50) NULL,
	slope_cat varchar(50) NULL,
	geoid varchar(50) NULL,
	geom varchar(50) NULL,
	rid integer NULL,
	pid integer NULL,
	"number" varchar(50) NULL,
	duration integer NULL,
	"slopeType" varchar(50) NULL,
	"regionId" integer NULL,
	skiline integer NULL
);

alter table dolomiti_superski_piste add column  id SERIAL PRIMARY KEY
alter table dolomiti_superski_piste add column embedding FLOAT[];

update dolomiti_superski_piste 
set embedding = pgml.embed('intfloat/e5-small-v2', jsonb_build_object(
    'metrics', jsonb_build_object(
      'height_diff', (altitude_from - altitude_to)::float / (SELECT MAX(altitude_from - altitude_to) FROM dolomiti_superski_piste),
      'length', length::float / (SELECT MAX(length) FROM dolomiti_superski_piste),
      'duration', duration::float / (SELECT MAX(duration) FROM dolomiti_superski_piste)
    ),
    'slope_category', slope_cat,
    'slope_type', "slopeType",
    'region', "regionId"
  )::text
); 

-- or

update dolomiti_superski_piste 
set embedding = pgml.embed('all-MiniLM-L6-v2', jsonb_build_object(
    'height_diff', (altitude_from - altitude_to),
    'length', length,
    'duration', duration,
    'slope_category', slope_cat,
    'slope_type', "slopeType",
    'region', "regionId"
  )::text
); 

-- or

update dolomiti_superski_piste 
set embedding = pgml.embed('all-MiniLM-L6-v2', jsonb_build_object(
	'height_diff', (altitude_from - altitude_to)::float / (SELECT MAX(altitude_from - altitude_to) FROM dolomiti_superski_piste),
	'length', length::float / (SELECT MAX(length) FROM dolomiti_superski_piste),
	'duration', duration::float / (SELECT MAX(duration) FROM dolomiti_superski_piste),
	'slope_cat', (case 
		when slope_cat = 'hard' then 3 
		when slope_cat = 'medium' then 2   
		when slope_cat = 'easy' then 1
		else -3			
	end)::float / 3,
	'slopeType', ((case 
		when "slopeType" = 'orange' then 4
		when "slopeType" = 'black' then 3 
		when "slopeType" = 'red' then 2   
		when "slopeType" = 'blue' then 1
		else -4			
	end)::float / 4),
	'regionId', "regionId"::float / (SELECT MAX("regionId") FROM dolomiti_superski_piste)	  )::text
); 


-- or (better)

update dolomiti_superski_piste 
set embedding = array[
	(altitude_from - altitude_to)::float / (SELECT MAX(altitude_from - altitude_to) FROM dolomiti_superski_piste),
	length::float / (SELECT MAX(length) FROM dolomiti_superski_piste),
	duration::float / (SELECT MAX(duration) FROM dolomiti_superski_piste),
	(case 
		when slope_cat = 'hard' then 3 
		when slope_cat = 'medium' then 2   
		when slope_cat = 'easy' then 1
		else -3			
	end)::float / 3,
	 ((case 
		when "slopeType" = 'orange' then 4
		when "slopeType" = 'black' then 3 
		when "slopeType" = 'red' then 2   
		when "slopeType" = 'blue' then 1
		else -4			
	end)::float / 4),
	"regionId"::float / (SELECT MAX("regionId") FROM dolomiti_superski_piste)	
	]::float[]
;

select * from (
select rank () over (partition by id order by angle asc) rank, * from (
select b.id, b.name, 
c.id id_other, c.name name_other,
(b.altitude_from - b.altitude_to) height_diff,
(c.altitude_from - c.altitude_to) height_diff_other,
b.length, c.length length_other,
b.duration, c.duration duration_other,
b.slope_cat, c.slope_cat slope_cat_other,
b."slopeType", c."slopeType" slopeType_other,
b."regionId", c."regionId" regionId_other,
b.embedding, c.embedding embedding_other,
pgml.distance_l2(b.embedding, c.embedding) angle from tracking a
join dolomiti_superski_piste b on a."PID" = b.rid
, dolomiti_superski_piste c
where b.id <> c.id 
and b.altitude_from is not null and c.altitude_from is not null
and b.altitude_to is not null and c.altitude_to is not null
and b.length is not null and c.length is not null
and b.duration is not null and c.duration is not null
and b.slope_cat is not null and c.slope_cat is not null
and b."slopeType" is not null and c."slopeType" is not null
and b."regionId" is not null and c."regionId" is not null
) d) e
where e.rank <= 3
order by id, rank 
 