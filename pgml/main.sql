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
	0
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
join dolomiti_superski_piste_ok b on a."PID" = b.rid
, dolomiti_superski_piste_ok c
where b.id <> c.id 
) d) e
where e.rank <= 3
order by id, rank 


/* 
 * 1^ APPROCCIO (non AI)
 * 
 * Raccomandazioni basate sulla media
 */

CREATE VIEW dolomiti_superski_piste_ok
AS 
SELECT * FROM dolomiti_superski_piste b 
where b.altitude_from is not null
and b.altitude_to is not null
and b.length is not null
and b.duration is not null
and b.slope_cat is not null
and b."slopeType" is not null
and b."regionId" is not null


CREATE OR REPLACE FUNCTION get_recommendations_ni(
    not_skied_only BOOLEAN default true,
    num_recommendations INTEGER DEFAULT 3
) RETURNS TABLE (
    id INTEGER
) AS $$
BEGIN
    RETURN QUERY
	with 
	avg_piste as (
		select pgml.divide (pgml.sum (embedding), count(*)) as embedding  from tracking a 
		join dolomiti_superski_piste_ok b on a."PID" = b.rid
	),
	ranked as (
		select b.*, pgml.distance_l2(a.embedding, b.embedding) "rank" from avg_piste a, dolomiti_superski_piste_ok b
		where not not_skied_only or b.id not in (select d.id from tracking c 
			join dolomiti_superski_piste_ok d on c."PID" = d.rid)
	)
	select a.id from ranked a
	order by a."rank"
	limit num_recommendations;
end;
$$ LANGUAGE plpgsql


select * from dolomiti_superski_piste a
where a.id in (select get_recommendations_ni ())


/* 
 * 2^ APPROCCIO (AI)
 * 
 * Raccomandazioni basate sul clustering
 */

create view pgml.dolomiti_superski_piste_training
as select embedding from public.dolomiti_superski_piste_ok

SELECT pgml.train(
    project_name => 'dolomiti_superski_piste_training',  -- project name
    task => 'clustering',                     -- task
    algorithm => 'kmeans',                            -- algorithm
    relation_name => 'pgml.dolomiti_superski_piste_training',  -- target relation
    y_column_name => NULL,                                -- y_column_name (not needed for clustering)
    hyperparams => '{"n_clusters": 10}'::JSONB           -- hyperparameters as JSONB
);

select * from pgml.projects 
select * from pgml.models
--SELECT * FROM pgml.deploy(6);
select * from pgml.deployments 

CREATE OR REPLACE FUNCTION get_recommendations_ai_clustering(
    not_skied_only BOOLEAN default true,
    num_recommendations INTEGER DEFAULT 3
) RETURNS TABLE (
    id INTEGER
) AS $$
BEGIN
    RETURN QUERY
	with 
	all_slopes as (
		SELECT 
	    	a.*,
		    pgml.predict('dolomiti_superski_piste_training', a.embedding) as cluster_id
		FROM public.dolomiti_superski_piste_ok a
	),
	skied_slopes as (
		select * from (
	    select *, max(cluster_count) over () as max_count from (
		select *, count (*) over (partition by cluster_id) cluster_count from (
		SELECT 
	    	c.*,
		    pgml.predict('dolomiti_superski_piste_training', c.embedding) as cluster_id	    
		FROM tracking b 
			join dolomiti_superski_piste_ok c on b."PID" = c.rid) d) e) f
		where cluster_count = max_count
	)
	select a.id from all_slopes a
	where a.cluster_id = (select distinct b.cluster_id from skied_slopes b)
	and (not not_skied_only or a.id not in (select d.id from tracking c 
				join dolomiti_superski_piste_ok d on c."PID" = d.rid))
	ORDER BY RANDOM()  -- o ordina per una metrica di similarità
	limit num_recommendations;
end;
$$ LANGUAGE plpgsql


select * from dolomiti_superski_piste a
where a.id in (select get_recommendations_ai_clustering ())


select slope_cat, count (*) from tracking c 
				join dolomiti_superski_piste_ok d on c."PID" = d.rid			
group by slope_cat


/* 
 * 3^ APPROCCIO (AI)
 * 
 * Raccomandazioni basate sulle abitudini
 */
select embedding from dolomiti_superski_piste 

--drop view pgml.dolomiti_superski_piste_habit 
create or replace view pgml.dolomiti_superski_piste_habit  as
select b.embedding [1] as data1, b.embedding [2]  as data2, b.embedding [3]  as data3, b.embedding [4] as data4, b.embedding [5] as data5, b.embedding [6] as data6, 1 as skied 
from public.tracking a 
join public.dolomiti_superski_piste_ok b on a."PID" = b.rid;

SELECT * FROM pgml.train('dolomiti_superski_piste_habit', 'regression', 'pgml.dolomiti_superski_piste_habit', 'skied');

select pgml.predict('dolomiti_superski_piste_habit', embedding) as prediction_score, 
	(select case when cnt = 0 then 0 else 1 end score from (select count (*) cnt from tracking b where b."PID" = a.rid) c) score, 
	* from dolomiti_superski_piste_ok a

			