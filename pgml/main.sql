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
(select b.embedding [1] as data1, b.embedding [2]  as data2, b.embedding [3]  as data3, b.embedding [4] as data4, b.embedding [5] as data5, 1 as skied from dolomiti_superski_piste_ok b where b.rid in (select a."PID" from tracking a)
limit 200)
union
(select b.embedding [1] as data1, b.embedding [2]  as data2, b.embedding [3]  as data3, b.embedding [4] as data4, b.embedding [5] as data5, 0 as skied from dolomiti_superski_piste_ok b where b.rid not in (select a."PID" from tracking a)
limit 200);

select * from dolomiti_superski_piste_habit;

select * from pgml.projects ;
select * from pgml.models
SELECT * FROM pgml.deploy(26);
select * from pgml.deployments 

delete from pgml.projects where id = 13;
delete from pgml.models where project_id = 13;
delete from pgml.deployments where project_id = 13;

SELECT * FROM pgml.train('dolomiti_superski_piste_habit', 'regression', 'pgml.dolomiti_superski_piste_habit', 'skied');

CREATE OR REPLACE FUNCTION get_recommendations_ai_habit(
    not_skied_only BOOLEAN default true,
    num_recommendations INTEGER DEFAULT 3
) RETURNS TABLE (
    id INTEGER
) AS $$
BEGIN
    RETURN QUERY
	select b.id from (
	select a.id, pgml.predict('dolomiti_superski_piste_habit', embedding) as score
		from dolomiti_superski_piste_ok a
		ORDER BY 2 desc
	) b
	where (not not_skied_only or b.id not in (select d.id from tracking c 
				join dolomiti_superski_piste_ok d on c."PID" = d.rid))
    and score between 0.7 and 1.3
	limit num_recommendations;
end;
$$ LANGUAGE plpgsql

select * from dolomiti_superski_piste a
where a.id in (select get_recommendations_ai_habit ())
			

/* ----------------------------------------------
 * 
 * 					NEW DATABASE
 * 
 * ---------------------------------------------- 
 */ 

create table public."document" (
	id serial,
	name varchar(256),
	content jsonb
)

-- docker cp data\pistebasis.json c101b5be8b3f:/tmp/
insert into public."document" (name, "content") values ('pistebasis', pg_read_file('/tmp/pistebasis.json')::jsonb)
-- docker cp data\talschaften.json c101b5be8b3f:/tmp/
insert into public."document" (name, "content") values ('talschaften', pg_read_file('/tmp/talschaften.json')::jsonb)

GRANT EXECUTE ON FUNCTION pg_read_file(text) TO postgresml

--drop view activity;
create view activity as
select persona_guid 
	, (track ->> 'id')::int id
	, track ->> 'skiAreaId' ski_area_id
	, track ->> 'type' type
	, (track ->> 'start')::timestamp start
	, (track ->> 'end')::timestamp end
	, (track ->> 'distance')::float distance
	, (track ->> 'speed')::float speed
	, (track ->> 'completionPercentage')::int completion_percentage
from  (select persona_guid, jsonb_path_query (slopes_and_lifts::jsonb, '$[*]') track from tracking) a

select * from activity 

--drop view slope_valid
--drop view slope;
create or replace view slope as
select (slope ->> 'state')::int state
, (slope ->> 'rid')::int rid
, (slope ->> 'pid')::int pid
, slope ->> 'slopeType' slope_type
, (slope -> 'skiresort' ->> 'pid')::int ski_resort_pid
, slope ->> 'slopetype' difficulty
, slope ->> 'regionId' region_id
, slope -> 'name' ->> 'it' name
, slope -> 'description' ->> 'it' description
, (slope -> 'data' ->> 'length')::int length
, (slope -> 'data' -> 'altitude' ->> 'start')::int start
, (slope -> 'data' -> 'altitude' ->> 'end')::int end
, (slope -> 'data' ->> 'height-difference')::int height_difference
, (case when slope ->> 'duration' = '' then null else slope ->> 'duration' end)::int duration
, (slope -> 'location' ->> 'lat')::float latitude
, (slope -> 'location' ->> 'lon')::float longitude
from (
select jsonb_path_query (content, '$.items[*]') as slope from document where name = 'pistebasis') a

select * from slope

create view region as 
select region ->> 'rid' region_id
, region -> 'name' ->> 'it' name
from (
select jsonb_path_query (content, '$.items[*]') as region from document where name = 'talschaften') a




 /* 
 * 1^ APPROCCIO (non AI)
 * 
 * Raccomandazioni basate sulla media
 */


--drop function slope_metric_data (slope_valid)
--drop view slope_valid
CREATE table slope_valid
AS 
SELECT * FROM slope b 
where b.start is not null
and b.duration is not null
and b.end is not null
and b.height_difference is not null
and b.length is not null
and b.difficulty is not null
and b.slope_type is not null;

create index slope_valid_idx01 on slope_valid (pid)

CREATE OR REPLACE FUNCTION slope_metric_data (slope slope_valid)
RETURNS float[]
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN array[
        slope.height_difference::float / (SELECT MAX(height_difference) FROM slope_valid),
        slope.length::float / (SELECT MAX(length) FROM slope_valid),
        slope.duration::float / (SELECT MAX(duration) FROM slope_valid),
        (CASE
            WHEN slope.difficulty = 'hard' THEN 3
            WHEN slope.difficulty = 'medium' THEN 2
            WHEN slope.difficulty = 'easy' THEN 1
            ELSE -3
        END)::float / 3,
        (CASE
            WHEN slope.slope_type = 'orange' THEN 4
            WHEN slope.slope_type = 'black' THEN 3
            WHEN slope.slope_type = 'red' THEN 2
            WHEN slope.slope_type = 'blue' THEN 1
            ELSE -4
        END::float / 4),
        0
    ]::float[];
END;
$$;

alter table slope_valid add column metric_data FLOAT[];
update slope_valid a set metric_data = slope_metric_data (a.*);

DROP FUNCTION get_recommendations_ni(text,boolean,integer) 

CREATE OR REPLACE FUNCTION get_recommendations_ni(
	guid text,
    not_skied_only BOOLEAN default true,
    usual_region BOOLEAN default false,
    num_recommendations INTEGER DEFAULT 3
) RETURNS TABLE (
    id INTEGER
) AS $$
BEGIN
    RETURN QUERY
	with 
	avg_piste as (
		select pgml.divide (pgml.sum (b.metric_data), count(*)) as metric_data  from activity a 
		join slope_valid b on a.id = b.pid
        where a.persona_guid = guid
	),
	ranked as (
		select b.*, pgml.distance_l2(a.metric_data, b.metric_data) "rank" from avg_piste a, slope_valid b
		where (not not_skied_only or b.pid not in (select d.pid from activity c 
			join slope_valid d on c.id = d.pid))
		and (not usual_region or b.region_id  = (select f.region_id from (
			select e.region_id, e.cnt, max (e.cnt) over (partition by e.region_id) max_cnt from (
			select d.region_id, count (*) cnt from slope_valid d
			where d.pid in (select c.id from activity c where c.persona_guid = guid)
			group by d.region_id) e) f
			where f.cnt = f.max_cnt
			limit 1))
	)
	select a.pid from ranked a
	order by a."rank"
	limit num_recommendations;
end;
$$ LANGUAGE plpgsql


select * from slope_valid a
where a.pid in (select get_recommendations_ni ('811767B26C8824B4E053898FBA2534D5', true, true))

select d.* from activity c 
	join slope_valid d on c.id = d.pid
	where c.persona_guid = '298EE2A3BD1F3287E063898FBA255E9B'
	
	
/* 
 * 2^ APPROCCIO (AI)
 * 
 * Raccomandazioni basate sul clustering
 */

create view pgml.slope_valid_cluster
as select metric_data from public.slope_valid

SELECT pgml.train(
    project_name => 'slope_cluster',  -- project name
    task => 'clustering',                     -- task
    algorithm => 'kmeans',                            -- algorithm
    relation_name => 'pgml.slope_valid_cluster',  -- target relation
    y_column_name => NULL,                                -- y_column_name (not needed for clustering)
    hyperparams => '{"n_clusters": 10}'::JSONB           -- hyperparameters as JSONB
);

drop function get_recommendations_ai_clustering (BOOLEAN, INTEGER);

drop function get_recommendations_ai_clustering (text, BOOLEAN, INTEGER)

CREATE OR REPLACE FUNCTION get_recommendations_ai_clustering(
	guid text,
    not_skied_only BOOLEAN default true,
    usual_region BOOLEAN default false,    
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
		    pgml.predict('slope_cluster', a.metric_data) as cluster_id
		FROM public.slope_valid a
	),
	skied_slopes as (
		select * from (
	    select *, max(cluster_count) over () as max_count from (
		select *, count (*) over (partition by cluster_id) cluster_count from (
		SELECT 
	    	c.*,
		    pgml.predict('slope_cluster', c.metric_data) as cluster_id	    
		FROM activity b 
			join slope_valid c on b.id = c.pid
		where b.persona_guid = guid) d) e) f
		where cluster_count = max_count
	)
	select a.pid from all_slopes a
	where a.cluster_id = (select distinct b.cluster_id from skied_slopes b)
	and (not not_skied_only or a.pid not in (select d.pid from activity c 
		join slope_valid d on c.id = d.pid))
	and (not usual_region or a.region_id  = (select f.region_id from (
		select e.region_id, e.cnt, max (e.cnt) over (partition by e.region_id) max_cnt from (
		select d.region_id, count (*) cnt from slope_valid d
		where d.pid in (select c.id from activity c where c.persona_guid = guid)
		group by d.region_id) e) f
		where f.cnt = f.max_cnt
		limit 1))
	ORDER BY RANDOM()  -- o ordina per una metrica di similarità
	limit num_recommendations;
end;
$$ LANGUAGE plpgsql


select a.* from slope_valid a
where a.pid in (select get_recommendations_ai_clustering ('811767B26C8824B4E053898FBA2534D5'))


select pgml.predict('slope_cluster', a.metric_data), 'suggested' from slope_valid a
where a.pid in (select get_recommendations_ni ('811767B26C8824B4E053898FBA2534D5'))
union all
select pgml.predict('slope_cluster', a.metric_data), 'skied' from slope_valid a
where a.pid in (select id from activity where persona_guid = '811767B26C8824B4E053898FBA2534D5')


select distinct region_id from slope 
order by 1

select distinct region_id from region 
order by 1