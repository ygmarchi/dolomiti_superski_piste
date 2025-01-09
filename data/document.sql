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