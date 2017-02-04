-- pg_fact pseudo extension for windows, rds, and linux db.
-- depends on :
-- * role admin created.
create role admin;
-- 
--jkroeze

--functions used in fact schema

 ----------------
--     --     --
--     fact tables --
-- --         --
    ----------------

-- test db
-- uncomment these line to do test in psql (console)
--drop database pg_fact_test;
--create database pg_fact_test;
--\c pg_fact_test;
--

-- v1.create_source
-- depends on table source.fact
create extension if not exists "uuid-ossp";

create extension if not exists pgcrypto;

create  schema if not exists v1;

create schema if not exists fact;

-- Table fact.eid stores all entity (thing) identifiers which is a
-- uuid.
create
    table
    if not exists fact.eid(
	eid uuid not null default uuid_generate_v1mc() primary key
	);
--test structure and default values
begin; savepoint test;
insert into fact.eid default values;
select true "ok?" from fact.eid where eid is not null limit 1;
rollback to savepoint test; commit;



-- Table fact.lookup_ref stores all entity references to an alternate
-- identifier
-- e.g. Employee Id, Government id, etc.
CREATE TABLE if not exists fact.lookup_ref (
	"ref" text not NULL,
        "ref_tag" text not null,
	eid uuid NULL,
	CONSTRAINT lookup_ref_ref_key UNIQUE ("ref"),
	CONSTRAINT lookup_ref_eid_fkey FOREIGN KEY (eid) REFERENCES fact.eid(eid)
        )
    WITH (
	OIDS=FALSE
        );


--test structure
begin; savepoint test;
select 'should have ref, ref_tag, and eid columns' test;
select
    ref,
    ref_tag,
    eid
from
    fact.lookup_ref limit 2;
rollback to savepoint test; commit;

-- Table fact.src keeps track of all sources of information with
-- similar structure to the lookup_ref table.
-- Each fact.fact child table is a source, often a literal snapshot
-- of an external system. Keep in mind child tables don't inherit indexes.
create table if not exists fact.src (like fact.lookup_ref including all );
--If exists: alter table fact.src rename source_id to src;

alter table fact.src  rename eid to src;

alter table fact.src add unique (src);

alter table fact.src alter src  set default (uuid_generate_v1mc());

begin; savepoint test;
select ref,
    ref_tag,
    src
from fact.src;
rollback to savepoint test; commit;

-- Table fact.fact is the one big table for all the things
-- Inheritance in postgres isn't perfect (see notes in postgres docs)
-- This is an entity, attribute, value style which we can use to store
-- documents or smaller attribute-values
-- For example, fact.hr can store the big hr state of the world per
-- entity (person). Or, you can store fact.person_name for all people.
-- Or both :)!!
-- * txid: the sequence of storage. There is but one txid sequence per db
-- * txtime: the physical storage time.
-- * ef_at: the logical time this attribute is/was/becomes in effect.
-- * touched_at: the last time a process attempted storage. Example,
-- process x has attribute y with no change. The db marks the records
-- was touched. One cannot alter records in this db.
    --
-- These three timestamptz give nice query options.
-- * eid: the entity identifier
-- * sha1: the hash of this document or attribute
-- * add: Mark as false to retract an attribute
-- * timeagg: a Time Aggregate label. We use this to group a contract
-- year with revisions before and after a fiscal year.
-- * src: Where the entity attribute came from. Eg. a namespaced label
-- like "hr/person-name"
-- * val: The data

create
    table if not exists
    fact.fact(
	txid bigserial,
	txtime timestamptz not null default now(),
        ef_at timestamptz not null default now(),
        touched_at timestamptz default now(),
	eid uuid references fact.eid(eid),
	sha1 text,
	add boolean default true,
        timeagg text default null,
        src uuid references fact.src(src),
        val jsonb not null
	);

-- test structure of inherited table
begin;savepoint test;
create
    table
    fact.test1 () inherits (fact.fact);
--test		
insert into fact.test1 (val) values('[1,2,3]');
select txid, txtime, ef_at, touched_at, eid, sha1, add, timeagg, src, val from fact.test1;
select
    txid, -- default integer nNOT insertable!
    txtime, -- default tnow, can be overri
    eid, -- insertable , required!
    sha1, -- default sha1 of val NOT insertable
    add, -- default true can be overidden
    timeagg, -- default fiscal_year() , can be overriden
    src, -- insertable
    val -- required, no default.
from fact.test1 limit 1;
rollback to savepoint test; commit;


---------------------
-- Fact Functions
--------------------

-- Function fact.eid
-- Initiate or lookup an entity in the database given a ref

create or replace function fact.eid(_ref text, _ref_tag text, out result uuid )
language plpgsql
    as $$
    begin
    select eid from fact.lookup_ref where ref = _ref into result;
        if result is null then 
    insert into fact.eid default values returning eid  into result;
    insert into fact.lookup_ref (eid, ref, ref_tag) values (result, _ref, _ref_tag) returning eid into result;
        end if;
end;
$$;

--test just one eid results
begin; savepoint test;
select * from fact.eid('0000547', 'testeid');
select * from fact.eid('0000547', 'test2eid');
select * from fact.lookup_ref;
rollback to savepoint test; commit;

-- update: bug found in this function. It's likely unused.
-- -- Function fact.eid (with match_on_ref paramater
-- -- will find existing ref, and match 2 refs to one entity id (eid).
-- -- for example employee id and government id can point to the same eid.
-- --doc: look for existing ref. if exists,  match on ref, create an eid if none
-- --exists, and ensure both ref and match on ref exists or are
-- --created with identical eids.
-- create or replace function fact.eid(_ref text, _ref_tag text, _match_on_ref text, out result uuid)
--     language plpgsql
--     as $$
--     declare _ref_exists uuid;
--     declare _match uuid;
--     begin
-- -- use existing function fact.eid(text,text).
-- select eid from fact.lookup_ref where _match_on_ref = ref into _match;
--     raise notice 'ref exists %s', _match;
--     if  _match is null then
-- select null::uuid into result;
--     raise notice '%s', 'ref does not exist';
--     end if;
--     if _match is not null then
-- insert into fact.lookup_ref (eid, ref, ref_tag) values (_match, _ref, _ref_tag)
--     on conflict do nothing
--     returning eid into result;
--     end if;
--     if result is null then select _match into result; end if;
--     end;
-- $$;

--test just one result from 2 ids.
--------------------------------------------------------------------------------------------------------------
-- begin; savepoint test;                                                                                   --
-- select * from fact.eid('0000547', 'gds/employee-id');                                                    --
-- select * from fact.eid('0123456789', 'gds/usc-id', '0000547');                                           --
-- select * from fact.lookup_ref where ref = '0000547' or ref = '0123456789'; -- should be 2 rows same eid; --
-- rollback to savepoint test; commit;                                                                      --
--------------------------------------------------------------------------------------------------------------


-- Function fact.src
-- Initiate a new source (src as short name for convenience.

create or replace function fact.src(_ref text, _ref_tag text, out result uuid )
language plpgsql
    as $$
    begin
    select src from fact.src where ref = _ref into result;
        if result is null then 
    insert into fact.src (src, ref, ref_tag)
      select uuid_generate_v1mc() , _ref, _ref_tag
      returning src into result;
    end if;
end;
$$;

begin; savepoint test;
select fact.src('a-source', 'a-ref-tag');
rollback to savepoint test; commit;


-- Function create_fact_table
-- Create an inherited table like fact.fact and add correct 
-- references and constaints.
-- this is done by admin user to get around table ownership.

create
    or replace function fact.create_fact_table(
	_schema text,
	_name text,
	_ref_tag text,
	out result text
	) 
security definer
    language plpgsql
    as $$
    begin
    execute format('
create schema if not exists "%1$s";
create table if not exists %1$I."%2s" () inherits (fact.fact);
alter table %1$I.%2$I owner to admin;
alter table if exists %1$I.%2$I add unique (sha1);
alter table if exists %1$I.%2$I add foreign key (src) references fact.src(src) on delete cascade;
alter table if exists %1$I.%2$I add foreign key (eid) references fact.eid(eid) on delete cascade;
', _schema, _name);
select fact.src(_name, _ref_tag) into result;
    end;
    $$;

begin;savepoint test;
select fact.create_fact_table('a-schema', 'a-name', 'a-ref-tag');
--select v1.drop_source('testsource');
select
txid,
txtime,
eid,
-- the first external id "ref" inserted to dim.lookup_ref.
sha1,
timeagg,
src,
val
from "a-schema"."a-name";
select 'has record in src table?' test;
select * from  fact.src s;
rollback to savepoint test; commit;


--test function for eid and for src work.
begin; savepoint test;
select * from fact.src('xyz', 'xyy/xy');
select eid from fact.eid('0000000', 'contracttest/eid');
select * from fact.lookup_ref;
rollback to savepoint test; commit;


--test
begin; savepoint test;
select * from fact.eid('0000547', 'testeid');
select eid, ref, ref_tag from fact.lookup_ref limit 5;
delete from fact.lookup_ref where ref like '0000547%';
rollback to savepoint test; commit;

----------------------
-- FACT API ---
-----------------------

-- Fucntion create_source
-- depends on function fact.create_fact_table
-- this allows for modifying all saves (increase perf, add search) by
-- changing one function. Simpler than triggers.
-- new api
-- v1.save_document( varchar, jsonb)
-- v1.save_document( varchar, jsonb[])
-- v1.set_search_text( varchar, id uuid)
-- # add wrapper functions for current v1.create_x funs
-- -- where x is
--    contract                       
--    department                     
--    faculty_hr                     
--    faculty_hr_education           
--    fcheck                         
--    gds_affiliate_faculty          
--    gds_employed_faculty           
--    lf_entry                       
--    lf_entry_delete                
--    name_doc                       
--    school                         
--    staff_user                     


CREATE
    or replace function fact.create_src(
	_name text,
	_ref_tag text,
	out result jsonb
	) security definer language plpgsql as $$ 
    DECLARE _eid uuid;
    begin
select
    fact.create_fact_table(
	'fact',
	_name,
	_ref_tag
	) into
    _eid;
select
    jsonb_build_array(
	'ok',
	jsonb_build_object(
	    'src',
	    _eid
	    )
	) into result;
    end;
    $$;

begin; savepoint test;
--select v1.drop_source('testsource');
select fact.create_src('testsource', 'bxt/testsource');
select
txid,
txtime,
eid,
-- the first external id "ref" inserted to dim.lookup_ref.
sha1,
timeagg,
src,
val
from fact.testsource;
select 'has record in src table?' test;
select * from fact.src s;
rollback to savepoint test; commit;


-- Function fact.drop_src
-- Remove a source fact table and record from fact.src
create or replace function fact.drop_src(_name text, out result json)
    language plpgsql as $$
    declare _src uuid;
    BEGIN
    execute format('drop table fact.%I', _name);
delete from fact.src where ref = _name returning src into _src;
select jsonb_build_array('ok', jsonb_build_object('src', _src)) into result;
    end;
    $$;
begin; savepoint test;
select * from fact.create_src('test1','test1tag');
 --should have test1
select * from fact.src;
select * from fact.drop_src('test1');
select 'should not have record in fact src' test;
select * from fact.src;
 -- should not have test1
rollback to savepoint test; commit;


-- Function fact.set_search
-- Set search sideeffects. This one is just a shell.
create or replace function fact.set_search(_txid bigint, OUT result boolean)
    language plpgsql as $$
    begin
select true into result;
end $$;




-- Function fact._save_fact
-- save ONE fact used within fact.save_facts function.
-- depends on search api set_search.
create or replace function fact._save_fact(
        _table text,
        _ref text,
        _ef_at timestamptz,
        _val jsonb,
        _timeagg text,
        _add boolean default true,
        _txtime timestamptz default now(),
        out txid text)
    security definer
    language plpgsql as $$
declare _txid bigint;
    begin
execute format('
insert into fact.%1$s
    ( txtime,
        eid,
        src,
        sha1,
        add,
        timeagg,
        val,
        ef_at ) values
    ( %2$L,
      (select eid from fact.eid(%6$L , %1$L ||''/eid'' )),
      (select result from fact.src(%1$L , ''bxt/''||%1$L )),
      encode(digest(%5$L::jsonb::text, ''SHA1''), ''hex''), 
      %3$L,
      %4$L,
      %5$L,
      %7$L)
    on conflict (sha1) do update set (touched_at, add) = (now(), %3$L)
    returning *
    ', _table, _txtime, _add, _timeagg, _val, _ref, _ef_at ) into _txid;
PERFORM (select true from  fact.set_search(_txid));
select _txid into txid;
end$$;

create or replace function fact.save_facts(
        tbl text,
        ref text,
        val jsonb,
        timeagg text default null,
        ef_at timestamptz default now(), 
        txtime timestamptz default now(),
        add boolean default true,
        OUT result jsonb)
    language plpgsql as $$
    declare _create_tbl jsonb;
    begin
    if not exists (select table_name from information_schema.tables where table_name = tbl and table_schema = 'fact') then
       select fact.create_src(tbl, 'bxt/'|| 'tbl') into _create_tbl;
    end if;
    case jsonb_typeof(val)
    when 'object' then
select jsonb_build_object( 'success', 'false', 'message', 'please use an array') into result;
    when 'array' then
select jsonb_build_object(
        'success', 'true',
        'txids', (select array_agg(txid) from
            (select value from jsonb_array_elements(val)) it,
            fact._save_fact(
                _table:=tbl::text,
                _ref:=ref::text,
                _ef_at:=ef_at::timestamptz,
                _val:=it.value::jsonb,
                _txtime:=txtime::timestamptz,
                _add:=add::boolean,
                _timeagg:=timeagg::text ))) into result;
    else
--select jsonb_build_object('message','Cannot save json typeof') into result;
select true into result;
    end case;
end;
$$ ;

begin; savepoint test;

select * from fact.save_facts(tbl:='test', ref:='1234567', ef_at:='2013/01/01', val:='[{"a":1}, {"b":2}, {"c":3}]'::jsonb);
select 'fact.test should have 3 rows' test;
select * from fact.test;

rollback to savepoint test; commit;

-- Function v1.save_facts wraps save_facts for api access.
create or replace function v1.save_facts(
        tbl text,
        ref text,
        val jsonb,
        timeagg text default null,
        ef_at timestamptz default now(), 
        txtime timestamptz default now(),
        add boolean default true,
        out result jsonb )
    language sql as $$
select * from fact.save_facts(
        tbl, ref, val, timeagg, ef_at, txtime, add
        )
$$;

-- Canonical "time travel" view
--View definition:
create or replace view fact.fact_v as
 SELECT fact.txid,
    fact.txtime,
    fact.touched_at,
    fact.ef_at,
    fact.eid,
    fact.sha1,
    fact.add,
    fact.timeagg,
    fact.src,
    fact.val,
    tstzrange(fact.ef_at, lead(fact.ef_at)
        OVER (
            PARTITION BY
            fact.eid, fact.src, fact.timeagg
        ORDER BY
            fact.txtime, fact.txid)) AS during
FROM fact.fact;
--test eg
select 'should query during with a timestamp' test;
select * from fact.fact_v where during @> now();


-- set up roles for aws rds
create role authenticator;
create role basic_school;
create role guest;
grant guest, basic_school to authenticator;


