-- pg-fact-edges.sql
-- exposes Function fact.save_facts2 to add edges as a thing.
--  Save facts with refs and edges
--  refs create an eid and lookup_ref for all paths in ref
-- val: {"a": 1, "b": 2, "c-edge": "xyz"}, refs: ["a", "b"] //would create two lookup_ref records for a single new eid.
-- , edges: ["c-edge"] // would create an edge in the many-to-many table "edge"
-- insert into (f1, f2, ref, )


-- Function jsonb_arr2text_arr(jsonb)
CREATE OR REPLACE FUNCTION jsonb_arr2text_arr(_js jsonb)
RETURNS text[] AS
$func$
SELECT ARRAY(SELECT jsonb_array_elements_text(_js))
$func$
LANGUAGE sql IMMUTABLE;


-- Function jsonb_extract_paths(jsonb, jsonb)
-- returns jsonb with paths as keys and fields as values
create or replace function jsonb_extract_paths
(paths jsonb, val jsonb, OUT result jsonb )
language plpgsql
as $$
begin
if jsonb_typeof(val) != 'object' then raise exception '%', 'jsonb_extract_path(jsonb, jsonb) second arg must be an object';
end if;
--raise notice '%', (select val#>> array[path] from unnest(jsonb_arr2text_arr(paths) ) path;
select jsonb_object(
       (select array_agg( array[a,b]) as ab
               from (
                    select unnest(jsonb_arr2text_arr(paths)) as a
                    ,unnest((select array_agg(
                    coalesce(val::jsonb #>> array[path], 'value-not-found'::text)) from unnest(jsonb_arr2text_arr(paths)) path )) b
                    ) arrays
                    )) into result;
end;
$$;
begin; savepoint test;
select * from jsonb_extract_paths(
paths:='[]',
val:='{}');
select * from jsonb_extract_paths(
paths:='["emplid","uscid", "email"]', 
val:='{"emplid":"0123456", "uscid":"0123456789", "email": "my@example.usc.emai"}');
rollback to savepoint test; commit;

-- Function fact.eid( ref text, tag text, eid uuid)
-- insert a new ref into lookup_ref table so that
-- two references point to the same eid. Using an existing eid
-- e.g. 7 digit Employee id and 10 digit employee ids point to the same person eid (entity id)
create or replace function fact.eid(_ref text, _ref_tag text, _eid uuid, out result uuid)
    language plpgsql
    as $$
    begin
insert into fact.lookup_ref (eid, ref, ref_tag) values (_eid, _ref, _ref_tag)
    on conflict do nothing
    returning eid into result;
end;
$$;
begin;savepoint test;
select 'test connecting two references using fact.eid(text,text,uuid)';
with entity as (select result eid from fact.eid('first-original-ref', 'gds/employee-id')
) select  fact.eid('new-ref', 'new-tag', entity.eid) from entity;
select * from fact.lookup_ref where ref like '%ref%';
rollback to savepoint test; commit;



-- Function fact.eid (with refs_object jsonb)
-- will find existing ref(s) (refs_object vals)
-- , and match 1,2 or 3+ ref values (refs_object vals) to one entity id (eid).
-- for example employee id and government id can point to the same eid.
-- doc: look for existing ref. if exists,  match on ref, create an eid if none
--exists, and ensure both ref and match on ref exists or are
--created with identical eids.

begin;
create or replace function fact.eid(_refs_obj jsonb, out result uuid)
       language plpgsql as $$
       declare _eid uuid;
       declare _test text;
       declare _refs_arr text[];
       begin
--raise notice 'inserting refs: %', _refs_obj;
select array_agg(o.key) from jsonb_each_text(_refs_obj) o into _refs_arr;
--raise notice 'checking refs for existing entity id: %', _refs_arr;
select eid from fact.lookup_ref where ref = ANY (_refs_arr) into _eid;
if _eid is null then
   select first_eid.result AS eid
          from (select * from jsonb_each_text(_refs_obj) o order by o.key asc limit 1) first
          ,fact.eid(first.value, first.key) first_eid into _eid;
end if;
--raise notice 'eid is set: %', _eid;
perform value, key from jsonb_each_text(_refs_obj) o,
       fact.eid(o.value, o.key, _eid) rest_eids;
select _eid into result;
end;
$$;
commit;
begin; savepoint test;
select 'insert 3 new refs';
select result eid from fact.eid('{"email": "my@example.usc.email", "uscidx": "0123456789x", "emplid": "0123456"}'::jsonb);
select 'test 3 new refs with one eid';
select count(*) = 3 from fact.lookup_ref where fact.eid('0123456', 'emplid') = eid;
rollback to savepoint test; commit;

-- Table fact.edge
-- store relation between two facts.
create table if not exists fact.edge
(f1_eid uuid references fact.eid(eid), f1_txid bigint not null,
f2_eid uuid references fact.eid(eid), f2_txid bigint not null,
label text,
unique (f1_eid, f1_txid, f2_eid, f2_txid)
);

-- Table fact.fact
-- add exp_at non null timestamptz 
alter table fact.fact add if not exists exp_at timestamptz not null default 'infinity'::timestamptz;


-- Function hoist (temporarily) save.save_facts2
-- this way _save_fact2 can depend on save_facts2
create or replace function fact.save_facts2(
        tbl text,
        refs jsonb,
        edges jsonb,
        val jsonb,
        timeagg text default null,
        ef_at timestamptz default now(), 
        txtime timestamptz default now(),
        add boolean default true,
        OUT result jsonb)
    language plpgsql as $$ begin end$$;



-- Function fact._save_fact2
-- save ONE fact used within fact.save_facts function.
-- depends on search api set_search.
begin;
create or replace function fact._save_fact2(
        _table text,
        _refs jsonb,
        _edges jsonb,
        _ef_at timestamptz,
        _val jsonb,
        _timeagg text,
        _add boolean default true,
        _txtime timestamptz default now(),
        out txid text)
    security definer
    language plpgsql as $$
declare _txid bigint;
declare _eid uuid;
declare _refs_object jsonb;
declare _edges_object jsonb;
    begin
-- build up refs object with is ref-key: ref-val
select jsonb_extract_paths(_refs, _val) into _refs_object;
-- refs objects "value" place may have "value-not-found".
select eid from fact.eid(_refs_object) into _eid;
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
      %6$L,
      (select result from fact.src(%1$L , ''bxt/''||%1$L )),
      encode(digest(%5$L::jsonb::text, ''SHA1''), ''hex''), 
      %3$L,
      %4$L,
      %5$L,
      %7$L)
    on conflict (sha1) do update set (touched_at, add) = (now(), %3$L)
    returning *
    ', _table, _txtime, _add, coalesce(_timeagg, 'INF'::text), _val, _eid, _ef_at ) into _txid;
PERFORM (select true from  fact.set_search(_txid));
update fact.fact fact set exp_at = now() 
from ( select fact_inner.txid from fact.fact fact_inner
       where (eid, timeagg, src)
           = (_eid, coalesce(_timeagg, 'INF'::text), (select result from fact.src(_table, 'ignore')))
order by timeagg desc, ef_at desc, txid desc
limit 1) _prior where _prior.txid  = fact.txid;
-- insert an edges record connecting 2 facts.
select jsonb_extract_paths(_edges, _val) into _edges_object;
raise notice 'Edge object %', _edges_object;
with edges as (select
     _eid f1_eid,
     _txid f1_txid,
     fact.eid(value, key) f2_eid,
     (select 
coalesce(
        max(f.txid) 
        ,(select (new_edge.result->'txids'->>0)::bigint 
                 from fact.save_facts2(tbl:='default_edge', refs:='[]', edges:='[]', 
                                  ef_at:=now(), timeagg:='INF', 
                                  val:=jsonb_build_array(jsonb_build_object(key, value))) new_edge))
             from fact.fact f where  eid = fact.eid(value, key) and ef_at <= _ef_at ) f2_txid,
     key "label"
     from jsonb_each_text(_edges_object) o
) insert into fact.edge (f1_eid, f1_txid, f2_eid, f2_txid, "label") 
            select o.f1_eid, o.f1_txid, o.f2_eid, o.f2_txid, o."label"
            from edges o on conflict do nothing;
select _txid into txid;
end;
$$;
commit;


create or replace function fact.save_facts2(
        tbl text,
        refs jsonb,
        edges jsonb,
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
            fact._save_fact2(
                _table:=tbl::text,
                _refs:=refs,
                _edges:=edges,
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
select * from fact.save_facts2(
       tbl:='test',
       refs:='["emplid","uscid"]', 
       edges:='["primary_dept"]',
       ef_at:='2013/01/01', 
       timeagg:='2013',
       val:='[{"emplid":"0123456", "uscid":"0123456789"},{"a":1}, {"b":2}, {"c":3}]'::jsonb);
select 'fact.test should have 3 rows' test;
select * from fact.test;
select * from fact.lookup_ref where ref like '%not%';
select * from fact.edge;-- todo!
rollback to savepoint test; commit;

begin; savepoint test;
select 'create fact test source';
 select fact.create_src('test', 'bxt/'|| 'tbl') ;
select 'test expiration is properly set for facts that match eid, timeagg, src';
select fact._save_fact2(
'test'::text,
'["emplid","uscid"]'::jsonb, 
'["primary_dept", "secondary_dept"]'::jsonb,
'2013-01-01'::timestamptz, 
'{"new": "attribute", "emplid":"0123456", "uscid":"0123456789", "primary_dept": "012dept", "secondary_dept": "2dept222"}'::jsonb,
'2013',
true,
now()
);
select fact._save_fact2(
'test'::text,
'["emplid","uscid"]'::jsonb, 
'["primary_dept", "secondary_dept"]'::jsonb,
'2013-01-01'::timestamptz, 
'{"another": "new attribute", "emplid":"0123456", "uscid":"0123456789", "primary_dept": "012dept", "secondary_dept": "2dept222"}'::jsonb,
'2013',
true,
now()
);
select fact._save_fact2(
'test'::text,
'["emplid","uscid"]'::jsonb, 
'["primary_dept", "secondary_dept"]'::jsonb,
'2013-01-01'::timestamptz, 
'{"another": "new attribute, Again!", "emplid":"0123456", "uscid":"0123456789", "primary_dept": "012dept", "secondary_dept": "2dept222"}'::jsonb,
'2013',
true,
now()
);
select * from fact.fact join fact.lookup_ref using (eid) where ref = '0123456';
rollback to savepoint test; commit;



