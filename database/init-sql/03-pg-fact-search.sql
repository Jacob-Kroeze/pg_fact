--------------------------------------------------------


----------------
-- SEARCH API (optional) just to tri grams for now.
----------------

--
-- Table search
-- 
-- set search function for updating v1.search table on each document
-- save. Keys found in fact.search_keys_config will be used to store
-- search (cache).
-- uses a pg_trgm (Postgres Trigram extension) function
--
-- Extension
-- - use tri grams for fuzzy search.
create extension if not exists pg_trgm;

create table if not exists fact.search
    (
        txid bigint,
        eid uuid,
        src uuid,
        source_name text,
        k text, 
        q text);

alter table fact.search add primary key (txid,k, q);

CREATE INDEX fact_search_trgm_gin ON fact.search USING GIN(q gin_trgm_ops);

create index on fact.search (txid);


create table if not exists fact.search_keys_config
    ( k text unique);



-- move to set search function

create or replace function fact.set_search_trgm(_txid bigint, out result boolean)
    language sql as $$
with _search as (
select t.*, src.ref source_name from (
    select 
        txid
        ,eid
        ,src
        ,objs.*
    from
-- get all object like documents and select only keys that match
-- search keys
        (select * from fact.fact where txid = _txid and val is not null and  jsonb_typeof(val) = 'object' ) fact
        left join lateral (select
            r.key k
            ,r.value q
        from jsonb_each_text(val) r 
            ) objs on true
    union
-- do it again with array like documents
    select
        txid
        ,eid
        ,src
        , arrs.*
    from (select * from fact.fact where txid = _txid and val is not null and  jsonb_typeof(val) = 'array') fact
        left join lateral (select
            r.key k , r.value q
        from jsonb_array_elements(val) a
            , jsonb_each_text (a.value) r where jsonb_typeof(a.value) = 'object' 
            ) arrs  on true
        ) t 
    join fact.search_keys_config on search_keys_config.k = t.k
    join fact.src using (src))
insert into fact.search as s
    (txid, eid, src, source_name, k, q) 
     select distinct txid, eid, src, source_name, k, q from _search where q is not null
on conflict (txid,k,q)  -- on constraint search_txid_pkey 
do update set q = s.q where s.txid = excluded.txid and s.k = excluded.k
returning true  ;
$$;

-- Function fact.set_search
-- wraps side-effects for fact._save_fact to call.
-- Example to update bulk the search table.
--  select count(s.*) from fact.fact, v1.set_search_trgm(txid) s;
-- select count(s.*) from fact.contract,  v1.set_search_trgm(txid) s;
-- select count(s.*) from fact.faculty_hr,  v1.set_search_trgm(txid) s;

create or replace function fact.set_search(_txid bigint, OUT result boolean)
    language plpgsql as $$
    begin
select true from fact.set_search_trgm(_txid) into result;
end $$;
--same test as above should work with this function.
begin; savepoint test;
insert into fact.search_keys_config select 'uscid';
select * from 
 jsonb_array_elements_text(fact.save_facts('test','a-ref','[[1],{"uscid":"42"}]') ->'txids' ) t
 ,fact.set_search(t.value::bigint);
select 'should have one row with q = 42' test;
select * from fact.search;
rollback to savepoint test; commit;
