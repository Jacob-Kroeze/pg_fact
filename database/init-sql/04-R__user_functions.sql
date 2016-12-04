-- api at v1
-- for list users and login etc.

    grant usage on schema v1 to admin, guest;

-- depends on ./pg_message_queue/install.sql
-- create extension if not exists pg_message_queue VERSION '0.2.1'; --
-- uncomment pg_message queue if you have access to a plain old, non-docker.
create extension if not exists pgcrypto;
create extension if not exists "uuid-ossp";
create schema if not exists basic_auth;
create schema if not exists v1;

create table if not exists
    basic_auth.users (
        email text primary key check (email ~* '^.+@.+\..+$' ),
        pass text not null check (length(pass) < 512),
        name text not null default '',
        department_mask varchar(10),
        role     name not null check (length(role) < 512),
        verified boolean not null default false
    -- If you like add more columns, or a json column
        );

create or replace function
basic_auth.check_role_exists() returns trigger
  language plpgsql
  as $$
begin
  if not exists (select 1 from pg_roles as r where r.rolname = new.role) then
    raise foreign_key_violation using message =
      'unknown database role: ' || new.role;
    return null;
  end if;
  return new;
end
$$;

drop trigger if exists ensure_user_role_exists on basic_auth.users;
create constraint trigger ensure_user_role_exists
  after insert or update on basic_auth.users
  for each row
  execute procedure basic_auth.check_role_exists();

create or replace function
basic_auth.clearance_for_role(u name) returns void as
$$
declare
  ok boolean;
begin
  select exists (
    select rolname
      from pg_authid
     where pg_has_role(current_user, oid, 'member')
       and rolname = u
  ) into ok;
  if not ok then
    raise invalid_password using message =
      'current user not member of role ' || u;
  end if;
end
$$ LANGUAGE plpgsql;

create or replace function
basic_auth.encrypt_pass() returns trigger
  language plpgsql
  as $$
begin
  if tg_op = 'INSERT' or new.pass <> old.pass then
    new.pass = crypt(new.pass, gen_salt('bf'));
  end if;
  return new;
end
$$;

drop trigger if exists encrypt_pass on basic_auth.users;
create trigger encrypt_pass
  before insert or update on basic_auth.users
  for each row
  execute procedure basic_auth.encrypt_pass();

-- send user validations --
-- 1. create a queue for validation and reset
select * from  pg_mq_create_queue('user_validate', 'text');


-- send user a token.
create or replace function
basic_auth.send_validation() returns trigger
  language plpgsql
  as $$
declare
  tok uuid;
begin
  select uuid_generate_v4() into tok;
  insert into basic_auth.tokens (token, token_type, email)
         values (tok, 'user_validation', new.email);
  perform pg_mq_send_message('user_validate',
    json_build_object(
      'email', new.email,
      'token', tok,
      'token_type', 'user_validation'
    )::text
  );
  return new;
end
$$;

drop trigger if exists send_validation on basic_auth.users;
create trigger send_validation
  after insert on basic_auth.users
  for each row
  execute procedure basic_auth.send_validation();

-------------------------------------------------------------------------------
-- Email Validation and Password Reset

drop type if exists token_type_enum cascade;
create type token_type_enum as enum ('user_validation', 'user_reset');

create table if not exists
basic_auth.tokens (
  token       uuid primary key,
  token_type  token_type_enum not null,
  email       text not null references basic_auth.users (email)
                on delete cascade on update cascade,
  created_at  timestamptz not null default current_date
);

-------------------------------------------------------------------------------
-- Login helper

create or replace function
basic_auth.user_role(email text, pass text) returns name
  language plpgsql
  as $$
begin
  return (
  select role from basic_auth.users
   where users.email = user_role.email
     and users.pass = crypt(user_role.pass, users.pass)
  );
end;
$$;

create or replace function
basic_auth.current_email() returns text
  language plpgsql
  as $$
begin
  return current_setting('postgrest.claims.email');
exception
  -- handle unrecognized configuration parameter error
  when undefined_object then return '';
end;
$$;

-------------------------------------------------------------------------------
-- Public functions (in current schema, not basic_auth)

create or replace function
v1.request_password_reset(email text) returns void
  language plpgsql
  as $$
declare
  tok uuid;
begin
  delete from basic_auth.tokens
   where token_type = 'user_reset'
     and tokens.email = request_password_reset.email;
  select uuid_generate_v4() into tok;
  insert into basic_auth.tokens (token, token_type, email)
         values (tok, 'user_reset', request_password_reset.email);
  perform pg_mq_send_message('user_reset',
    json_build_object(
      'email', request_password_reset.email,
      'token', tok,
      'token_type', 'user_reset'
    )::text
  );
end;
$$;

create or replace function
v1.reset_password(email text, token uuid, pass text)
  returns void
  language plpgsql
  as $$
declare
  tok uuid;
begin
  if exists(select 1 from basic_auth.tokens
             where tokens.email = reset_password.email
               and tokens.token = reset_password.token
               and token_type = 'user_reset') then
    update basic_auth.users set pass=reset_password.pass
     where users.email = reset_password.email;
    delete from basic_auth.tokens
     where tokens.email = reset_password.email
       and tokens.token = reset_password.token
       and token_type = 'user_reset';
  else
    raise invalid_password using message =
      'invalid user or token';
  end if;
  delete from basic_auth.tokens
   where token_type = 'user_reset'
     and tokens.email = reset_password.email;
  select uuid_generate_v4() into tok;
  insert into basic_auth.tokens (token, token_type, email)
         values (tok, 'user_reset', reset_password.email);
  perform pg_mq_send_message('user_reset',
    json_build_object(
      'email', reset_password.email,
      'token', tok
    )::text
  );
end;
$$;

-- user verify by token
create or replace function
v1.verify_user(token uuid)
  returns table (email text, success boolean)
  language plpgsql
  as $$
declare
  tok uuid;
  _email text;
begin
  select email from basic_auth.tokens
  where tokens.token  = verify_user.token
      and token_type = 'user_validation'
  into _email;
  if _email is not NULL then
    update basic_auth.users set verified=true
     where users.email = _email;
    delete from basic_auth.tokens
     where tokens.email = _email
       and tokens.token = verify_user.token
       and token_type = 'user_validation';
    select _email, true;
  else
    raise invalid_password using message =
      'invalid token';
      end if;
  return query
  select _email, true;     
end;
$$;

drop type if exists basic_auth.jwt_claims cascade;
create type
basic_auth.jwt_claims AS (role text, email text);

create or replace function
v1.login(email text, pass text) returns basic_auth.jwt_claims
  language plpgsql
  as $$
declare
  _role name;
  _verified boolean;
  result basic_auth.jwt_claims;
begin
  select basic_auth.user_role(email, pass) into _role;
  if _role is null then
    raise invalid_password using message = 'invalid user or password';
  end if;
  select true from basic_auth.users where basic_auth.users.email = login.email into _verified;
  if _verified then
    select _role as role, login.email as email into result;
  else
    raise invalid_password using message = 'user has not verified email';
  end if;
  return result;
end;
$$;

grant guest,basic_school to authenticator;

create or replace function
v1.signup(email text, pass text) returns void
as $$
  insert into basic_auth.users (email, pass, role) values
    (signup.email, signup.pass, 'basic_school');
    $$ language sql;

------------------------------------------------------------------
-- editing users securely
----
-------------------------------------------------------------------------------
-- User management

create or replace view v1.users as
select actual.role as role,
       '***'::text as pass,
       actual.email as email,
       actual.verified as verified
from basic_auth.users as actual,
     (select rolname
        from pg_authid
       where pg_has_role(current_user, oid, 'member')
     ) as member_of
where actual.role = member_of.rolname
  and (
    actual.role <> 'author'
    or email = basic_auth.current_email()
  );


create or replace function
update_users() returns trigger
language plpgsql
AS $$
begin
  if tg_op = 'INSERT' then
    perform basic_auth.clearance_for_role(new.role);
    insert into basic_auth.users
      (role, pass, email, verified)
    values
      (new.role, new.pass, new.email,
      coalesce(new.verified, false));
    return new;
  elsif tg_op = 'UPDATE' then
    -- no need to check clearance for old.role because
    -- an ineligible row would not have been available to update (http 404)
    perform basic_auth.clearance_for_role(new.role);
    update basic_auth.users set
      email  = new.email,
      role   = new.role,
      pass   = new.pass,
      verified = coalesce(new.verified, old.verified, false)
      where email = old.email;
    return new;
  elsif tg_op = 'DELETE' then
    -- no need to check clearance for old.role (see previous case)
    delete from basic_auth.users
     where basic_auth.email = old.email;
    return null;
  end if;
end
$$;

drop trigger if exists update_users on v1.users;
create trigger update_users
  instead of insert or update or delete on
    v1.users for each row execute procedure update_users();


create or replace function
basic_auth.current_email() returns text
  language plpgsql
  as $$
begin
  return current_setting('postgrest.claims.email');
exception
  -- handle unrecognized configuration parameter error
  when undefined_object then return '';
end;
$$;


------------------------------
-- permissions
grant usage on schema public, basic_auth, v1 to guest;
grant usage on schema public, basic_auth, v1 to admin;

grant insert on table basic_auth.users, basic_auth.tokens to guest;

grant select, insert, update, delete
  on basic_auth.tokens, basic_auth.users to guest;

grant execute on function
  v1.login(text,text),
  v1.request_password_reset(text),
  v1.reset_password(text,uuid,text),
  v1.verify_user(uuid),
  v1.signup(text, text)
  to guest;

insert into basic_auth.users
 (email, pass, name, department_mask, role, verified)
values
 ('admin@example.com', 'password', 'admin-name', '', 'admin', 't');
