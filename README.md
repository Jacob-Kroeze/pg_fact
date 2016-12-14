# pg_fact (sort of in use at my work, but not in this form. Use at your own risk)

A Clojure library designed to
* TODO: pg_fact console datalog
* TODO: pg_fact data entry ui
* TODO: pg_fact data correction ui with rules engine

## Dependencies

docker image postgres:9.6.1
docker image begriffs/postgrest:latest

## Usage

Resolve docker dependencies and build your environment.

- run the system
  `$ docker-compose up`

- run an sql console
  `$ psql -h localhost -U postgres`
  
- TODO: run data entry ui service; run console datalog; run console
  rules engine

- Create an admin user

`
insert into basic_auth.users
 (email, pass, name, department_mask, role, verified)
values
 ('admin@example.com', 'password', 'admin-name', '', 'admin', 't')
`
Of course, use a secure password if you deploy this somewhere.

## License

Copyright © 2016 Jacob Kroeze

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
