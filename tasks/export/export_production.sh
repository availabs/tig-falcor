ssh deploy@tig.nymtc.org -p 899 '/usr/bin/pg_dump --host "127.0.0.1" --username "gateway" --verbose --format=c --blobs --section=pre-data --section=data --section=post-data --schema "public" --exclude-table-data 'public.*speed_fact*' --exclude-table-data 'public.tmp*' "gateway_test2"' > ~/production.sql


#/usr/bin/pg_restore --host "lor.availabs.org" --port "5432" --username "postgres" --no-password --dbname "tig_production" --verbose --schema "public" "/home/alex/production.pgdump"


ssh deploy@tigtest2.nymtc.org -p 899 '/usr/bin/pg_dump --host "127.0.0.1" --username "gateway" --verbose --format=c --blobs --section=pre-data --section=data --section=post-data --exclude-schema 'speed_facts_partitions' --exclude-schema 'link_speed_facts_partitions' --exclude-table-data 'public.*speed_fact*' --exclude-table-data 'public.tmp*' "gateway_test2"' > ./tigtest2.sql


pg_restore  --username "gateway" --dbname "tig_production"