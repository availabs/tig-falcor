#!/bin/bash

set -e

pg_dump \
    --host pluto.availabs.org \
    --port 5432 \
    --username npmrds_admin \
    --dbname npmrds_production \
    --schema-only \
    --no-owner \
    --table=tmc_metadata_2022 \
 > tmc_export/tmc_metadata_2022.sql

psql \
    -hpluto.availabs.org \
    -p5432 \
    -Unpmrds_admin \
    -dnpmrds_production \
    -c 'COPY (SELECT * FROM tmc_metadata_2022) TO STDOUT WITH CSV HEADER' \
  > tmc_export/tmc_metadata_2022.csv

scp -P 899 -r tmc_export/ deploy@tigtest2.nymtc.org:~/

ssh deploy@tigtest2.nymtc.org -p 899 'psql --host "127.0.0.1" --username "gateway" --dbname "tig_production" ~/tmc_export/tmc_metadata_2022.sql'

ssh deploy@tigtest2.nymtc.org -p 899 'psql --host "127.0.0.1" --username "gateway" --dbname "tig_production" -c "COPY public.tmc_metadata_2022 FROM '/tmc_export/tmc_metadata_2022.csv' WITH CSV HEADER"'

