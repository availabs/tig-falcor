BEGIN;

ALTER TABLE public.views ADD COLUMN layer character varying(255);

UPDATE public.views
set layer =
        CASE
            WHEN id IN (19)
                THEN 'npmrds'

            WHEN id IN (46,45,47,48,50,43,44,42,41)
                THEN 'sed_county_2040'

            WHEN id IN (113,110,109,111,112,114,107,108,106,105)
                THEN 'sed_county_2050'

            WHEN id IN (207,204,203,205,206,208,201,202,200,199)
                THEN 'sed_county_2055'

            WHEN id IN (37,34,30,29,31,28,26,27,32,33,36,35,13,25,24,38)
                THEN 'sed_taz_2040'

            WHEN id IN (169,166,162,161,163,160,158,159,164,165,168,167,155,157,156,170)
                THEN 'sed_taz_2055'

            WHEN id IN (58,62)
                THEN 'bpm_performance'

            WHEN id IN (23)
                THEN 'hub_bound_travel_data'

            WHEN id IN (53,141)
                THEN 'rtp_project_data'

            WHEN id IN (131,64,187)
                THEN 'tip'

            WHEN id IN (22,132,134,142,146,145,18,128,133,143,147,144)
                THEN 'acs_census'
            END;

COMMIT;
END;




UPDATE public.views
set layer = 'npmrds'
where data_model like '%SpeedFact%';

UPDATE public.views
set layer = 'tip'
where id in (SELECT distinct view_id FROM public.tip_projects);

UPDATE public.views
set layer = 'acs_census'
where data_model like '%ComparativeFact%';


UPDATE public.views
set layer = 
    CASE 
        WHEN source_id in (5)
            THEN 'sed_taz_2040'
        WHEN source_id in (45)
            THEN 'sed_county_2040'
        WHEN source_id in (61)
            THEN 'sed_taz_2050'
        WHEN source_id in (64)
            THEN 'sed_county_2050'
        WHEN source_id in (78)
            THEN 'sed_taz_2055'
        WHEN source_id in (76)
            THEN 'sed_county_2055'
    END



/*

WHEN id IN (23)
                THEN 'hub_bound_travel_data'

            WHEN id IN (53,141)
                THEN 'rtp_project_data'
*/