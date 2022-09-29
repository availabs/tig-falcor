--
-- PostgreSQL database dump
--

-- Dumped from database version 11.5 (Ubuntu 11.5-3.pgdg18.04+1)
-- Dumped by pg_dump version 12.12 (Ubuntu 12.12-0ubuntu0.20.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

--
-- Name: tmc_metadata_2022; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tmc_metadata_2022 (
    tmc character varying,
    roadnumber character varying,
    roadname character varying,
    firstname character varying,
    tmclinear integer,
    country character varying,
    state_name character varying,
    county_name character varying,
    zip character varying,
    direction character varying,
    startlat double precision,
    startlong double precision,
    endlat double precision,
    endlong double precision,
    miles double precision,
    frc smallint,
    border_set character varying,
    f_system smallint,
    faciltype smallint,
    structype smallint,
    thrulanes smallint,
    route_numb integer,
    route_sign smallint,
    route_qual smallint,
    altrtename character varying,
    aadt integer,
    aadt_singl integer,
    aadt_combi integer,
    nhs smallint,
    nhs_pct smallint,
    strhnt_typ smallint,
    strhnt_pct smallint,
    truck smallint,
    state character(2),
    is_interstate boolean,
    is_controlled_access boolean,
    avg_speedlimit real,
    mpo_code character varying,
    mpo_acrony character varying,
    mpo_name character varying,
    ua_code character varying,
    ua_name character varying,
    congestion_level public.traffic_dist_congestion_level_type,
    directionality public.traffic_dist_directionality_type,
    bounding_box public.box2d,
    avg_vehicle_occupancy real,
    state_code character(2),
    county_code character(5),
    type character varying,
    road_order real,
    isprimary smallint,
    timezone_name character varying,
    active_start_date date,
    active_end_date date,
    region_code character varying
);


--
-- Name: TABLE tmc_metadata_2022; Type: ACL; Schema: public; Owner: -
--

GRANT SELECT ON TABLE public.tmc_metadata_2022 TO readonly_access;


--
-- PostgreSQL database dump complete
--

