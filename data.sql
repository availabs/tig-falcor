--
-- PostgreSQL database dump
--

-- Dumped from database version 11.5 (Ubuntu 11.5-3.pgdg18.04+1)
-- Dumped by pg_dump version 12.9 (Ubuntu 12.9-0ubuntu0.20.04.1)

-- Started on 2022-01-27 12:41:58 EST

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
-- TOC entry 1573 (class 1259 OID 287295)
-- Name: npmrds_shapefile_2016; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.npmrds_shapefile_2016 (
    tmc character varying NOT NULL,
    state character varying,
    wkb_geometry public.geometry(MultiLineString,4326)
)
WITH (fillfactor='100');


ALTER TABLE public.npmrds_shapefile_2016 OWNER TO postgres;

--
-- TOC entry 16825 (class 0 OID 287295)
-- Dependencies: 1573
-- Data for Name: npmrds_shapefile_2016; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.npmrds_shapefile_2016 (tmc, state, wkb_geometry) FROM stdin;
\.


--
-- TOC entry 16597 (class 2606 OID 287302)
-- Name: npmrds_shapefile_2016 npmrds_shapefile_2016_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.npmrds_shapefile_2016
    ADD CONSTRAINT npmrds_shapefile_2016_pkey PRIMARY KEY (tmc);


-- Completed on 2022-01-27 12:42:03 EST

--
-- PostgreSQL database dump complete
--

