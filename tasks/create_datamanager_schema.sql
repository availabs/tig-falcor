CREATE SCHEMA IF NOT EXISTS data_manager;

-- Table: data_manager.sources

-- DROP TABLE IF EXISTS data_manager.sources;
CREATE SEQUENCE data_manager.sources_id_seq START 100;

CREATE TABLE IF NOT EXISTS data_manager.sources
(
    id integer NOT NULL DEFAULT nextval('data_manager.sources_id_seq'::regclass),
    name text COLLATE pg_catalog."default" NOT NULL,
    update_interval text COLLATE pg_catalog."default",
    category text[] COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    statistics jsonb,
    metadata jsonb,
    categories jsonb DEFAULT '[]'::jsonb,
    type character varying COLLATE pg_catalog."default",
    display_name character varying COLLATE pg_catalog."default",
    CONSTRAINT sources_pkey PRIMARY KEY (id),
    CONSTRAINT sources_name_key UNIQUE (name)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
-- Table: data_manager.views

-- DROP TABLE IF EXISTS data_manager.views;
CREATE SEQUENCE data_manager.views_id_seq START 100;
CREATE TABLE IF NOT EXISTS data_manager.views
(
    id integer NOT NULL DEFAULT nextval('data_manager.views_id_seq'::regclass),
    source_id integer NOT NULL,
    data_type text COLLATE pg_catalog."default",
    interval_version text COLLATE pg_catalog."default",
    geography_version text COLLATE pg_catalog."default",
    version text COLLATE pg_catalog."default",
    source_url text COLLATE pg_catalog."default",
    publisher text COLLATE pg_catalog."default",
    data_table text COLLATE pg_catalog."default",
    download_url text COLLATE pg_catalog."default",
    tiles_url text COLLATE pg_catalog."default",
    start_date date,
    end_date date,
    last_updated timestamp without time zone,
    statistics jsonb,
    metadata jsonb,
    CONSTRAINT views_pkey PRIMARY KEY (id),
    CONSTRAINT views_source_data_table_uniq UNIQUE (data_table),
    CONSTRAINT views_source_id_fkey FOREIGN KEY (source_id)
        REFERENCES data_manager.sources (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
