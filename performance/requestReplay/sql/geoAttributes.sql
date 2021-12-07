SELECT
    'STATE' AS geolevel,
    state_code AS geoid,
    state_name AS geoname,
    ROUND(
      SUM(
        m.miles::NUMERIC
        * m.is_interstate::INT
        * (nhs_pct::NUMERIC / 100::NUMERIC)
      )
      , 2
    )::DOUBLE PRECISION AS interstate_miles,
    SUM((m.is_interstate AND nhs_pct > 0)::INT)::INT AS interstate_tmcs_ct,
    ROUND(
      SUM(
        m.miles::NUMERIC
        * (NOT m.is_interstate)::INT
        * (nhs_pct::NUMERIC / 100::NUMERIC)
      )
      , 2
    )::DOUBLE PRECISION AS noninterstate_miles,
    SUM(((NOT m.is_interstate) AND (nhs_pct > 0))::INT)::INT AS noninterstate_tmcs_ct,
    ST_AsText(
      ST_Extent(m.bounding_box)
    ) AS bounding_box,
    NULL AS population_info,
    JSON_AGG(DISTINCT m.state_code ORDER BY m.state_code) AS states,
    2020 AS year
  FROM tmc_metadata_2020 AS m
    
  WHERE ( state_code IN ('36') )
  GROUP BY state_code, state_name

UNION ALL

SELECT
    'COUNTY' AS geolevel,
    county_code AS geoid,
    county_name AS geoname,
    ROUND(
      SUM(
        m.miles::NUMERIC
        * m.is_interstate::INT
        * (nhs_pct::NUMERIC / 100::NUMERIC)
      )
      , 2
    )::DOUBLE PRECISION AS interstate_miles,
    SUM((m.is_interstate AND nhs_pct > 0)::INT)::INT AS interstate_tmcs_ct,
    ROUND(
      SUM(
        m.miles::NUMERIC
        * (NOT m.is_interstate)::INT
        * (nhs_pct::NUMERIC / 100::NUMERIC)
      )
      , 2
    )::DOUBLE PRECISION AS noninterstate_miles,
    SUM(((NOT m.is_interstate) AND (nhs_pct > 0))::INT)::INT AS noninterstate_tmcs_ct,
    ST_AsText(
      ST_Extent(m.bounding_box)
    ) AS bounding_box,
    NULL AS population_info,
    JSON_AGG(DISTINCT m.state_code ORDER BY m.state_code) AS states,
    2020 AS year
  FROM tmc_metadata_2020 AS m
    
  WHERE ( county_code IN ('36005','36091','36033','36025','36059','36007','36045','36051','36121','36105','36065','36001','36069','36087','36101','36123','36011','36027','36095','36023','36107','36097','36085','36081','36119','36009','36047','36075','36083','36079','36057','36037','36053','36019','36029','36073','36013','36055','36113','36049','36021','36031','36041','36061','36111','36103','36017','36077','36109','36035','36089','36099','36043','36039','36063','36067','36015','36093','36117','36071','36115','36003') )
  GROUP BY county_code, county_name

UNION ALL

SELECT
    'MPO' AS geolevel,
    mpo_code AS geoid,
    mpo_name AS geoname,
    ROUND(
      SUM(
        m.miles::NUMERIC
        * m.is_interstate::INT
        * (nhs_pct::NUMERIC / 100::NUMERIC)
      )
      , 2
    )::DOUBLE PRECISION AS interstate_miles,
    SUM((m.is_interstate AND nhs_pct > 0)::INT)::INT AS interstate_tmcs_ct,
    ROUND(
      SUM(
        m.miles::NUMERIC
        * (NOT m.is_interstate)::INT
        * (nhs_pct::NUMERIC / 100::NUMERIC)
      )
      , 2
    )::DOUBLE PRECISION AS noninterstate_miles,
    SUM(((NOT m.is_interstate) AND (nhs_pct > 0))::INT)::INT AS noninterstate_tmcs_ct,
    ST_AsText(
      ST_Extent(m.bounding_box)
    ) AS bounding_box,
    NULL AS population_info,
    JSON_AGG(DISTINCT m.state_code ORDER BY m.state_code) AS states,
    2020 AS year
  FROM tmc_metadata_2020 AS m
    
  WHERE ( mpo_code IN ('09197504','09198100','34198200','36196500','36197200','36197300','36197401','36197402','36197500','36197700','36198201','36198202','36198203','36198204','36199200','36200300','36201400') )
  GROUP BY mpo_code, mpo_name

UNION ALL

SELECT
    'UA' AS geolevel,
    ua_code AS geoid,
    ua_name AS geoname,
    ROUND(
      SUM(
        m.miles::NUMERIC
        * m.is_interstate::INT
        * (nhs_pct::NUMERIC / 100::NUMERIC)
      )
      , 2
    )::DOUBLE PRECISION AS interstate_miles,
    SUM((m.is_interstate AND nhs_pct > 0)::INT)::INT AS interstate_tmcs_ct,
    ROUND(
      SUM(
        m.miles::NUMERIC
        * (NOT m.is_interstate)::INT
        * (nhs_pct::NUMERIC / 100::NUMERIC)
      )
      , 2
    )::DOUBLE PRECISION AS noninterstate_miles,
    SUM(((NOT m.is_interstate) AND (nhs_pct > 0))::INT)::INT AS noninterstate_tmcs_ct,
    ST_AsText(
      ST_Extent(m.bounding_box)
    ) AS bounding_box,
    NULL AS population_info,
    JSON_AGG(DISTINCT m.state_code ORDER BY m.state_code) AS states,
    2020 AS year
  FROM tmc_metadata_2020 AS m
    
  WHERE ( ua_code IN ('00970','07732','10162','11350','27118','33598','41914','45262','63217','71803','75664','79633','86302','89785','92674','99998','99999') )
  GROUP BY ua_code, ua_name

UNION ALL

SELECT
    'REGION' AS geolevel,
    region_code AS geoid,
    region_name AS geoname,
    ROUND(
      SUM(
        m.miles::NUMERIC
        * m.is_interstate::INT
        * (nhs_pct::NUMERIC / 100::NUMERIC)
      )
      , 2
    )::DOUBLE PRECISION AS interstate_miles,
    SUM((m.is_interstate AND nhs_pct > 0)::INT)::INT AS interstate_tmcs_ct,
    ROUND(
      SUM(
        m.miles::NUMERIC
        * (NOT m.is_interstate)::INT
        * (nhs_pct::NUMERIC / 100::NUMERIC)
      )
      , 2
    )::DOUBLE PRECISION AS noninterstate_miles,
    SUM(((NOT m.is_interstate) AND (nhs_pct > 0))::INT)::INT AS noninterstate_tmcs_ct,
    ST_AsText(
      ST_Extent(m.bounding_box)
    ) AS bounding_box,
    NULL AS population_info,
    JSON_AGG(DISTINCT m.state_code ORDER BY m.state_code) AS states,
    2020 AS year
  FROM tmc_metadata_2020 AS m
    LEFT OUTER JOIN (
      SELECT
          name AS region_name,
          fips_code AS county_code
        FROM ny.nysdot_regions AS x
          INNER JOIN ny.nysdot_region_names AS y
            USING (region)
    ) AS r
      USING ( county_code )
  WHERE ( region_code IN ('1','10','11','2','3','4','5','6','7','8','9') )
  GROUP BY region_code, region_name
