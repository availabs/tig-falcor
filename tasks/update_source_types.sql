

update public.sources set 
    type = 
	case 
		when lower(name) like '%sed%taz%' then 'sed_taz'
		when lower(name) like '%sed%county%' then 'sed_county'
		when lower(name) like '%acs/census%' then 'acs_census'
		when lower(name) like '%tip%' then 'tip_projects'
		when lower(name) like '%transcom%' then 'transcom' 
		when lower(name) like '%bpm%' then 'bpm_performance' 
		when lower(name) like '%rtp%' then 'rtp_projects' 
		when lower(name) like '%hub bound%' then 'hub_bound_travel_data' 
		when lower(name) like '%upwp pro%' then 'upwp_projects' 
		when lower(name) like '%npmrds%' then 'npmrds' 
		else 'no_type'
	end;

-- SELECT 
-- 	id, 
-- 	name, 
-- 	case 
-- 		when lower(name) like '%sed%taz%' then 'sed_taz'
-- 		when lower(name) like '%sed%county%' then 'sed_county'
-- 		when lower(name) like '%acs/census%' then 'acs_census'
-- 		when lower(name) like '%tip%' then 'tip_projects'
-- 		when lower(name) like '%transcom%' then 'transcom' 
-- 		when lower(name) like '%bpm%' then 'bpm_performance' 
-- 		when lower(name) like '%rtp%' then 'rtp_projects' 
-- 		when lower(name) like '%hub bound%' then 'hub_bound_travel_data' 
-- 		when lower(name) like '%upwp pro%' then 'upwp_projects' 
-- 		when lower(name) like '%npmrds%' then 'npmrds' 
-- 		else 'no_type'
-- 	end as test_type
-- 	FROM public.sources
