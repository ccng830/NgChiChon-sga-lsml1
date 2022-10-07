with 
error_time_table as (
	select user_id, session_id, min(timestamp) as error_timestamp from clickstream
	where upper(event_type) like "%ERROR%"
	group by user_id, session_id
),
filter_err_time_table as (
	select cs.* from clickstream as cs
	left join error_time_table as ett
	on cs.user_id = ett.user_id and cs.session_id = ett.session_id
	where cs.timestamp <= coalesce(ett.error_timestamp, cs.timestamp) and cs.event_type = "page"
),
drop_duplicate_table as(
	select DISTINCT * from filter_err_time_table as fett
	order by fett.user_id, fett.session_id, fett.timestamp
),
routes_table as (
	select user_id, session_id, concat_ws('-', COLLECT_LIST(event_page)) as route from drop_duplicate_table
	group by user_id, session_id
) select route, count(*) as counts from routes_table group by route order by counts desc limit 30;