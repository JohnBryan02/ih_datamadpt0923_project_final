--SHIFTS!!

select driver_id
,date(start_shift)
,sum(shift_duration_s/3600.0) as shift_duration
,sum(off_duty_s)*1.0/sum(shift_duration_s)*1.0 as abscence
from mart_business_operations_int.int_shift_consolidated isc 
where country_id = 3
and driver_type like '%stuart%'
and date (start_shift) between dateadd(day,convert(int,-date_part(day,current_date))-convert(int,date_part(day,dateadd(day,convert(int,-date_part(day,current_date)),current_date)))+21,current_date) and dateadd(day,convert(int,20-date_part(day,current_date)),current_date)
and online_ratio < 0.30
and lower(department_name) not in ('no_disponible', 'sesiondefeedback', 'no_laborable_retribuido', 'bcnsusupervisor')
group by 1,2
order by 1,2



--CANCELACIONES QUE NOS INTERESAN
--CAMBIAR EL NOMBRE DE LAS COLUMNAS
select driver_id 
, delivery_id
, zone_name 
, date(pu_tw_start)
, del_cancelation_reason
, del_accepted_at 
, del_canceled_at 
from mart_business_operations_int.int_delivery_consolidated idc
where country_id = 3
and driver_type like '%stuart%'
and is_on_shift = true 
and delivery_status = 'canceled'
and date(pu_tw_start) between date(getdate())-61 and date(getdate())-1
and del_cancelation_reason in ('Lack mandatory equipment', 'Late / Unresponsive', 'Refused delivery', 'Equipment failure')
order by 1,7


with
-- PERFORMANCE --
performance as (
select 
driver_id,
"week",
sum(st_deliveries) as performance_deliveries,
sum(picking_time_seconds) as picking_time ,
sum(waiting_time_at_pu_seconds) as wapu,
sum(delivering_time_plus_wado) - sum(waiting_time_at_do_seconds) as delivering_time,
sum(waiting_time_at_do_seconds) as wado,
sum(distance_to_pu) as distance_to_pu,
sum(distance_to_do) as distance_to_do
from
        (SELECT
                driver_id,
                date_part(week,pu_tw_start) as "week",
        coalesce(stack_id,text(delivery_id)) as st_id,
        count(delivery_id) as st_deliveries,
        count(do_almost_arrived_at) as do_check,
        count(pu_almost_arrived_at) as pu_check,
        max(picking_time_seconds) as picking_time_seconds,
        max(waiting_time_at_pu_seconds) as waiting_time_at_pu_seconds,
        max(datediff('s',pu_succeeded_at,COALESCE(del_succeeded_at,del_canceled_at))) as delivering_time_plus_wado,
        sum(waiting_time_at_do_seconds) as waiting_time_at_do_seconds,
        max(distance_to_pu) as distance_to_pu,
        sum(distance_to_do) as distance_to_do 
        from
        mart_business_operations_int.int_delivery_consolidated idc
        where 
        country_id = 3
        and idc.is_invoiced 
        and date(pu_tw_start) >  date(getdate() - datepart(dw, date(getdate())) - 60)
        and date(pu_tw_start) <= date(getdate() - datepart(dw, date(getdate()))     )
    group by
            1,2,3)
where 
        st_deliveries = pu_check 
        and st_deliveries = do_check
group by 
        1,2),
-- DELIVERIES --
deliveries as (
select 
        driver_id,
        "week",
        transport_type,
        driver_type,
    sum(deliveries_invoiced) as deliveries_invoiced,
    sum(E2E_invoiced) as E2E_invoiced,
    sum(client_price_invoiced) as client_price_invoiced,
    sum(driver_earnings_invoiced) as driver_earnings_invoiced,
    sum(distance_to_pu_invoiced) as distance_to_pu_invoiced,
    sum(distance_to_do_invoiced) as distance_to_do_invoiced,
    sum(deliveries_not_invoiced) as deliveries_not_invoiced,
    sum(E2E_not_invoiced) as E2E_not_invoiced,
    sum(deliveries_succeeded) as deliveries_succeeded,
    sum(E2E_succeeded) as E2E_succeeded,
    sum(client_price_succeeded) as client_price_succeeded,
        sum(driver_earnings_succeeded) as driver_earnings_succeeded,
        sum(distance_to_pu_succeeded) as distance_to_pu_succeeded,
        sum(distance_to_do_succeeded) as distance_to_do_succeeded
from
        (select
        driver_id,
        date_part(week,pu_tw_start) as "week",
        transport_type,
        driver_type,
    coalesce(stack_id,text(delivery_id)) as st_id,
    sum(case when is_invoiced  = true then 1 else 0 end) as deliveries_invoiced,
    max(case when is_invoiced  = true then datediff('s',del_accepted_at,COALESCE(del_succeeded_at,del_canceled_at)) else 0 end) as E2E_invoiced,
    sum(case when is_invoiced  = true then client_price else 0 end) as client_price_invoiced,
    sum(case when is_invoiced  = true then driver_earnings_total else 0 end) as driver_earnings_invoiced,
    max(case when is_invoiced  = true then distance_to_pu else 0 end) as distance_to_pu_invoiced,
    sum(case when is_invoiced  = true then distance_to_do else 0 end) as distance_to_do_invoiced,
    sum(case when is_invoiced  = false then 1 else 0 end) as deliveries_not_invoiced,
    max(case when is_invoiced  = false then datediff('s',del_accepted_at,COALESCE(del_succeeded_at,del_canceled_at)) else 0 end) as E2E_not_invoiced,
    sum(case when delivery_status  = 'succeeded' then 1 else 0 end) as deliveries_succeeded,
    max(case when delivery_status  = 'succeeded' then datediff('s',del_accepted_at,COALESCE(del_succeeded_at,del_canceled_at)) else 0 end) as E2E_succeeded,
    sum(case when delivery_status  = 'succeeded' then client_price else 0 end) as client_price_succeeded,
    sum(case when delivery_status  = 'succeeded' then driver_earnings_total else 0 end) as driver_earnings_succeeded,
    max(case when delivery_status  = 'succeeded' then distance_to_pu else 0 end) as distance_to_pu_succeeded,
    sum(case when delivery_status  = 'succeeded' then distance_to_do else 0 end) as distance_to_do_succeeded
        from
            mart_business_operations_int.int_delivery_consolidated idc
        where 
                   country_id = 3
            and date(pu_tw_start) >  date(getdate() - datepart(dw, date(getdate())) - 60)
            and date(pu_tw_start) <= date(getdate() - datepart(dw, date(getdate()))     )
            and driver_id is not null
            and driver_subtype <> 'self-employed'
        group by
            1,2,3,4,5)
group by 
        1, 2, 3, 4),
-- CANCELLATIONS --
cancellations as (        
select
        ic.driver_id,
        date_part(week,ic.inv_received_at) as "week",
        count(distinct(ic.invitation_id)) as invitations,
        sum(ic.non_accepted_or_cancel) as non_commit,
        sum(ic.lack_equipment) as lack_equipment,
        sum(ic.late_unresponsive) as late_unresponsive,
        sum(ic.refused_delivery) as refused_delivery,
        sum(ic.equipment_failure) as equipment_failure
from 
        mart_business_operations_int.int_invitation_consolidated ic
        left join mart_business_operations_int.int_delivery_consolidated idc on idc.delivery_id = ic.delivery_id 
where
        ic.country_id = 3
        and date(ic.inv_received_at) >  date(getdate() - datepart(dw, date(getdate())) - 60)
        and date(ic.inv_received_at) <= date(getdate() - datepart(dw, date(getdate()))     )
        and idc.is_on_shift = true
group by
        1, 2
order by
        1, 2),
-- DRIVER --
driver as (
select
        dl.driver_id,
        datediff(week,min(dl.onboarded_at),getdate()) as tenure
from 
        mart_business_operations_int.driver_lifecycle dl
where
        country_id = 3
group by
        1),
-- SHIFTS --
shifts as (
select 
        driver_id,
        date_part(week,starts_date) as "week",
        round(sum(case when lower(department_name) not in ('no_laborable_retribuido','no_disponible') then shift_duration_s else 0 end)/3600.0,2) as assigned_hours,
        round(sum(online_s_in_shift_area+online_s_outside_shift_area)/3600.0,2) as online_hours,
        round(sum(overtime_s)/3600.0,2) as extra_time,
        round(sum(case when lower(department_name) = 'no_laborable_retribuido' then shift_duration_s else 0 end)/3600.0,2) as no_laborable_retribuido,
        round(sum(case when lower(department_name) = 'no_disponible' then shift_duration_s else 0 end)/3600.0,2) as no_disponible,
        count(case when lower(department_name) not in('no_laborable_retribuido','no_disponible') then staffomatic_shift_id else null end) as assigned_shifts,
        sum(case when is_in_area_begin = true and lower(department_name) not in('no_laborable_retribuido','no_disponible') then 1 else 0 end) as is_in_area_begin,
        sum(case when lower(department_name) not in('no_laborable_retribuido','no_disponible') then no_show else 0 end) as no_shows
from 
    mart_business_operations_int.int_shift_consolidated isc 
where
    country_id = 3
    and date(starts_date) > date(getdate() - datepart(dw, date(getdate())) -60)
    and date(starts_date) <= date(getdate() - datepart(dw, date(getdate())))
group by
        1,2)
select
        sht.driver_id,
        sht."week",
        --driver_type,
        del.transport_type,
        tenure,
        deliveries_invoiced,
        E2E_invoiced,
        client_price_invoiced,
        driver_earnings_invoiced,
        deliveries_not_invoiced,
        E2E_not_invoiced,
        distance_to_pu_invoiced,
        distance_to_do_invoiced,
        deliveries_succeeded,
        E2E_succeeded,
         client_price_succeeded,
        driver_earnings_succeeded,
        distance_to_pu_succeeded,
        distance_to_do_succeeded,
        performance_deliveries,
        picking_time,
        wapu,
        delivering_time,
        wado,
        distance_to_pu,
        distance_to_do,
        assigned_hours,
        online_hours,
        extra_time,
        no_laborable_retribuido,
        no_disponible,
        assigned_shifts,
        is_in_area_begin,
        no_shows,
        invitations,
        non_commit,
        lack_equipment,
        late_unresponsive,
        refused_delivery,
        equipment_failure
from
        shifts sht
        left join performance per on per.driver_id = sht.driver_id and per."week" = sht."week"
        left join driver drv on drv.driver_id = sht.driver_id
        left join deliveries del on del.driver_id = sht.driver_id and del."week" = sht."week"
        left join cancellations cll on cll.driver_id = sht.driver_id and cll."week" = sht."week"
        left join modeled_core.driver d on d.driver_id = sht.driver_id and d.is_last_record = true
where 
                state = 'active'
                and driver_type in ('stuart_logistica')
order by 
        1,2
        
        
        
        