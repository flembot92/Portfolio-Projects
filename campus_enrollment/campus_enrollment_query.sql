with ct as ( -- grabs the current term
    select term_code
    from wsrpmgr.dim_term
    where (current_term_ind = 'Y' or
            (registration_start_date >= sysdate and
            term_start_date <= sysdate)
        )
        and term_code not like '%50'      
)
,target_periods as ( -- all term codes in the past seven years with their max days since start of registration
    select term_code 
        ,max(days_since_reg_start) days_since_reg_start
    from wsrpmgr.fact_daily_section_enrollments
    where term_code >= (select max(term_code) - 700 from ct)
    group by term_code
    union all
    select '201920' term_code, 192 from dual
)
--select * from target_periods;

,periods as ( -- DSRS matches with each season's most recent term and that term's max days since start of registration,
--                    but offsets by 1 since the data warehouse updates the day after
    select distinct term_code, days_since_reg_start
        ,min(days_since_reg_start) over(partition by substr(term_code,5))-1 dsrs -- adjusts for data being gathered yesterday
    from target_periods
)
--select * from periods;

,campus as ( -- all campuses and their campus code
    select distinct campus_code,campus
    from wsrpmgr.dim_section
    
),dse as( -- daily section enrollments; one row per term per campus; 8 fields for students/FTE countings
    select
         fde.term_code
        ,case when fde.term_code in (select term_code from periods order by term_code desc fetch first 3 rows only) then 'Y' end recent_term -- need this for the dashboard
        ,dt.term_season
        ,dt.term_name
        ,nvl(ds.campus_code,'All') campus_code
        ,fde.dim_date_key
        ,fde.days_since_reg_start        
        ,count(distinct case dst.continuing_student_ind when 'N' then fde.pidm end) new_stu
        ,count(distinct case dst.continuing_student_ind when 'Y' then fde.pidm end) cont_stu
        
        ,count(distinct case when ds.budget_related_ind = 'Y' 
            and dst.continuing_student_ind = 'N' then fde.pidm end) new_br_stu
        ,count(distinct case when ds.budget_related_ind = 'Y' 
            and dst.continuing_student_ind = 'Y' then fde.pidm end) cont_br_stu
            
        ,round(sum(case dst.continuing_student_ind when 'N' then fde.credits_attempted end)/15) new_fte
        ,round(sum(case dst.continuing_student_ind when 'Y' then fde.credits_attempted end)/15) cont_fte
        
        ,round(sum(case when ds.budget_related_ind = 'Y'
            and dst.continuing_student_ind = 'N' then fde.credits_attempted end)/15) new_br_fte
        ,round(sum(case when ds.budget_related_ind = 'Y'
            and dst.continuing_student_ind = 'Y' then fde.credits_attempted end)/15) cont_br_fte
            
        ,count(distinct fde.crn) sections
        ,count(distinct case ds.budget_related_ind when 'Y' then fde.crn end) br_sections  
        ,sum(case when cross_list_group is not null then students_enrolled_group/2
            else students_enrolled_section end) enrolled
        ,sum(case when cross_list_group is not null then max_enrollment_group/2
            else max_enrollment_section end) capacity
        ,sum(students_enrolled_waitlist) waitlisted
    from wsrpmgr.fact_daily_enrollments fde
        join wsrpmgr.dim_student_term dst on dst.dim_student_term_key = fde.dim_student_term_key
        join wsrpmgr.dim_course dc using(dim_course_key)
        join wsrpmgr.dim_term dt using(dim_term_key)
        join wsrpmgr.dim_section ds using(dim_section_key)
        join periods p on fde.term_code = p.term_code
            and fde.days_since_reg_start = p.dsrs
    group by fde.term_code, dt.registration_drop_deadline, dt.term_season, dt.term_name
            ,fde.dim_date_key, fde.days_since_reg_start
        ,rollup(ds.campus_code)
)
--select* from dse;

,pct_adjustments as ( -- adjustable enrollment target percentages for each season
    select 'Fall' term_season, .925 knob from dual
    union
    select 'Spring' term_season, .925 knob from dual
    union
    select 'Summer' term_season, .925 knob from dual
    
),target as (
    select campus_code
        ,dse.term_season
        ,round(avg(new_stu)* max(knob)) new_stu_tar
        ,round(avg(cont_stu)* max(knob)) cont_stu_tar
        ,round(avg(new_br_stu)* max(knob)) new_br_stu_tar
        ,round(avg(cont_br_stu)* max(knob)) cont_br_stu_tar
        ,round(avg(new_fte)* max(knob)) new_fte_tar
        ,round(avg(cont_fte)* max(knob)) cont_fte_tar
        ,round(avg(new_br_fte)* max(knob)) new_br_fte_tar
        ,round(avg(cont_br_fte)* max(knob)) cont_br_fte_tar
    from dse 
        join pct_adjustments pct on dse.term_season = pct.term_season
    where dse.term_code not in (
            select distinct max(term_code) over(partition by substr(term_code,5)) 
            from periods
        )
        and campus_code <> 'All'
    group by campus_code ,dse.term_season
    union 
    select 'All' campus_code
        ,dse.term_season
        ,round(avg(new_stu)* max(knob)) new_stu_tar
        ,round(avg(cont_stu)* max(knob)) cont_stu_tar
        ,round(avg(new_br_stu)* max(knob)) new_br_stu_tar
        ,round(avg(cont_br_stu)* max(knob)) cont_br_stu_tar
        ,round(avg(new_fte)* max(knob)) new_fte_tar
        ,round(avg(cont_fte)* max(knob)) cont_fte_tar
        ,round(avg(new_br_fte)* max(knob)) new_br_fte_tar
        ,round(avg(cont_br_fte)* max(knob)) cont_br_fte_tar
    from dse 
        join pct_adjustments pct on dse.term_season = pct.term_season
    where dse.term_code not in (
            select distinct max(term_code) over(partition by substr(term_code,5)) 
            from periods
        )
    group by dse.term_season
),agg as(
    select a.*
        ,b.new_stu_tar,b.cont_stu_tar
        ,b.new_br_stu_tar,b.cont_br_stu_tar
        ,b.new_fte_tar,b.cont_fte_tar
        ,b.new_br_fte_tar,b.cont_br_fte_tar
    from dse a
        left join target b on a.term_season||a.campus_code
        = b.term_season||b.campus_code
)
select agg.*
    ,case when agg.campus_code = 'All' then 'All' else campus.campus end campus
    ,to_date(agg.dim_date_key,'yyyymmdd') calendar_date
    ,sysdate
from agg
    left join campus on agg.campus_code = campus.campus_code
