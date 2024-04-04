with baseline as(
        select
             academic_year
            ,term_code
            ,term_start_date
            ,term_end_date
        from wsrpmgr.dim_term dt
        where term_start_date <= sysdate
            and dt.academic_year not in ('Unknown','None')
            and term_code not like '%50'
    )
,ay_raw as( -- fetches the current academic year and previous 5
        select
             distinct academic_year
            ,min(term_start_date) ay_start_date
            ,max(term_end_date) ay_end_date
        from baseline
        group by academic_year
        order by 1 desc
        fetch first 6 rows only
    )

,ay as( -- adds artificial start and end dates for current and following year which will be important later for transfer records
    select
         academic_year
        ,ay_start_date
        ,case when to_char(ay_end_date,'MON') <> 'AUG' then ay_end_date else lag(ay_start_date) over(order by academic_year desc)-1 end ay_end_date
        ,lag(ay_start_date) over(order by academic_year desc) ny_start_date
        ,case when to_char(lag(ay_end_date) over (order by academic_year desc),'MON') <> 'AUG' then lag(ay_end_date) over (order by academic_year desc)
            else lag(ay_start_date,2) over(order by academic_year desc)-1 end ny_end_date
    from ay_raw
    )

,student_pop as( -- PIDM of all students who've either been enrolled or graduated in the pre-determined academic years
        select distinct pidm
        from wsrpmgr.fact_slcc_course_history fsch
            join wsrpmgr.dim_term dt using(dim_term_key)
            join ay on ay.academic_year = dt.academic_year
        union
        select distinct pidm
        from wsrpmgr.fact_slcc_grad_outcomes fsgo
            join wsrpmgr.dim_term dt using(dim_term_key)
            join ay on ay.academic_year = dt.academic_year
        where ug_ap_graduated_ind = 'Y' or sat_graduated_ind = 'Y'
    )

,cross_join as( -- each PIDM now has 6 rows, one per academic year
        select *
        from student_pop
        cross join ay
    )

,enrollments as ( -- details on program for all students enrolled in the pre-determined academic years
        select
             distinct fsch.pidm
            ,fsch.term_code
            ,dt.term_name
            ,dt.term_start_date
            ,dt.academic_year
            ,dpost.term_area_of_study area_of_study
            ,case when instr(term_program_name, ':') > 0 then substr(term_program_name, 1, instr(term_program_name, ':') - 1) else term_program_name end program
            ,dpost.term_degree_code degree_code
            ,term_major_cip cip
            ,sum(gpa_quality_points * gpa_credits) over (partition by fsch.pidm, fsch.term_code) quality_points_term
            ,sum(gpa_credits) over (partition by fsch.pidm, fsch.term_code) gpa_credits_term
            ,sum(gpa_quality_points * gpa_credits) over (partition by fsch.pidm, dt.academic_year) quality_points_ay
            ,sum(gpa_credits) over (partition by fsch.pidm, dt.academic_year) gpa_credits_ay
            ,dense_rank() over (partition by fsch.pidm, dt.academic_year order by dt.term_code desc) seq -- this gets the student's latest program within an academic year
        from wsrpmgr.fact_slcc_course_history fsch
            join wsrpmgr.dim_student_term dst using (dim_student_term_key)
            join wsrpmgr.dim_term dt using (dim_term_key)
            join wsrpmgr.dim_pgm_of_study_term dpost on dpost.dim_pgm_of_study_term_key = dst.dim_pgm_of_study_term_key
            join ay on ay.academic_year = dt.academic_year
    )

,grads as( -- details on program for all students graduated in the pre-determined academic years, along with alumni wages
        select
             distinct fsgo.pidm
            ,fsgo.term_code grad_term_code
            ,dt.term_name grad_term_name
            ,fsgo.graduation_date grad_date
            ,dt.academic_year grad_year
            ,dpos.area_of_study grad_area_of_study
            ,case when instr(program_name, ':') > 0 then substr(program_name, 1, instr(program_name, ':') - 1) else program_name end grad_program
            ,dpos.degree_code grad_degree_code
            ,dpos.major_cip grad_cip
            ,sum(cum_institution_gpa * cum_institution_credits) over (partition by fsgo.pidm, fsgo.term_code) grad_quality_points
            ,sum(cum_institution_credits) over (partition by fsgo.pidm, fsgo.term_code) grad_gpa_credits
            ,substr(quarter,0,4) wage_year
            ,round(sum(wages) over (partition by fsw.pidm, substr(quarter,1,4))
                 * (4/count(distinct quarter) over (partition by fsw.pidm, substr(quarter,0,4)))) adjusted_annual_wages
            ,round((fsgo.graduation_date - dt1.term_start_date)/365.25,2) years_to_completion
        from wsrpmgr.fact_slcc_grad_outcomes fsgo
            join wsrpmgr.dim_student ds using(dim_student_key)
            left join wsrpmgr.fact_student_wages fsw on fsgo.pidm = fsw.pidm and substr(fsw.quarter,0,4) = to_char(fsgo.graduation_date, 'YYYY') + 4
            join wsrpmgr.dim_term dt on fsgo.term_code = dt.term_code and dt.academic_year not in ('Unknown','None')
            join wsrpmgr.dim_program_of_study dpos using (dim_program_of_study_key)
            join ay on ay.academic_year = dt.academic_year
            LEFT join wsrpmgr.dim_term    dt1 ON ds.first_term_non_concurrent = dt1.term_code
        where ug_ap_graduated_ind = 'Y' or sat_graduated_ind = 'Y'
    )
    
,transfers as( -- details on transfer records for all students in the pre-determined academic years
        select
             distinct fne.pidm
            ,fne.enrollment_begin_date transfer_date
            ,di.institution_level
        from wsrpmgr.fact_nsc_enrollments fne
            join wsrpmgr.dim_institution di using(dim_institution_key)
            join ay on to_char(fne.enrollment_begin_date, 'YYYY') >= substr(ay.academic_year,1,4)
        where fne.opeid not in ('005220','005221')
    )
,combine as( -- start with all PIDMs from the cross join, then tack on any available records from enrollments, grads, and transfers
        select
             distinct cj.pidm
            ,cj.academic_year
            ,e.area_of_study
            ,e.program
            ,e.degree_code
            ,e.cip
            ,e.quality_points_ay quality_points
            ,e.gpa_credits_ay gpa_credits
            ,g.grad_date
            ,g.grad_term_code
            ,g.grad_area_of_study
            ,g.grad_program
            ,g.grad_degree_code
            ,g.grad_cip
            ,g.grad_quality_points
            ,g.grad_gpa_credits
            ,g.wage_year
            ,g.adjusted_annual_wages
            ,case when adjusted_annual_wages between 0 and 24999 then '$0-25k'
                  when adjusted_annual_wages between 25000 and 49999 then '$25k-50k'
                  when adjusted_annual_wages between 50000 and 74999 then '$50k-75k'
                  when adjusted_annual_wages between 75000 and 99999 then '$75k-100k'
                  when adjusted_annual_wages >= 100000 then '$100k +'
                  else 'None' end wage_bin
            ,g.years_to_completion
            ,min(tay.transfer_date) over (partition by cj.pidm, cj.academic_year) transfer_date_this_year
            ,min(tny.transfer_date) over (partition by cj.pidm, cj.academic_year) transfer_date_next_year
        from cross_join cj
            left join ay on cj.academic_year = ay.academic_year
            left join enrollments e on e.pidm = cj.pidm and e.academic_year = cj.academic_year and seq = 1
            left join grads g on g.pidm = cj.pidm and g.grad_year = cj.academic_year
            left join transfers tay on tay.pidm = cj.pidm and tay.transfer_date between ay.ay_start_date and ay.ay_end_date
            left join transfers tny on tny.pidm = cj.pidm and tny.transfer_date between ay.ny_start_date and ay.ny_end_date
    )
    
,calc as( -- adding demographics and calculations for retention
        select c.*
            ,row_number() over (partition by c.pidm, c.academic_year, c.program order by c.degree_code) enrolled_seq
            ,ds.first_term_non_concurrent
            ,dt.term_start_date first_term_nc_start_date
            ,ds.gender
            ,ds.race
            ,case ds.hispanic_ind when 'Y' then 'Hispanic or Latinx'
                               when 'N' then 'Not Hispanic or Latinx'
                               else 'Prefer not to Say'
                end hispanic_origin
            ,case ds.first_generation_ind when 'Y' then 'Yes'
                                          when 'N' then 'No'
                                          when 'U' then 'Unknown'
                end first_generation            
            ,case when program is null then 'Not Enrolled'
                  when program is not null and lead(c.academic_year) over (partition by c.pidm order by c.academic_year) = c.academic_year then 'Same Year'
                  when program is not null and lead(c.program) over (partition by c.pidm order by c.academic_year) is not null
                    and substr(lead(c.academic_year) over (partition by c.pidm order by c.academic_year),1,4) = substr(c.academic_year,1,4) + 1 then 'Yes'
                  else 'No'
                 end retained_numerator
            ,case 
                  when program is null then 'Not Enrolled'
                  when program is not null and lead(c.academic_year) over (partition by c.pidm order by c.academic_year) = c.academic_year then 'Same Year'
                  when program is not null and lead(c.program) over (partition by c.pidm order by c.academic_year) is not null
                    and substr(lead(c.academic_year) over (partition by c.pidm order by c.academic_year),1,4) = substr(c.academic_year,1,4) + 1 then 'Yes'
                  when grad_program is not null then 'No'
                  when transfer_date_next_year is not null then 'No'
                  else 'Yes'
                end retained_denominator
        from combine c
            left join wsrpmgr.dim_student ds on c.pidm = ds.pidm
            left join wsrpmgr.dim_term dt on ds.first_term_non_concurrent = dt.term_code
    )

,agg_base as( -- this should give us a template for the grain we want; one row per year/program/demographics/wage_bin
-- in other words, this is giving us every possible combination of year/program/demographics regardless of enrolled vs grads. Then we'll tack on the numbers for both
        select
             distinct academic_year
            ,area_of_study
            ,program
            ,degree_code
            ,cip
            ,gender
            ,race
            ,hispanic_origin
            ,first_generation
            ,wage_bin
        from calc
            union
        select
             distinct academic_year
            ,grad_area_of_study
            ,grad_program
            ,grad_degree_code
            ,grad_cip
            ,gender
            ,race
            ,hispanic_origin
            ,first_generation
            ,wage_bin
        from calc
    )    
    
,agg_enrollments as( -- aggregating enrollments with the relevant metrics so it can fit into agg_base
        select
             c.academic_year
            ,c.area_of_study
            ,c.program
            ,c.degree_code
            ,c.cip
            ,c.gender
            ,c.race
            ,c.hispanic_origin
            ,c.first_generation
            ,c.wage_bin
            ,sum(case when enrolled_seq = 1 then c.quality_points end) quality_points
            ,sum(c.quality_points) quality_points_v2
            ,sum(distinct case when enrolled_seq = 1 then c.quality_points end) quality_points_v3
            ,sum(case when enrolled_seq = 1 then c.gpa_credits end) gpa_credits
            ,count(distinct case when c.program is not null and enrolled_seq = 1 then c.pidm end) enrolled_headcount
            ,count(distinct case when c.retained_numerator = 'Yes' then c.pidm end) retained_numerator
            ,count(distinct case when c.retained_denominator = 'Yes' then c.pidm end) retained_denominator
        from calc c
        group by
             c.academic_year
            ,c.area_of_study
            ,c.program
            ,c.degree_code
            ,c.cip
            ,c.gender
            ,c.race
            ,c.hispanic_origin
            ,c.first_generation
            ,c.wage_bin
    )

,agg_grads as( -- aggregating grads with the relevant metrics so it can fit into agg_base
        select
             academic_year
            ,grad_area_of_study
            ,grad_program
            ,grad_degree_code
            ,grad_cip
            ,gender
            ,race
            ,hispanic_origin
            ,first_generation
            ,wage_bin
            ,sum(grad_quality_points) grad_quality_points
            ,sum(grad_gpa_credits) grad_gpa_credits
            ,count(case when grad_program is not null then pidm end) grad_headcount
            ,sum(years_to_completion) years_to_completion
        from calc
        group by
             academic_year
            ,grad_area_of_study
            ,grad_program
            ,grad_degree_code
            ,grad_cip
            ,gender
            ,race
            ,hispanic_origin
            ,first_generation
            ,wage_bin
    )

,agg_full as( -- this is where we should have accurate counts
        select
             b.academic_year "Academic Year"
            ,b.area_of_study "Area of Study"
            ,b.program "Program"
            ,b.degree_code "Degree Code"
            ,b.cip "CIP"
            ,b.gender "Gender"
            ,b.race "Race"
            ,b.hispanic_origin "Hispanic Origin"
            ,b.first_generation "First Generation"
            ,b.wage_bin "Wage Bin"
            ,e.quality_points "Quality Points"
            ,e.gpa_credits "GPA Credits"
            ,e.enrolled_headcount "Enrolled Headcount"
            ,e.retained_numerator "Retained Numerator"
            ,e.retained_denominator "Retained Denominator"
            ,g.grad_headcount "Grad Headcount"
            ,g.grad_quality_points "Grad Quality Points"
            ,g.grad_gpa_credits "Grad GPA Credits"
            ,g.years_to_completion "Sum of Years To Completion"
        from agg_base b
            left join agg_enrollments e on e.academic_year = b.academic_year
                                       and e.area_of_study = b.area_of_study
                                       and e.program = b.program
                                       and e.degree_code = b.degree_code
                                       and e.cip = b.cip
                                       and e.gender = b.gender
                                       and e.race = b.race
                                       and e.hispanic_origin = b.hispanic_origin
                                       and e.first_generation = b.first_generation
                                       and e.wage_bin = b.wage_bin
            left join agg_grads g on b.academic_year = g.academic_year
                                 and b.area_of_study = g.grad_area_of_study
                                 and b.program = g.grad_program
                                 and b.degree_code = g.grad_degree_code
                                 and b.cip = g.grad_cip
                                 and b.gender = g.gender
                                 and b.race = g.race
                                 and b.hispanic_origin = g.hispanic_origin
                                 and b.first_generation = g.first_generation
                                 and b.wage_bin = g.wage_bin
    )
   
,final as(
        select agg_full.*
            ,'Total' "Total"
            ,case "Wage Bin" when '$0-25k' then 1
                           when '$25k-50k' then 2
                           when '$50k-75k' then 3
                           when '$75k-100k' then 4
                           when '$100k +' then 5
                           when 'None' then 6
                    end "Wage Bin Sort"
            ,dense_rank() over (order by "Academic Year" desc) "AY Recency"
            ,sysdate "As of Date"
        from agg_full
    )
select * from final
