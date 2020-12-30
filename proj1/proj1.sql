-- comp9311 19s1 Project 1
--
-- MyMyUNSW Solutions

-- made by Jiaqi Sun z5233100

-- Q1:

create or replace view room_and_room_types(id, unswid, longname)
as
select distinct r.id, r.unswid, r.longname
from   rooms r inner join room_types rt on r.rtype = rt.id
where  rt.description = 'Laboratory'
;

create or replace view Q1(unswid, longname)
as
select distinct r.unswid, r.longname
from   room_and_room_types r, classes cl 
where  cl.room = r.id and 
       cl.course in (select courses.id 
	                 from   courses, subjects sub, semesters sem
	                 where  sem.year = 2013 and sem.term = 'S1' and sub.code = 'COMP9311' and 
	                        sub.id = courses.subject and sem.id = courses.semester)
;

-- Q2:

create or replace view taught_staff(id)
as
select distinct cs.staff
from   course_enrolments ce natural join course_staff cs
where  ce.student = (select s.id
	                 from   people p natural join students s
	                 where  p.name = 'Bich Rae')
;

create or replace view Q2(unswid,name)
as
select p.unswid, p.name
from   taught_staff natural join people p
;

-- Q3:

-- use exists statement to screen the qualified student who select COMP9021 and COMP9311 in the same semester.

create or replace view intlstudents_9021_9311(student)
as
select distinct s1.id
from   students s1, course_enrolments ce, subjects sub, semesters sem1, courses c
where  s1.stype = 'intl' and ce.student = s1.id and sub.code = 'COMP9311' and sub.id = c.subject and 
       ce.course = c.id and sem1.id = c.semester and 
	   exists (select * 
	   		   from   students s2, course_enrolments ce, subjects sub, semesters sem2, courses c
	   		   where  ce.student = s2.id and sub.code = 'COMP9021' and sub.id = c.subject and ce.course = c.id and 
	   		          sem2.id = c.semester and s1.id = s2.id and sem2.id = sem1.id)
;

create or replace view Q3(unswid, name)
as
select distinct p.unswid, p.name
from   intlstudents_9021_9311 i, people p
where  i.student = p.id
;

-- Q4:

-- qualify the international students and the number of them in distinct programs.

create or replace view intl_students(project, code, name, count)
as
select distinct p.id, p.code, p.name, count(distinct s.id)
from   programs p inner join program_enrolments pe on p.id = pe.program
	   inner join students s on s.id = pe.student
where  s.stype = 'intl'
group by p.id, p.code, p.name
;

-- calculate the total number of students in different programs.

create or replace view all_students(project, count)
as
select distinct pe.program, count(distinct pe.student)
from   program_enrolments pe
group by pe.program
;

-- calculate the percent of international students in a program, and 
-- qualify the programs which percentage between 30 and 70.

create or replace view Q4(code,name)
as
select distinct i.code, i.name
from   intl_students i inner join all_students a on i.project = a.project
where  (i.count * 100/a.count) >= 30 and (i.count * 100/a.count) <= 70
;

--Q5:

-- use group statement to select the minmum mark in a course.

create or replace view all_min_marks(course, min_mark)
as
select course, min(mark)
from   course_enrolments
where  course in (
	select course
	from   course_enrolments
	group by course
	having count(mark) > 20)
group by course
;

-- select the courses which have the highest mimnum mark.
-- join with subjects and semesters to give more explicit information.

create or replace view Q5(code,name,semester)
as
select sub.code, sub.name, sem.name
from   subjects sub, semesters sem, courses c
where  sub.id = c.subject and sem.id = c.semester and 
	   c.id in (select distinct a.course 
	   	        from   all_min_marks a
		        where  a.min_mark = (select max(min_mark) 
		       					     from   all_min_marks))
;

-- Q6:

-- num1 record the number of local students who enrolled in 10S1 in stream named Chemistry.

create or replace view num1(count)
as
select count(distinct pe.student)
from   semesters s, streams st inner join stream_enrolments se on st.id = se.stream,
	   program_enrolments pe
where  s.year = 2010 and s.term = 'S1' and st.name = 'Chemistry' and
	   s.id = pe.semester and se.partof = pe.id and 
	   pe.student in (select id 
	   				  from   students 
	   				  where  stype = 'local')
;

-- num2 record the number of international students enrolled in 10S1.
-- join with the orgunits to find the id of Faculty of Engineering.

create or replace view num2(count)
as
select count(distinct pe.student)
from   semesters s, programs p inner join program_enrolments pe on p.id = pe.program,
	   orgunits o
where  s.year = 2010 and s.term = 'S1' and o.longname = 'Faculty of Engineering' and 
	   o.id = p.offeredby and s.id = pe.semester and 
	   pe.student in (select id 
	  				  from   students 
	  				  where  stype = 'intl')
;

-- num3 record the number of students who enrolled in 3978 program.
-- program code has 4-digit varchar domain.

create or replace view num3(count)
as
select count(distinct pe.student)
from   semesters s, programs p inner join program_enrolments pe on p.id = pe.program
where  s.year = 2010 and s.term = 'S1' and s.id = pe.semester and p.code = '3978'
;

create or replace view Q6(num1, num2, num3)
as
select num1.count, num2.count, num3.count
from   num1, num2, num3
;

-- Q7:

-- qualify the staff who taught at least 1 subject.
	
create or replace view staff_subject(id, sub_count)
as
select cs.staff, count(distinct s.id)
from   courses c, course_staff cs, subjects s
where  cs.course = c.id and c.subject = s.id
group by cs.staff
having count(distinct s.id) >= 1
;

-- select the staff who has a primary role in Dean.

create or replace view staff_Dean(id, starting, faculty)
as
select distinct a.staff, a.starting, o.longname
from   affiliations a, orgunits o inner join orgunit_types ot on ot.id = o.utype, 
       staff_roles sr
where  a.isprimary = 't' and ot.name = 'Faculty' and sr.name = 'Dean' and
	   a.orgunit = o.id and a.role = sr.id and 
	   a.staff in (select id 
	   			   from   staff_subject)
;

-- give more information by joining with people table.

create or replace view Q7(name, school, email, starting, num_subjects)
as
select p.name, sd.faculty, p.email, sd.starting, ss.sub_count
from   people p, staff_subject ss, staff_Dean sd
where  p.id = ss.id and ss.id = sd.id
;

-- Q8: 

-- qualify the subjects which offered at least 20 courses.

create or replace view popular_subject1(subject, course)
as
select s.id, c.id
from   subjects s, courses c
where  s.id = c.subject and 
	   s.id in (select s.id
	   			from   subjects s, courses c
	   			where  s.id = c.subject
	   			group by s.id
	   			having count(distinct c.id) >= 20)
;

-- select all the subjects which have at least 20 courses and each offering
-- course has at least 20 students.

create or replace view popular_subject2(subject)
as
select p1.subject
from   popular_subject1 p1
where  p1.course in (select ce.course
					 from   popular_subject1 p1, course_enrolments ce
					 where  ce.course = p1.course
					 group by ce.course
					 having count(distinct ce.student) >= 20)
group by p1.subject
having count(p1.course) >= 20
;

-- use || to rename the subject

create or replace view Q8(subject)
as
select s.code || ' ' || s.name AS subject
from   popular_subject2 p2, subjects s
where  p2.subject = s.id
;

-- Q9:

-- calculate the number of international students enrolled in different unit in different year.

create or replace view unitswithintl(orgunit, year, count)
as
select distinct o.id, s.year, count(distinct pe.student)
from   program_enrolments pe, programs p, orgunits o, semesters s
where  pe.program = p.id and s.id = pe.semester and p.offeredby = o.id and 
       pe.student in (select id 
			          from   students
			          where  stype = 'intl')
group by o.id, s.year
;

-- select the maximum students number in each orgunit. 

create or replace view maxorgunit(orgunit, max)
as
select orgunit, max(count)
from   unitswithintl
group by orgunit
;

-- select the year whhen each unit has the maximum international students number.

create or replace view Q9(year,num,unit)
as
select u.year, u.count, o.longname
from   unitswithintl u, orgunits o
where  o.id = u.orgunit and 
       exists (select * 
       	       from   maxorgunit m
			   where  m.orgunit = u.orgunit and u.count = m.max)
;

-- Q10:

-- calculate average mark of each qualified student who has enrolled in 3 courses.

create or replace view avg_mark(student, avg_mark)
as
select ce.student, cast(avg(ce.mark) as numeric(4,2))
from   semesters s, course_enrolments ce, courses c
where  ce.course = c.id and s.year = 2011 and s.term = 'S1' and
	   c.semester = s.id and ce.mark >= 0
group by ce.student
having count(ce.course) >= 3
order by avg(ce.mark) desc
;

-- select top 10 avg_mark and search for the corresponding students who gain this mark.

create or replace view Q10(unswid,name,avg_mark)
as
select p.unswid, p.name, a.avg_mark
from   people p, avg_mark a
where  p.id = a.student and a.avg_mark in (select avg_mark
										  from   avg_mark
										  limit  10)
order by a.avg_mark desc
;

-- Q11:

-- select the mark that qualified students receive in a course.

create or replace view stu_mark(student, course, mark)
as
select ce.student, c.id, ce.mark
from   course_enrolments ce, semesters s, courses c, people p
where  ce.mark >= 0 and ce.course = c.id and s.year = 2011 and s.term = 'S1' and
	   c.semester = s.id and ce.student = p.id and cast(p.unswid as varchar) like '313%'
;

-- calculate the number of passed course and all enrolled courses for each student, respectively.

create or replace view pass_course(student, count)
as
select student, count(course)
from   stu_mark
where  mark >= 50
group by student
;

create or replace view all_course(student, count)
as
select student, count(course)
from   stu_mark
group by student
;

-- regarding the pass rate classify the academic standing of each student.
-- use academic_standing table to give the name of the standing classified before.

create or replace view course_standing(student, standing)
as
select distinct a.student,
				(case when a.count = 1 and p.count is NULL then 2
				      when a.count > 1 and p.count is NULL then 3
				      when (p.count * 100/a.count) > 50 then 1
				      else 2 end)
				as standing
from   pass_course p right outer join all_course a on p.student = a.student
;

create or replace view Q11(unswid, name, academic_standing)
as
select p.unswid, p.name, a.standing
from   people p, course_standing cs, academic_standing a
where  p.id = cs.student and cs.standing = a.id
;

-- Q12:

-- rename the time by combining the term and year for each course.
-- select those subjects which start with COMP90

create or replace view course_year_term(code, sub_name, course, sem_name, year, term)
as
select distinct s.code, s.name, c.id, (cast(sem.year as varchar) || sem.term), sem.year, sem.term
from   subjects s, courses c, semesters sem
where  s.code like 'COMP90%' and c.subject = s.id and sem.id = c.semester and (sem.term = 'S1' or sem.term = 'S2')
;

-- find all time between 2003 and 2012 with S1 and S2 term.
-- use not exists statement to judge all the course which are offered in all time mentioned before.

create or replace view qualified_course(course, code, sub_name, year, term)
as
select distinct c1.course, c1.code, c1.sub_name, c1.year, c1.term
from   course_year_term c1
where  not exists ((select distinct cast(year as varchar) || term  
	                from   semesters 
	                where  year <= 2012 and year >= 2003 and (term = 'S1' or term = 'S2'))
                   except 
                   (select c3.sem_name 
                    from   course_year_term c3 
                    where  c3.code = c1.code))
;

-- calculate the number of students who selected this course and get a mark from the course.
-- calculate the number of students whose mark are greater than 50, which means the student passed this course.

create or replace view all_student_number(course, subject, sub_name, year, term, sum)
as
select q.course, q.code, q.sub_name, q.year, q.term, count(ce.student)
from   qualified_course q, course_enrolments ce
where  q.course = ce.course and ce.mark >= 0
group by q.course, q.code, q.sub_name, q.year, q.term
;

create or replace view pass_number(course, pass)
as
select q.course, cast(count(ce.student) as float)
from   qualified_course q, course_enrolments ce
where  q.course = ce.course and ce.mark >= 50
group by q.course
;

-- calculate pass rate of each course in each semester.
-- group by subject and seperate S1 term and S2 term in two different column.

create or replace view Q12(code, name, year, s1_ps_rate, s2_ps_rate)
as
select a.subject, a.sub_name, substring(cast(a.year as text), 3) as year, 
	   max(case when a.term = 'S1' and p.pass is NULL then 0.00
	   	        when a.term = 'S1' and p.pass >= 1 then cast(p.pass/a.sum as numeric(4, 2)) end) as s1_ps_rate,
	   max(case when a.term = 'S2' and p.pass is NULL then 0.00
	   	        when a.term = 'S2' and p.pass >= 1 then cast(p.pass/a.sum as numeric(4, 2)) end) as s2_ps_rate
from   all_student_number a left outer join pass_number p on a.course = p.course
group by a.subject, a.sub_name, substring(cast(a.year as text), 3)
order by a.subject
;
