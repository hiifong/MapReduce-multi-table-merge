create external table if not exists student(
    name string,
    course string,
    score int
)
row format delimited fields terminated by "," location "/output/";

create temporary function myfun as "cc.hiifong.bigdata.CustomFunction" using jar "/root/BigData-1.0.0.jar";

select myfun(course) from student limit 1;
