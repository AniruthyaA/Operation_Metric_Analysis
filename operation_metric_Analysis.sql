create database Project3_Trainity;
use project3_trainity;

create table job_data ( ds date,job_id int , actor_id int,event varchar(1000),language varchar(100),time_spent int,org varchar(1)); 

 load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/job_data.csv'
into table job_data
fields terminated by ','
lines terminated by '\n'
ignore 1 rows; 

set secure_file_priv = 'C:\Program Files\MySQL\MySQL Server 8.0\Uploads';
 select * from job_data;
INSERT INTO job_data VALUES
('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'),
('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
('2020-11-28', 23, 1005, 'transfer', 'Persian', 22, 'D'),
('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C'),
('2020-11-24', 30, 1010, 'skip', 'German', 34, 'A'),
('2020-11-23', 19, 1009, 'transfer', 'Spanish', 18, 'B'),
('2020-11-22', 17, 1011, 'decision', 'Portuguese', 27, 'C'),
('2020-11-21', 16, 1008, 'skip', 'Russian', 39, 'D'),
('2020-11-20', 15, 1012, 'transfer', 'Chinese', 12, 'A'),
('2020-11-19', 14, 1013, 'decision', 'Japanese', 54, 'B'),
('2020-11-18', 13, 1014, 'skip', 'Korean', 23, 'C'),
('2020-11-17', 12, 1015, 'transfer', 'Hindi', 46, 'D'),
('2020-11-16', 11, 1016, 'decision', 'Bengali', 67, 'A'),
('2020-11-15', 10, 1017, 'skip', 'Punjabi', 30, 'B'),
('2020-11-14', 9, 1018, 'transfer', 'Javanese', 21, 'C'),
('2020-11-13', 8, 1019, 'decision', 'Vietnamese', 42, 'D'),
('2020-11-12', 7, 1020, 'skip', 'Telugu', 53, 'A'),
('2020-11-11', 6, 1021, 'transfer', 'Marathi', 64, 'B'),
('2020-11-10', 5, 1022, 'decision', 'Tamil', 15, 'C'),
('2020-11-09', 4, 1023, 'skip', 'Urdu', 26, 'D'),
('2020-11-08', 3, 1024, 'transfer', 'Gujarati', 37, 'A'),
('2020-11-07', 2, 1025, 'decision', 'Malayalam', 48, 'B'),
('2020-11-06', 1, 1026, 'skip', 'Kannada', 59, 'C'),
('2020-11-05', 0, 1027, 'transfer', 'Odia', 70, 'D'),
('2020-11-04', 29, 1028, 'decision', 'Assamese', 81, 'A'),
('2020-11-03', 28, 1029, 'skip', 'Maithili', 92, 'B');

drop table job_data;
create table job_data(ds date,job_id int,actor_id int,event varchar(10),language varchar(10),time_spent int ,org varchar(10));
 truncate table job_data;
 
 select ds,count(job_id) as jobs_reviewed from job_data group by ds;
 
 WITH daily_event_count AS (
    SELECT
        ds,
        COUNT(*) AS event_count
    FROM
        job_data
    GROUP BY
        ds
),
rolling_avg AS (
    SELECT
        ds,
        event_count,
        AVG(event_count) OVER (
            ORDER BY ds
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_event_count
    FROM
        daily_event_count
)
SELECT
    ds,
    event_count,
    rolling_avg_event_count
FROM
    rolling_avg
ORDER BY
    ds;
    
    
    SELECT
    language,
    COUNT(*) AS event_count,
    (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM job_data)) AS percentage
FROM
    job_data
GROUP BY
    language
ORDER BY
    percentage DESC;


SELECT *, COUNT(*) AS duplicate_count
FROM job_data
GROUP BY
    ds,
    job_id,
    actor_id,
    event,
    language,
    time_spent,
    org
HAVING
    COUNT(*) > 1;

