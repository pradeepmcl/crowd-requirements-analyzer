DELIMITER //

DROP PROCEDURE IF EXISTS proc_select_req_ratings`;

CREATE PROCEDURE `proc_select_req_ratings` (IN param1 INT)
BEGIN
	DECLARE batch INT;
	DECLARE outFilename VARCHAR(50);
	
	SET batch = 1;
	
	WHILE batch <= 16 DO
	  SET outFilename = CONCAT('/tmp/ratings_self_batch_', batch, '.csv');
	  
	  SET @q1 = CONCAT("select user_id, requirement_id, novelty, feasibility into outfile", 
	  outFilename,
	  "fields terminated by ',' optionally enclosed by '\"' lines terminated by '\n'
    from requirements_ratings RR 
    join 
      (select id from users where completion_code is not null and created_batch = batch) as U 
    on RR.user_id = U.id
    where RR.requirement_id in
      (select R1.id from requirements R1 
       join 
         (select id from users where completion_code is not null and created_batch = batch) as U1
       on U1.id = R1.user_id)");

    prepare s1 from @q1;
    execute s1;deallocate prepare s1;	  

	END WHILE;
END //