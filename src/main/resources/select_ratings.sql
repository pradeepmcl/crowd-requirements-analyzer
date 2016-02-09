/* All ratings by users in a batch */
select user_id, requirement_id, novelty, feasibility
  into outfile '/tmp/ratings_batch_3.txt'
  fields terminated by ','
  optionally enclosed by '"'
  lines terminated by '\n'
from requirements_ratings RR 
join 
  (select id from users where completion_code is not null and created_batch = 3) as U 
on RR.user_id = U.id;

/* Users'self ratings in a batch */
select user_id, requirement_id, novelty, feasibility
  into outfile '/tmp/ratings_self_batch_3.txt'
  fields terminated by ','
  optionally enclosed by '"'
  lines terminated by '\n'
from requirements_ratings RR 
join 
  (select id from users where completion_code is not null and created_batch = 3) as U 
on RR.user_id = U.id
where RR.requirement_id in
  (select R1.id from requirements R1 
   join 
     (select id from users where completion_code is not null and created_batch = 3) as U1
   on U1.id = R1.user_id);
   
/* Users' ratings of others' requirements in a batch */
select user_id, requirement_id, novelty, feasibility
  into outfile '/tmp/ratings_others_batch_3.txt'
  fields terminated by ','
  optionally enclosed by '"'
  lines terminated by '\n'
from requirements_ratings RR 
join 
  (select id from users where completion_code is not null and created_batch = 3) as U 
on RR.user_id = U.id
where RR.requirement_id not in
  (select R1.id from requirements R1 
   join 
     (select id from users where completion_code is not null and created_batch = 3) as U1
   on U1.id = R1.user_id);