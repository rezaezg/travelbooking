-- What is the monthly count of unique Users (headcount)
-- who have made a booking for the last 6 months?
SELECT 
     d."date" , COUNT(DISTINCT f.user_id) AS unique_users
FROM 
    fact_booking f
JOIN 
    dim_date d
ON 
    f.created_date = d.date
WHERE 
    d.date >= ('2023-05-31'::date - INTERVAL '6 months')
GROUP BY 
     d.date
ORDER BY 
    d.date;
    
   
--How many users need more than 30 days to make their first booking,
-- and from which company are those users ?
   
   
   SELECT 
    fact_booking.user_id, 
    fact_booking.company_id,
    dim_company.company_name 
FROM 
(
    SELECT 
        user_id, 
        company_id,
        MIN(created_date) as first_booking_date
    FROM 
        fact_booking
    GROUP BY 
        user_id, 
        company_id
) AS fact_booking
JOIN 
    dim_user
ON 
    fact_booking.user_id = dim_user."userID"
JOIN 
    dim_company
ON 
    fact_booking.company_id = dim_company.company_id
WHERE 
    (fact_booking.first_booking_date - dim_user.created) > 30 ;
    
   
   
--What is the daily 7 day rolling total booking amount for March 2023?

SELECT
  d1.date AS start_date,
  d2.date AS end_date,
  COUNT(f.booking_id) AS booking_count
FROM
  dim_date d1
JOIN
  dim_date d2 ON d2.date = d1.date + INTERVAL '6 days'
LEFT JOIN
  fact_booking f ON f.created_date >= d1.date AND f.created_date <= d2.date
WHERE
  d1.date >= '2023-03-01' AND d1.date <= '2023-03-31' -- Adjust the end date as needed
GROUP BY
  d1.date, d2.date
ORDER BY
  d1.date;