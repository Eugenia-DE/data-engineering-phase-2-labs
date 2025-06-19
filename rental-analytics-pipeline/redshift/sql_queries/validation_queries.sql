-- Top weekly listings
SELECT
    CAST(DATE_TRUNC(‘week’, booking_date) AS DATE) AS week_start,
    apartment_id,
    SUM(total_price) AS weekly_revenue
FROM curated.bookings
WHERE booking_status LIKE ‘%confirmed%’
GROUP BY CAST(DATE_TRUNC(‘week’, booking_date) AS DATE), apartment_id
ORDER BY week_start;


-- Average listing price
SELECT
    CAST(DATE_TRUNC(‘week’, listing_created_on) AS DATE) AS week_start,
    ROUND(AVG(price), 2) AS avg_listing_price
FROM curated.apartments
WHERE is_active = TRUE
GROUP BY 1
ORDER BY 1

-- total_bookings_per_user_weekly
SELECT
    CAST(DATE_TRUNC(‘week’, booking_date) AS DATE) AS week_start,
    user_id,
    COUNT(*) AS total_bookings
FROM curated.bookings
WHERE booking_status LIKE ‘%confirmed%’
GROUP BY CAST(DATE_TRUNC(‘week’, booking_date) AS DATE), user_id

-- Average Booking Duration Weekly
SELECT
    CAST(DATE_TRUNC(‘week’, checkin_date) AS DATE) AS week_start,
    ROUND(AVG(DATEDIFF(day, checkin_date, checkout_date)), 2) AS avg_booking_duration_days
FROM curated.bookings
WHERE booking_status LIKE ‘%confirmed%’
GROUP BY 1
ORDER BY 1

---Repeat customers
WITH user_bookings AS (
    SELECT
        user_id,
        booking_date::DATE AS booking_date,
        LEAD(booking_date::DATE) OVER (
            PARTITION BY user_id
            ORDER BY booking_date::DATE
        ) AS next_booking_date
    FROM curated.bookings
    WHERE booking_status LIKE ‘%confirmed%’
),
repeat_customers AS (
    SELECT DISTINCT user_id
    FROM user_bookings
    WHERE next_booking_date IS NOT NULL
      AND DATEDIFF(day, booking_date, next_booking_date) <= 30
)
SELECT
    TO_CHAR(DATE_TRUNC(‘month’, b.booking_date::DATE), ‘YYYY-MM’)   AS month,
    COUNT(DISTINCT rc.user_id)                                      AS repeat_customers
FROM curated.bookings b
JOIN repeat_customers rc ON b.user_id = rc.user_id
GROUP BY 1
ORDER BY 1;

---Occupancy rate
WITH recent_bookings AS (
    SELECT
        user_id,
        booking_date
    FROM curated.bookings
    WHERE DATEDIFF(day, booking_date, CURRENT_DATE) <= 30
),
booking_counts AS (
    SELECT
        user_id,
        COUNT(*) AS total_bookings
    FROM recent_bookings
    GROUP BY user_id
),
repeat_customers AS (
    SELECT
        user_id,
        total_bookings,
        CASE
            WHEN total_bookings > 1 THEN 1
            ELSE 0
        END AS is_repeat_customer
    FROM booking_counts
),
final_agg AS (
    SELECT
        CURRENT_DATE - INTERVAL ‘30 day’ AS rolling_window_start_date,
        COUNT(*) AS total_users,
        SUM(is_repeat_customer) AS repeat_customers,
        ROUND(SUM(is_repeat_customer)::DECIMAL / COUNT(*), 4) AS repeat_customer_rate
    FROM repeat_customers
)
SELECT * FROM final_agg;