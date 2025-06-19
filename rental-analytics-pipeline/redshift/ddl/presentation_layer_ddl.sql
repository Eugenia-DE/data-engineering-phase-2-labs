create schema presentation;
CREATE TABLE IF NOT EXISTS presentation.avg_listing_price_weekly (
    week_start_date DATE,
    avg_listing_price DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS presentation.occupancy_rate_monthly (
    month DATE,
    booked_nights INTEGER,
    available_nights INTEGER,
    occupancy_rate_pct DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS presentation.popular_locations_weekly (
    week_start DATE,
    cityname VARCHAR(255),
    bookings_count INTEGER
);
CREATE TABLE IF NOT EXISTS presentation.top_performing_listings_weekly (
    week_start DATE,
    apartment_id INTEGER,
    weekly_revenue DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS presentation.total_bookings_per_user_weekly (
    week_start DATE,
    user_id INTEGER,
    total_bookings INTEGER
);
CREATE TABLE IF NOT EXISTS presentation.avg_booking_duration_weekly (
    week_start DATE,
    avg_booking_duration_days DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS presentation.repeat_customer_rate (
    rolling_window_start_date DATE,
    total_users INTEGER,
    repeat_customers INTEGER,
    repeat_customer_rate_pct DOUBLE PRECISION
);