create schema curated;

CREATE TABLE curated.apartment_attributes (
    id INT PRIMARY KEY,
    category VARCHAR(50),
    body TEXT,
    amenities TEXT,
    bathrooms INT,
    bedrooms INT,
    fee DECIMAL(10,2),
    has_photo BOOLEAN,
    pets_allowed BOOLEAN,
    price_display VARCHAR(50),
    price_type VARCHAR(50),
    square_feet INT,
    address VARCHAR(255),
    cityname VARCHAR(100),
    state VARCHAR(100),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6)
);
CREATE TABLE curated.apartments (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    source VARCHAR(100),
    price DECIMAL(10,2),
    currency VARCHAR(10),
    listing_created_on DATE,
    is_active BOOLEAN,
    last_modified_timestamp DATE
);
CREATE TABLE curated.bookings (
    booking_id INT PRIMARY KEY,
    user_id INT,
    apartment_id INT,
    booking_date DATE,
    checkin_date DATE,
    checkout_date DATE,
    total_price DECIMAL(10,2),
    currency VARCHAR(10),
    booking_status VARCHAR(50),
    FOREIGN KEY (apartment_id) REFERENCES curated.apartments(id)
);
CREATE TABLE curated.user_viewing (
    user_id INT,
    apartment_id INT,
    viewed_at DATE,
    is_wishlisted BOOLEAN,
    call_to_action VARCHAR(100),
    PRIMARY KEY (user_id, apartment_id, viewed_at),
    FOREIGN KEY (apartment_id) REFERENCES curated.apartment_attributes(id)
);