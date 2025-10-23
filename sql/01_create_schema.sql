
-- PROPERTY TABLE (Main entity)

CREATE TABLE property (
    property_id INT PRIMARY KEY,
    property_title VARCHAR(500),
    address VARCHAR(500),
    street_address VARCHAR(300),
    city VARCHAR(100),
    state VARCHAR(5),
    zip VARCHAR(20),
    market VARCHAR(100),
    property_type VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    subdivision VARCHAR(200),
    year_built INT,
    bed INT,
    bath INT,
    sqft_total VARCHAR(50),
    sqft_mu INT,
    sqft_basement INT,
    basement_yes_no VARCHAR(10),
    layout VARCHAR(50),
    parking VARCHAR(50),
    flood VARCHAR(50),
    highway VARCHAR(50),
    train VARCHAR(50),
    htw VARCHAR(10),
    pool VARCHAR(10),
    commercial VARCHAR(10),
    water VARCHAR(50),
    sewage VARCHAR(50),
    tax_rate DECIMAL(10, 4),
    rent_restricted VARCHAR(10),
    neighborhood_rating INT,
    school_average DECIMAL(5, 2),
    
    -- Indexes for common queries
    INDEX idx_address (address(255)),
    INDEX idx_city_state (city, state),
    INDEX idx_zip (zip),
    INDEX idx_market (market),
    INDEX idx_property_type (property_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- LEADS TABLE (Sales/Deal tracking)
CREATE TABLE leads (
    lead_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    reviewed_status VARCHAR(50),
    most_recent_status VARCHAR(50),
    source VARCHAR(100),
    occupancy VARCHAR(50),
    
    net_yield DECIMAL(10, 4),
    irr DECIMAL(10, 4),
    selling_reason VARCHAR(200),
    seller_retained_broker VARCHAR(200),
    final_reviewer VARCHAR(200),
    
    FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE,
    INDEX idx_status (most_recent_status),
    INDEX idx_source (source),
    INDEX idx_reviewer (final_reviewer(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- TAXES TABLE (Property tax information)

CREATE TABLE taxes (
    tax_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    taxes DECIMAL(12, 2),
    
    FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE,
    INDEX idx_property (property_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- VALUATION TABLE (Multiple valuations per property)

CREATE TABLE valuation (
    valuation_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    list_price DECIMAL(12, 2),
    zestimate DECIMAL(12, 2),
    arv DECIMAL(12, 2),
    redfin_value DECIMAL(12, 2),
    previous_rent DECIMAL(10, 2),
    expected_rent DECIMAL(10, 2),
    rent_zestimate DECIMAL(10, 2),
    low_fmr DECIMAL(10, 2),
    high_fmr DECIMAL(10, 2),
    
    FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE,
    INDEX idx_property (property_id),
    INDEX idx_list_price (list_price)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- HOA TABLE (Homeowner Association data) 

CREATE TABLE hoa (
    hoa_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    hoa DECIMAL(10, 2),
    hoa_flag VARCHAR(10),
    
    FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE,
    INDEX idx_property (property_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- REHAB TABLE (Rehabilitation/Renovation estimates)

CREATE TABLE rehab (
    rehab_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    underwriting_rehab DECIMAL(12, 2),
    rehab_calculation DECIMAL(12, 2),
    paint VARCHAR(10),
    flooring_flag VARCHAR(10),
    foundation_flag VARCHAR(10),
    roof_flag VARCHAR(10),
    hvac_flag VARCHAR(10),
    kitchen_flag VARCHAR(10),
    bathroom_flag VARCHAR(10),
    appliances_flag VARCHAR(10),
    windows_flag VARCHAR(10),
    landscaping_flag VARCHAR(10),
    trashout_flag VARCHAR(10),
    
    FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE,
    INDEX idx_property (property_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

