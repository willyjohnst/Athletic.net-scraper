-- Populate standard_events table
-- Columns: name, distance_meters, is_field, is_relay

INSERT INTO standard_events (name, distance_meters, is_field, is_relay) VALUES
-- Sprints
('50m', 50, FALSE, FALSE),
('55m', 55, FALSE, FALSE),
('60m', 60, FALSE, FALSE),
('70m', 70, FALSE, FALSE),
('100m', 100, FALSE, FALSE),
('200m', 200, FALSE, FALSE),
('300m', 300, FALSE, FALSE),
('400m', 400, FALSE, FALSE),

-- Mid-distance
('500m', 500, FALSE, FALSE),
('600m', 600, FALSE, FALSE),
('800m', 800, FALSE, FALSE),
('1000m', 1000, FALSE, FALSE),
('1200m', 1200, FALSE, FALSE),
('1500m', 1500, FALSE, FALSE),
('1600m', 1600, FALSE, FALSE),
('1 Mile', 1609.34, FALSE, FALSE),

-- Distance
('2400m', 2400, FALSE, FALSE),
('3000m', 3000, FALSE, FALSE),
('3200m', 3200, FALSE, FALSE),
('2 Mile', 3218.69, FALSE, FALSE),
('5000m', 5000, FALSE, FALSE),
('10000m', 10000, FALSE, FALSE),

-- Steeplechase
('2000m Steeplechase', 2000, FALSE, FALSE),
('3000m Steeplechase', 3000, FALSE, FALSE),

-- Hurdles
('55m Hurdles', 55, FALSE, FALSE),
('60m Hurdles', 60, FALSE, FALSE),
('65m Hurdles', 65, FALSE, FALSE),
('75m Hurdles', 75, FALSE, FALSE),
('80m Hurdles', 80, FALSE, FALSE),
('100m Hurdles', 100, FALSE, FALSE),
('110m Hurdles', 110, FALSE, FALSE),
('200m Hurdles', 200, FALSE, FALSE),
('300m Hurdles', 300, FALSE, FALSE),
('400m Hurdles', 400, FALSE, FALSE),

-- Shuttle hurdle relays
('4x55 Shuttle Hurdles', 220, FALSE, TRUE),
('4x100 Shuttle Hurdles', 400, FALSE, TRUE),
('4x102.5 Shuttle Hurdles', 410, FALSE, TRUE),
('4x110 Shuttle Hurdles', 440, FALSE, TRUE),

-- Relays
('4x100m Relay', 400, FALSE, TRUE),
('4x100 Throwers Relay', 400, FALSE, TRUE),
('4x160m Relay', 640, FALSE, TRUE),
('4x200m Relay', 800, FALSE, TRUE),
('4x400m Relay', 1600, FALSE, TRUE),
('4x800m Relay', 3200, FALSE, TRUE),
('4x1500m Relay', 6000, FALSE, TRUE),
('4x1600m Relay', 6400, FALSE, TRUE),
('4xMile Relay', 6437.38, FALSE, TRUE),
('DMR', 4000, FALSE, TRUE),
('SMR', 1600, FALSE, TRUE),
('SMR 800m', 800, FALSE, TRUE),

-- Yard distances (older meets)
('100 Yards', 91.44, FALSE, FALSE),
('220 Yards', 201.17, FALSE, FALSE),
('440 Yards', 402.34, FALSE, FALSE),
('880 Yards', 804.67, FALSE, FALSE),
('120y Hurdles', 109.73, FALSE, FALSE),
('4x440 Yard Relay', 1609.34, FALSE, TRUE),
('4x220 Yard Relay', 804.67, FALSE, TRUE),

-- Field events
('High Jump', 0, TRUE, FALSE),
('Pole Vault', 0, TRUE, FALSE),
('Long Jump', 0, TRUE, FALSE),
('Triple Jump', 0, TRUE, FALSE),
('Shot Put', 0, TRUE, FALSE),
('Discus', 0, TRUE, FALSE),
('Hammer Throw', 0, TRUE, FALSE),
('Javelin', 0, TRUE, FALSE),
('Weight Throw', 0, TRUE, FALSE),

-- XC distances
('5K XC', 5000, FALSE, FALSE),
('4K XC', 4000, FALSE, FALSE),
('6K XC', 6000, FALSE, FALSE),
('8K XC', 8000, FALSE, FALSE),
('10K XC', 10000, FALSE, FALSE),
('3 Mile XC', 4828.03, FALSE, FALSE)

ON CONFLICT (name) DO NOTHING;