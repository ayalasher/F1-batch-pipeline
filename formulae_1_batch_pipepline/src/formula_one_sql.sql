CREATE TABLE IF NOT EXISTS drivers_championship (
    points BIGINT,
    position BIGINT,
    driver_name TEXT,
    team_teamId TEXT,
    driver_surname TEXT
);

CREATE TABLE IF NOT EXISTS constructors_championship (
    teamId TEXT,
    points BIGINT,
    position BIGINT,
    wins BIGINT,
    team_teamName TEXT
);

-- Check if tables exist
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN ('drivers_championship', 'constructors_championship');


SELECT * FROM drivers_championship ORDER BY position;


SELECT * FROM constructors_championship ORDER BY position;

SELECT 
    (SELECT COUNT(*) FROM drivers_championship) as driver_count,
    (SELECT COUNT(*) FROM constructors_championship) as constructor_count;


SELECT 
    position,
    team_teamName as team,
    points,
    wins
FROM constructors_championship
ORDER BY position;

-- Check constructors table columns
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'constructors_championship';

-- Check drivers table columns
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'drivers_championship';