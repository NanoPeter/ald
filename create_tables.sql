
CREATE TABLE IF NOT EXISTS pressure
(
    datetime DATETIME(3),
    pressure FLOAT,
    PRIMARY KEY (datetime)
);

CREATE TABLE IF NOT EXISTS sample 
(
    datetime DATETIME(3),
    resistance FLOAT,
    temperature FLOAT,
    PRIMARY KEY (datetime)
);

CREATE TABLE IF NOT EXISTS temperature 
(
    datetime DATETIME(3),
    loop_id TINYINT UNSIGNED,
    temperature FLOAT,
    working_set_point FLOAT,
    target_set_point FLOAT,
    output_power FLOAT,
    PRIMARY KEY (datetime, loop_id)
);

CREATE TABLE IF NOT EXISTS flow 
(
    datetime DATETIME(3),
    volume_flow FLOAT,
    mass_flow FLOAT,
    pressure FLOAT,
    set_point FLOAT,
    temperature FLOAT,
    PRIMARY KEY(datetime)
);

CREATE TABLE IF NOT EXISTS valves
(
    datetime DATETIME(3),
    name VARCHAR(64) CHARACTER SET UTF8,
    state BOOL,
    PRIMARY KEY(datetime, name)
);

CREATE TABLE IF NOT EXISTS process_log
(
    start_time DATETIME(3),
    end_time DATETIME(3),
    cycle_no SMALLINT UNSIGNED,
    phase VARCHAR(128) CHARACTER SET UTF8,
    PRIMARY KEY(start_time)
);
