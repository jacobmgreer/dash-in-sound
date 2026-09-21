CREATE OR REPLACE MACRO get_decade_mapping() AS TABLE (
    SELECT * FROM (VALUES 
        (0,  2020),
        (1,  2010),
        (2,  2000),
        (3,  1990),
        (4,  1980),
        (5,  1970),
        (6,  1960),
        (7,  1950),
        (8,  1940),
        (9,  1930),
        (10, 1920),
        (11, 1910),
        (12, 1900),
        (13, 1890),
        (14, 1880)
    ) AS t(bit, value)
);
