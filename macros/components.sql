CREATE OR REPLACE MACRO get_comp_mapping() AS TABLE (
    SELECT * FROM (VALUES 
        (0,  'artifact'),
        (1,  'crowd'),
        (2,  'discovery')
    ) AS t(bit, value)
);
