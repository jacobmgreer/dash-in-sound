CREATE OR REPLACE MACRO get_genre_mapping() AS TABLE (
    SELECT * FROM (VALUES 
        (0,  'Electronic'),
        (1,  'Rock'),
        (2,  'Folk, World, & Country'),
        (3,  'Pop'),
        (4,  'Classical'),
        (5,  'Jazz'),
        (6,  'Hip Hop'),
        (7,  'Funk / Soul'),
        (8,  'Latin'),
        (9,  'Reggae'),
        (10, 'Non-Music'),
        (11, 'Stage & Screen'),
        (12, 'Blues'),
        (13, 'Children'),
        (14, 'Brass & Military')
    ) AS t(bit, value)
);