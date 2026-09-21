CREATE OR REPLACE MACRO get_origin_mapping() AS TABLE (
    SELECT * FROM (VALUES 
        (0,  'english'),
        (1,  'germanic'),
        (2,  'francophone'),
        (3,  'hispanophone'),
        (4,  'subcontinental'),
        (5,  'japanese'),
        (6,  'nordic'),
        (7,  'sinophone'),
        (8,  'italophone'),
        (9,  'slavic_w'),
        (10, 'lusophone'),
        (11, 'slavic_e'),
        (12, 'korean'),
        (13, 'arabic'),
        (14, 'se_asia'),
        (15, 'turkic'),
        (16, 'hellenic'),
        (17, 'iranic'),
        (18, 'hebrew'),
        (19, 'ugric'),
        (20, 'baltic'),
        (21, 'latin_europe_e'),
        (22, 'caucasian'),
        (23, 'mongolic'),
        (24, 'afroasiatic')
    ) AS t(bit, value)
);
