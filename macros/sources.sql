CREATE OR REPLACE MACRO get_source_mapping() AS TABLE (
    SELECT * FROM (VALUES 
        (1,  'IMDb'),
        (2,  'TMDb'),
        (3,  'Wikidata'),
        (4,  'EIDR'),
        (5,  'MusicBrainz'),
        (6,  'Discogs'),
        (7,  'What.CD')
    ) AS t(bit, value)
);
