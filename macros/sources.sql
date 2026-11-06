CREATE OR REPLACE MACRO get_source_mapping() AS TABLE (
    SELECT * FROM (VALUES 
        (0,  'Discogs'),
        (1,  'MusicBrainz'),
        (2,  'What.CD'),
        (3,  'Wikidata')
    ) AS t(bit, value)
);