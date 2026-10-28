CREATE OR REPLACE MACRO get_source_mapping() AS TABLE (
    SELECT * FROM (VALUES 
        (1,  'Discogs'),
        (2,  'MusicBrainz'),
        (3,  'What.CD'),
        (4,  'Wikidata')
    ) AS t(bit, value)
);