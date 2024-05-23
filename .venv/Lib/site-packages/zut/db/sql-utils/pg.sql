CREATE EXTENSION IF NOT EXISTS unaccent;


-- See: https://stackoverflow.com/a/45741630
CREATE OR REPLACE FUNCTION escape_regexp_pattern(pattern text)
RETURNS text LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT regexp_replace(pattern, '([!$()*+.:<=>?[\\\]^{|}-])', '\\\1', 'g')
$$;


CREATE OR REPLACE FUNCTION slugify(input text, separator char default '-', keep char default null, if_null text default null)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    WITH step1 AS (
        -- Normalize the string: replace diacritics by standard characters, lower the string, etc
        SELECT lower(unaccent(input)) AS value
    )
    ,step2 AS (
        -- Remove special characters
        SELECT regexp_replace(value, '[^a-zA-Z0-9\s' || escape_regexp_pattern(separator) || COALESCE(escape_regexp_pattern(keep), '') || ']', '', 'g') AS value FROM step1
    )
    ,step3 AS (
        -- Replace spaces and successive separators by a single separator
        SELECT regexp_replace(value, '[\s' || escape_regexp_pattern(separator) || ']+', separator, 'g') AS value FROM step2
    )
    ,step4 AS (
        -- Strips separator and kept character
        SELECT trim(BOTH escape_regexp_pattern(separator) || COALESCE(escape_regexp_pattern(keep), '') FROM value) AS value FROM step3
    )
    SELECT CASE WHEN input IS NULL THEN if_null ELSE value END FROM step4;
$$;


CREATE OR REPLACE FUNCTION slugify_django(input text, separator char default '-', keep char default '_', if_null text default 'none')
RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT slugify(input, separator, keep, if_null);
$$;
