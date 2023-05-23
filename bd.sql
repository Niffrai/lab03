CREATE ON REPLACE FUNCTION import_dataset(dataset VARCHAR(10))
    RETURNS BOOLEAN LANGUAGE plpgsql AS $$
DECLARE
    script VARCHAR(250) = 'текст lua сценария';
BEGIN
    EXECUTE format('CREATE FOREIGN TABLE set_%s (month INTEGER,
                  day INTEGER, year INTEGER, temperature DOUBLE PRECISION)
                  SERVER file_fdw OPTIONS (program ' ' cat /somefolder/%s.txt |
                  lua5.3 -e "%s"'', header ''true'', format ''csv'')',
                  dataset, dataset, script);
    RETURNS TRUE;
END
$$;