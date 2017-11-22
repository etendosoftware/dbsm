-- CREATE TEMPORAL FUNCTION TO DROP uuid_generate_v4 WHEN NEEDED
CREATE OR REPLACE FUNCTION drop_uuid_generate_v4()
  RETURNS void AS
$BODY$ DECLARE

  v_exists NUMERIC;

BEGIN
  SELECT count(1) INTO v_exists FROM pg_extension WHERE extname = 'uuid-ossp';
  IF (v_exists = 0) THEN
    DROP FUNCTION IF EXISTS uuid_generate_v4();
  END IF;
END;  $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
/-- END

--EXECUTE TEMPORAL FUNCTION
SELECT drop_uuid_generate_v4();
/-- END

-- DROP TEMPORAL FUNCTION
DROP FUNCTION drop_uuid_generate_v4();
/-- END

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
/-- END

CREATE EXTENSION IF NOT EXISTS "pg_trgm";
/-- END