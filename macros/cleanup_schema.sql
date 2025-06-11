{% macro cleanup_schema() %}
  DECLARE
    object_name STRING;
    object_type STRING;
  BEGIN
    FOR rec IN (
      SELECT table_name AS object_name, 'TABLE' AS object_type
      FROM information_schema.tables
      WHERE table_schema = '{{ target.schema }}'
      UNION ALL
      SELECT view_name AS object_name, 'VIEW' AS object_type
      FROM information_schema.views
      WHERE table_schema = '{{ target.schema }}'
    )
    DO
      EXECUTE IMMEDIATE 'DROP ' || rec.object_type || ' IF EXISTS ' || '{{ target.schema }}.' || rec.object_name || ' CASCADE';
    END FOR;
  END;
{% endmacro %} 