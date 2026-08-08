SELECT
  ROUND(NUMERIC '1.245', 2) AS rounded_amount,
  BIGNUMERIC '123.456789012345678901' AS wide_amount,
  STRUCT(
    NUMERIC '2.5' AS ratio,
    ARRAY<STRING>['x', 'y'] AS labels
  ) AS detail,
  ARRAY<STRUCT<code INT64, value BIGNUMERIC>>[
    STRUCT(1 AS code, BIGNUMERIC '0.000000000000000001' AS value),
    STRUCT(2 AS code, BIGNUMERIC '2' AS value)
  ] AS entries
