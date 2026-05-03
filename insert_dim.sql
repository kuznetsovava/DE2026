INSERT INTO dwh.dim_date (id, date_dat, date_month, date_quarter, date_year)
SELECT 
    TO_CHAR(d, 'YYYYMMDD')::INT AS id,
    d::DATE AS date_dat,
    DATE_TRUNC('month', d)::DATE AS date_month,
    DATE_TRUNC('quarter', d)::DATE AS date_quarter,
    DATE_TRUNC('year', d)::DATE AS date_year
FROM generate_series(
    '2022-01-01'::DATE,
    '2023-12-31'::DATE,
    '1 day'::INTERVAL
) AS s(d)
ON CONFLICT (id) DO NOTHING;

-- Заполнение dim_city
INSERT INTO dwh.dim_city (id, ccode, nname) VALUES
    (56, 'MSK', 'Москва'),
    (324, 'SPB', 'Санкт-Петербург')
ON CONFLICT (id) DO NOTHING;

-- Заполнение dim_operation
INSERT INTO dwh.dim_operation (id, nname) VALUES
    (1, 'Поступление'),
    (2, 'Списание')
ON CONFLICT (id) DO NOTHING;
