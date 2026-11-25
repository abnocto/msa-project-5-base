CREATE TABLE orders (
    order_id   INT PRIMARY KEY,
    amount     NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

INSERT INTO orders
SELECT day * 1000 + n, 500 + n * 100, DATE '2026-11-22' + day + n * INTERVAL '30 minutes'
FROM generate_series(1, 3) AS day, generate_series(1, 20) AS n;

CREATE TABLE delivery_mart (
    report_date DATE NOT NULL,
    order_id    INT NOT NULL,
    status      TEXT NOT NULL,
    PRIMARY KEY (report_date, order_id)
);
