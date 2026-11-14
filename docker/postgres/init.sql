-- Source OLTP schema for the compose stack (same shape as the local simulator).
-- REPLICA IDENTITY FULL makes Debezium send complete "before" images for updates and deletes.
CREATE TABLE customers (
    id          integer PRIMARY KEY,
    name        text        NOT NULL,
    email       text        NOT NULL,
    city        text,
    tier        text        NOT NULL DEFAULT 'basic',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    id          integer PRIMARY KEY,
    customer_id integer     NOT NULL REFERENCES customers (id),
    status      text        NOT NULL DEFAULT 'pending',
    total       numeric(12, 2) NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_id    integer     NOT NULL REFERENCES orders (id),
    line_no     integer     NOT NULL,
    sku         text        NOT NULL,
    qty         integer     NOT NULL CHECK (qty > 0),
    unit_price  numeric(12, 2) NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

ALTER TABLE customers   REPLICA IDENTITY FULL;
ALTER TABLE orders      REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;

-- Seed rows: these reach Kafka through Debezium's initial snapshot (op = 'r').
INSERT INTO customers (id, name, email, city, tier)
SELECT g, 'Customer ' || g, 'customer' || g || '@example.com',
       (ARRAY['Mohali','Chandigarh','Ludhiana','Patiala','Jaipur','Delhi','Pune','Bengaluru'])[1 + g % 8],
       (ARRAY['basic','basic','silver','gold'])[1 + g % 4]
FROM generate_series(1, 200) AS g;

INSERT INTO orders (id, customer_id, status, total)
SELECT g, 1 + (g * 7) % 200, (ARRAY['pending','paid','shipped','delivered'])[1 + g % 4], 0
FROM generate_series(1, 300) AS g;

INSERT INTO order_items (order_id, line_no, sku, qty, unit_price)
SELECT o, l, 'SKU-' || lpad(((o * 3 + l) % 40 + 1)::text, 3, '0'), 1 + (o + l) % 4, 99 + ((o * 31 + l * 17) % 8000)
FROM generate_series(1, 300) AS o, generate_series(1, 2) AS l;

UPDATE orders o SET total = s.total
FROM (SELECT order_id, sum(qty * unit_price) AS total FROM order_items GROUP BY order_id) s
WHERE s.order_id = o.id;
