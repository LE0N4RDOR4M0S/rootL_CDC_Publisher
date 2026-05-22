CREATE TABLE contratos (
                           id SERIAL PRIMARY KEY,
                           numero VARCHAR(50) NOT NULL,
                           valor DECIMAL(15, 2) NOT NULL,
                           status VARCHAR(20) NOT NULL,
                           criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO contratos (numero, valor, status) VALUES ('CTR-2026/001', 150000.00, 'ATIVO');

CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_password';

GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON contratos TO cdc_user;

CREATE PUBLICATION cdc_publication FOR TABLE contratos;

SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');