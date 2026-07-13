CREATE TABLE public.contratos (
    id SERIAL PRIMARY KEY,
    numero_contrato VARCHAR(50) NOT NULL,
    valor NUMERIC(15, 2) NOT NULL,
    data_assinatura DATE NOT NULL
);

CREATE TABLE public.fornecedores (
    id SERIAL PRIMARY KEY,
    cnpj VARCHAR(14) UNIQUE NOT NULL,
    razao_social VARCHAR(150) NOT NULL
);

INSERT INTO public.fornecedores (cnpj, razao_social) VALUES ('11111111000111', 'Tech Corp S.A.');
INSERT INTO public.contratos (numero_contrato, valor, data_assinatura) VALUES ('CTR-PG-2026', 75000.00, '2026-01-15');

CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_password';
CREATE PUBLICATION cdc_publication FOR TABLE public.contratos, public.fornecedores;
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON TABLE public.contratos, public.fornecedores TO cdc_user;
ALTER TABLE public.contratos REPLICA IDENTITY FULL;
ALTER TABLE public.fornecedores REPLICA IDENTITY FULL;
