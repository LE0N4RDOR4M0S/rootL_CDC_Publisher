CREATE DATABASE IF NOT EXISTS rh_db;
USE rh_db;

CREATE TABLE contratos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    numero_contrato VARCHAR(50) NOT NULL,
    valor DECIMAL(15, 2) NOT NULL,
    data_assinatura DATE NOT NULL
);

CREATE TABLE fornecedores (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cnpj VARCHAR(14) UNIQUE NOT NULL,
    razao_social VARCHAR(150) NOT NULL
);

INSERT INTO fornecedores (cnpj, razao_social) VALUES ('22222222000122', 'Data Analytics Ltda');
INSERT INTO contratos (numero_contrato, valor, data_assinatura) VALUES ('CTR-MY-2026', 125000.00, '2026-02-20');

CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'cdc_password';
GRANT SELECT ON rh_db.* TO 'cdc_user'@'%';
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'cdc_user'@'%';
GRANT RELOAD ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
