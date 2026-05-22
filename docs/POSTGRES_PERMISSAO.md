# Especificação de Permissões e Governança para CDC (PostgreSQL)

**Documento de Arquitetura de Segurança e Governança de Dados**
Este documento descreve os pré-requisitos, permissões e configurações necessárias no banco de dados PostgreSQL para a operação da plataforma de Change Data Capture (CDC). 

A arquitetura adota o princípio de **Privilégio Mínimo (Least Privilege)** e **Governança na Origem (Governance-First)**. Ao contrário de ferramentas de prateleira que exigem permissões de criação de recursos dinâmicos (como o Debezium), nossa aplicação atua como uma leitora estritamente passiva. Todo o controle sobre *quais* dados são exportados permanece sob a autoridade do Administrador do Banco de Dados (DBA).

---

## 1. Configurações em Nível de Instância (`postgresql.conf`)

Antes de qualquer permissão de usuário, o servidor PostgreSQL precisa estar configurado para emitir logs detalhados.

| Parâmetro | Valor Exigido | Motivo / Justificativa |
| :--- | :--- | :--- |
| `wal_level` | `logical` | **Obrigatório.** Informa ao motor do banco para adicionar informações de decodificação lógica no Write-Ahead Log (WAL), permitindo reconstruir as linhas em formato de texto/JSON em vez de apenas blocos binários de disco. |
| `max_replication_slots` | `>= 1` | Define o número máximo de slots de replicação. O CDC consumirá exatamente 1 slot por banco de dados. |
| `max_wal_senders` | `>= 1` | Define o número de processos simultâneos que podem enviar dados do WAL pela rede. O CDC consumirá 1 processo enquanto estiver conectado. |

---

## 2. Usuário de Aplicação (Service Account)

A aplicação não deve utilizar usuários administrativos (como `postgres` ou `admin`). Um usuário dedicado deve ser provisionado.

**Comando:**
```sql
CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'sua_senha_segura';

```

**Justificativa das Permissões:**

* `LOGIN`: Permite que o microserviço abra uma conexão padrão via JDBC.
* `REPLICATION`: **Permissão Crítica.** Permite que o usuário abra uma conexão de replicação no modo protocolo binário e acesse o WAL. *Nota de segurança: Esta permissão não concede acesso aos dados das tabelas, apenas autoriza a abertura do canal de replicação.*

---

## 3. Controle de Escopo de Dados (Publications)

Em vez de a aplicação filtrar os dados em memória, o PostgreSQL filtrará os dados na origem. O DBA tem controle total sobre quais tabelas (ou quais linhas) serão transmitidas.

**Comando:**

```sql
CREATE PUBLICATION cdc_publication FOR TABLE public.contratos, public.fornecedores;

-- Ou, se o objetivo for replicar tudo:
-- CREATE PUBLICATION cdc_publication FOR ALL TABLES;

```

**Justificativa:**

* Garante que tabelas contendo senhas, logs de auditoria interna ou dados temporários não sejam acidentalmente vazados para o barramento do Kafka.
* Otimiza CPU e rede: o PostgreSQL sequer envia pela rede as alterações de tabelas que não estão declaradas na publicação.

---

## 4. Retenção Segura do WAL (Replication Slot)

O slot de replicação é o "ponteiro" que marca até onde a aplicação já leu o log. Na nossa arquitetura, **a aplicação não tem permissão para criar o slot dinamicamente**. O DBA deve criá-lo previamente.

**Comando:**

```sql
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');

```

**Justificativa:**

* **Prevenção de Queda de Servidor:** Se a aplicação pudesse criar slots sozinha (por erro de configuração), ela poderia criar slots órfãos. Um slot órfão impede o banco de limpar o WAL, o que invariavelmente enche o HD do servidor até travar o banco em Produção.
* `pgoutput`: É o plugin nativo (padrão) do PostgreSQL 10+. Ao usar o nativo, não há necessidade de instalar extensões de terceiros (como `wal2json`) no servidor do banco.

---

## 5. Permissões de Leitura para Carga Inicial (Snapshot)

Quando o CDC é ligado pela primeira vez, ele precisa ler os dados que já existiam antes da criação do slot. Ele faz isso abrindo uma transação em modo `REPEATABLE READ` e lendo as tabelas da publicação.

**Comando:**

```sql
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON TABLE public.contratos, public.fornecedores TO cdc_user;

```

**Justificativa:**

* A permissão de `SELECT` garante que a aplicação consiga fazer a query `SELECT * FROM tabela` durante a fase de Snapshot Inicial. O usuário não necessita, e não deve ter, permissões de `INSERT`, `UPDATE` ou `DELETE`.

---

## 6. Integridade de Atualizações (Replica Identity)

Para que a plataforma possua histórico transacional completo em caso de atualizações de registros.

**Comando:**

```sql
ALTER TABLE public.contratos REPLICA IDENTITY FULL;

```

**Justificativa:**

* Por padrão, para economizar disco, o PostgreSQL envia apenas os campos que foram alterados durante um `UPDATE`.
* O `REPLICA IDENTITY FULL` instrui o banco a gravar a linha inteira no WAL *antes* da mutação. Isso permite que a aplicação CDC gere um evento contendo o objeto `before` (como o dado era) e o `after` (como o dado ficou), essencial para auditoria e sincronização de Data Lakes.

---

## Resumo DDL: Script de Provisionamento para o DBA

Para facilitar a implantação, o script abaixo condensa todas as etapas necessárias para preparar uma nova base de dados para o CDC:

```sql
-- 1. Criação do Usuário
CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_password';

-- 2. Criação da Publicação (Definição de escopo)
CREATE PUBLICATION cdc_publication FOR TABLE public.contratos;

-- 3. Criação do Slot de Replicação (Ponteiro)
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');

-- 4. Permissões de Leitura (Para a fase de Snapshot)
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON TABLE public.contratos TO cdc_user;

-- 5. Configuração das Tabelas para Auditoria Completa
ALTER TABLE public.contratos REPLICA IDENTITY FULL;

```