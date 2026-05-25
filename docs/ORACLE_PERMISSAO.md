# Especificação de Permissões e Governança para CDC (Oracle Database)

**Documento de Arquitetura de Segurança e Governança de Dados**
Este documento descreve os pré-requisitos e configurações necessárias no banco de dados Oracle para a extração passiva de eventos via CDC.

A plataforma utiliza o recurso nativo **Oracle LogMiner** para minerar os *Redo Logs* e *Archive Logs*. A aplicação não instala agentes de terceiros, não cria *triggers* e não executa instruções de DDL ou DML no ambiente de produção. Todo o acesso é restrito à leitura transacional em memória.

Devido à arquitetura Multitenant do Oracle (12c+), o usuário de extração deve ser provisionado como um **Common User** (com o prefixo `C##`), operando na raiz do servidor (CDB) para conseguir enxergar os logs globais que englobam todos os Pluggable Databases (PDBs).

---

## 1. Configuração da Instância (Archive e Supplemental Logging)

Para que o Oracle preserve o histórico de mutações e permita a leitura assíncrona, a instância precisa estar em modo `ARCHIVELOG`. 
Além disso, o `SUPPLEMENTAL LOGGING` é **mandatório**. Sem ele, o Oracle grava no log apenas as colunas que foram alteradas em um `UPDATE`, impossibilitando a reconstrução do estado completo da linha (Before/After) no Data Lake.

**Executado pelo SYSDBA (Na raiz - CDB$ROOT):**
```sql
-- Requer janela de manutenção se o banco não estiver em ARCHIVELOG
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

-- Ativação da captura de metadados transacionais
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;

```

---

## 2. Provisionamento do Usuário CDC (Common User)

O usuário deve ser criado no *Container Database* (CDB) com privilégios globais (`CONTAINER=ALL`), permitindo que a aplicação transite entre os dicionários do sistema e os dados dos PDBs.

**Executado pelo SYSDBA (Na raiz - CDB$ROOT):**

```sql
-- Cria o Common User
CREATE USER C##CDC_USER IDENTIFIED BY "sua_senha_segura" CONTAINER=ALL;

-- Permissões básicas de conexão e transição de containers
GRANT CREATE SESSION, ALTER SESSION, SET CONTAINER TO C##CDC_USER CONTAINER=ALL;

```

---

## 3. Permissões de Extração (LogMiner API)

A aplicação precisa de acesso explícito aos pacotes da `SYS` que controlam o LogMiner, bem como às views dinâmicas do dicionário de dados que expõem os arquivos de log.

**Comando:**

```sql
-- Permite acesso à leitura de logs transacionais
GRANT LOGMINING TO C##CDC_USER CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO C##CDC_USER CONTAINER=ALL;

-- Permite a execução da procedure de montagem dos logs em memória
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO C##CDC_USER CONTAINER=ALL;

-- Permissões explícitas em views de dicionário utilizadas pelo conector
GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO C##CDC_USER CONTAINER=ALL;
GRANT SELECT ON SYS.V_$LOGMNR_LOGS TO C##CDC_USER CONTAINER=ALL;
GRANT SELECT ON SYS.V_$DATABASE TO C##CDC_USER CONTAINER=ALL;

```

---

## 4. Escopo de Dados (Permissões de Tabela)

A aplicação atua filtrando os dados na cláusula `WHERE` da query do LogMiner (via `SEG_OWNER = 'SCHEMA_ALVO'`). No entanto, o `C##CDC_USER` precisa ter permissão de leitura (`SELECT`) nas tabelas publicadas para que o banco permita a visualização do dado desofuscado e a resolução de mapeamentos de colunas no catálogo do Oracle.

**Comando (Governança Centralizada):**

```sql
-- Opção Recomendada: Acesso de leitura a todas as tabelas (O filtro de escopo ocorre no arquivo .json da aplicação)
GRANT SELECT ANY TABLE TO C##CDC_USER CONTAINER=ALL; 

```
