# Especificação de Permissões e Governança para CDC (Oracle Database)

**Documento de Arquitetura de Segurança e Governança de Dados**

Este documento descreve os pré-requisitos e configurações necessárias no banco de dados Oracle para a extração passiva de eventos via CDC (Change Data Capture).

A plataforma utiliza o recurso nativo **Oracle LogMiner** para minerar os *Redo Logs* e *Archive Logs*. A aplicação não instala agentes de terceiros, não cria *triggers* e não executa instruções de DDL ou DML no ambiente de produção. Todo o acesso é restrito à leitura transacional em memória.

Devido à arquitetura Multitenant do Oracle (12c+), o usuário de extração deve ser provisionado como um **Common User** (com o prefixo `C##`), operando na raiz do servidor (CDB$ROOT) para conseguir enxergar os logs globais que englobam todos os Pluggable Databases (PDBs).

---

## 1. Configuração da Instância (Archive e Supplemental Logging)

Para que o Oracle preserve o histórico de mutações e permita a leitura assíncrona, a instância precisa obrigatoriamente estar em modo `ARCHIVELOG`.

Além disso, o `SUPPLEMENTAL LOGGING` completo é **mandatório** para que o Oracle grave as colunas não alteradas na instrução transacional, permitindo a reconstrução do estado completo da linha (Before/After) no Data Lake. Para evitar impactos de performance e *overhead* de armazenamento no servidor, a governança determina que a ativação completa seja feita estritamente nas tabelas alvo do CDC, e não no banco inteiro.

**Executado pelo SYSDBA (Na raiz - CDB$ROOT):**

```sql
-- Requer janela de manutenção se o banco não estiver em ARCHIVELOG
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

-- 1. Ativação estrutural mínima do log transacional no nível do banco
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

```

**Executado pelo SYSDBA (No nível da Tabela / PDB):**

```sql
-- 2. Isola o overhead de armazenamento apenas nas tabelas específicas do escopo
-- (Executar para cada tabela que será monitorada)
ALTER TABLE NOME_DO_SCHEMA.NOME_DA_TABELA ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

```

---

## 2. Provisionamento do Usuário CDC (Common User)

O usuário deve ser criado no *Container Database* (CDB) com privilégios globais (`CONTAINER=ALL`), permitindo que a aplicação transite entre os dicionários do sistema raiz e os dados dos PDBs.

**Executado pelo SYSDBA (Na raiz - CDB$ROOT):**

```sql
-- Cria o Common User
CREATE USER C##CDC_USER IDENTIFIED BY "sua_senha_segura" CONTAINER=ALL;

-- Permissões básicas de conexão e transição de containers
GRANT CREATE SESSION, ALTER SESSION, SET CONTAINER TO C##CDC_USER CONTAINER=ALL;

```

---

## 3. Permissões de Extração (LogMiner API e Descoberta de Logs)

A aplicação atua de forma autônoma. Para isso, ela necessita de permissões explícitas para invocar os pacotes de controle do LogMiner e para pesquisar dinamicamente nas tabelas do sistema quais são os arquivos de Redo Log ativos e arquivados no disco a cada ciclo de mineração. Estas permissões não expõem dados de negócio.

**Comando (Governança de Leitura do Dicionário):**

```sql
-- Permite acesso geral ao modo de leitura de logs transacionais
GRANT LOGMINING TO C##CDC_USER CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO C##CDC_USER CONTAINER=ALL;

-- Permite a execução das procedures vitais do LogMiner
GRANT EXECUTE ON SYS.DBMS_LOGMNR TO C##CDC_USER CONTAINER=ALL;

-- Permissões explícitas nas views de leitura do Payload Transacional
GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS TO C##CDC_USER CONTAINER=ALL;
GRANT SELECT ON SYS.V_$DATABASE TO C##CDC_USER CONTAINER=ALL;

-- Permissões explícitas nas views de Descoberta Dinâmica de Arquivos Físicos (.log e .arc)
GRANT SELECT ON SYS.V_$LOGMNR_LOGS TO C##CDC_USER CONTAINER=ALL;
GRANT SELECT ON SYS.V_$LOG TO C##CDC_USER CONTAINER=ALL;
GRANT SELECT ON SYS.V_$LOGFILE TO C##CDC_USER CONTAINER=ALL;
GRANT SELECT ON SYS.V_$ARCHIVED_LOG TO C##CDC_USER CONTAINER=ALL;

```

---

## 4. Escopo de Dados (Permissões de Tabela Restritas)

Embora a mineração ocorra na raiz (CDB), o `C##CDC_USER` precisa ter permissão de leitura (`SELECT`) nas tabelas publicadas no PDB. Isso é exigido internamente pelo Oracle para que o banco libere a visualização do dado desofuscado e resolva os mapeamentos de colunas no catálogo do banco.

Seguindo o princípio do privilégio mínimo e adequação a auditorias de segurança, o acesso de leitura global é desencorajado. O acesso deve ser concedido **apenas às tabelas do escopo**, diretamente no PDB de origem.

**Executado dentro do Pluggable Database específico (Ex: ORCLPDB1):**

```sql
-- Alterna a sessão para o PDB alvo
ALTER SESSION SET CONTAINER = ORCLPDB1;

-- Concessão explícita por objeto
GRANT SELECT ON SCHEMA_ALVO.TABELA_1 TO C##CDC_USER;
GRANT SELECT ON SCHEMA_ALVO.TABELA_2 TO C##CDC_USER;

```

*(Opcional) Dica de Automação para o DBA:*
Como o Oracle não suporta sintaxe de *wildcard* (`GRANT SELECT ON SCHEMA.*`), caso o esquema possua muitas tabelas, o DBA pode executar o bloco PL/SQL abaixo para gerar as permissões granulares automaticamente, blindando o restante do PDB:

```sql
BEGIN
  FOR t IN (SELECT table_name FROM dba_tables WHERE owner = 'SCHEMA_ALVO') LOOP
    EXECUTE IMMEDIATE 'GRANT SELECT ON SCHEMA_ALVO.' || t.table_name || ' TO C##CDC_USER';
  END LOOP;
END;
/

```