# Especificação de Permissões e Governança para CDC (Oracle Database)

**Documento de Arquitetura de Segurança e Governança de Dados**

Este documento descreve os pré-requisitos e configurações necessárias no banco de dados Oracle para a extração passiva de eventos via CDC (Change Data Capture).

A plataforma utiliza o recurso nativo **Oracle LogMiner** para minerar os *Redo Logs* e *Archive Logs*. A aplicação não instala agentes de terceiros, não cria *triggers* e não executa instruções de DDL ou DML no ambiente de produção. Todo o acesso é restrito à leitura transacional em memória.

Devido à arquitetura Multitenant do Oracle (12c+), o usuário de extração deve ser provisionado como um **Common User** (com o prefixo `C##`), operando na raiz do servidor (CDB$ROOT) para conseguir enxergar os logs globais que englobam todos os Pluggable Databases (PDBs).

---

## 1. Configuração da Instância (Archive e Supplemental Logging)

Para que o Oracle preserve o histórico de mutações e permita a leitura assíncrona, a instância precisa obrigatoriamente estar em modo `ARCHIVELOG`. 

Além disso, o `SUPPLEMENTAL LOGGING` completo é **mandatório**. Sem ele, o Oracle grava no log apenas as colunas que foram alteradas em um `UPDATE` ou as chaves no `DELETE`, impossibilitando a reconstrução do estado completo da linha (Before/After) no Data Lake.

**Executado pelo SYSDBA (Na raiz - CDB$ROOT):**
```sql
-- Requer janela de manutenção se o banco não estiver em ARCHIVELOG
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;

-- Ativação da captura completa de metadados transacionais (Before Image)
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

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

A aplicação atua de forma autônoma. Para isso, ela necessita de permissões explícitas para invocar os pacotes de controle do LogMiner e para pesquisar dinamicamente nas tabelas do sistema quais são os arquivos de Redo Log ativos e arquivados no disco a cada ciclo de mineração.

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

## 4. Escopo de Dados (Permissões de Tabela)

Embora a mineração ocorra na raiz (CDB), o `C##CDC_USER` precisa ter permissão de leitura (`SELECT`) nas tabelas publicadas no PDB. Isso é exigido internamente pelo Oracle para que o banco libere a visualização do dado desofuscado e resolva os mapeamentos de colunas no catálogo do banco gerando o `SQL_REDO`.

O filtro exato de qual Schema/PDB será lido é configurado nos parâmetros seguros da aplicação.

**Comando (Governança Centralizada):**

```sql
-- Acesso de leitura a todas as tabelas (O filtro de escopo real ocorre no software CDC)
GRANT SELECT ANY TABLE TO C##CDC_USER CONTAINER=ALL; 

```
ou então grants específicos por PDB/Schema:

```sql
-- Acesso de leitura apenas para o PDB específico (Ex: PDB_RH)
GRANT SELECT ON PDB_RH.* TO C##CDC_USER CONTAINER=ALL;
```
