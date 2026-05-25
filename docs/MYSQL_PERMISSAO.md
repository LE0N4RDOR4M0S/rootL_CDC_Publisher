# Especificação de Permissões e Governança para CDC (MySQL)

**Documento de Arquitetura de Segurança e Governança de Dados**
Este documento descreve os pré-requisitos, permissões e configurações necessárias no banco de dados MySQL (ou MariaDB) para a operação da plataforma de Change Data Capture (CDC). 

A arquitetura adota o princípio de **Privilégio Mínimo (Least Privilege)**. A aplicação atua como uma leitora estritamente passiva. Não há criação de tabelas temporárias, *triggers* ou rotinas no banco de dados de origem.

---

## 1. Configurações em Nível de Instância (`my.cnf` / `my.ini`)

Para que o MySQL consiga emitir os eventos de mutação, o log binário (Binlog) precisa estar ativado e configurado no formato correto. Isso geralmente exige um *restart* do serviço se os parâmetros não estiverem ativos.

| Parâmetro | Valor Exigido | Motivo / Justificativa |
| :--- | :--- | :--- |
| `server-id` | `>= 1` | **Obrigatório.** Identificador único do servidor no cluster. Necessário para a replicação funcionar. |
| `log_bin` | `ON` (ou caminho) | Habilita a gravação do log binário no disco. |
| `binlog_format` | `ROW` | **Crítico.** Sem isso, o banco envia a instrução SQL textual (ex: `UPDATE tbl SET a=1 WHERE b=2`). No formato `ROW`, o banco envia o dado exato da linha alterada, garantindo determinismo na extração. |
| `binlog_row_image`| `FULL` | Garante que os eventos de `UPDATE` e `DELETE` contenham o estado completo da linha antes e depois da mutação, essencial para integridade em Data Lakes (Auditoria Completa). |

---

## 2. Usuário de Aplicação (Service Account)

Um usuário dedicado deve ser criado especificamente para a plataforma CDC, evitando o reuso de contas de sistemas (como root ou admin).

**Comando:**
```sql
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'sua_senha_segura';

```

---

## 3. Permissões de Replicação (O Acesso ao Binlog)

No ecossistema MySQL, a leitura do log transacional é uma operação inerentemente global em nível de instância.

**Comando:**

```sql
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'cdc_user'@'%';

```

**Justificativa e Governança:**

* `REPLICATION CLIENT`: Permite que o microserviço execute o comando `SHOW MASTER STATUS` para descobrir as coordenadas (Arquivo e Posição) atuais do log.
* `REPLICATION SLAVE`: Permite que o microserviço se conecte via protocolo TCP/IP solicitando o *stream* binário dos dados.
* *Nota ao DBA (O Escopo `*.*`):* O MySQL não permite conceder acesso ao Binlog apenas para um banco de dados específico. A permissão deve ser global. Para manter o isolamento de dados, **o filtro (Data Masking/Routing) é realizado em memória na camada da aplicação**. O conector descartará silenciosamente qualquer evento que não pertença ao banco de dados homologado para captura.

---

## 4. Permissões de Leitura e Snapshot (Dicionário de Dados)

O conector precisa saber o nome das colunas e os tipos de dados, além de realizar a carga inicial (Snapshot) das tabelas caso o CDC seja ligado pela primeira vez.

**Comando:**

```sql
GRANT SELECT ON meu_banco_rh.* TO 'cdc_user'@'%';
GRANT RELOAD ON *.* TO 'cdc_user'@'%';

```

**Justificativa:**

* `SELECT`: Limitado estritamente ao banco de dados alvo (ex: `meu_banco_rh`). Usado para consultar a `information_schema` (mapeamento de colunas) e para extrair os dados na fase de Snapshot Inicial.
* `RELOAD`: **Utilizado exclusivamente durante o Snapshot.** Permite que a aplicação execute `FLUSH TABLES WITH READ LOCK` por breves milissegundos. Isso garante que a aplicação anote a posição exata do log binário antes de ler as tabelas, evitando duplicação ou perda de dados entre o fim do *SELECT* e o início da leitura do Binlog. Após o Snapshot, essa permissão não é mais ativamente utilizada.

---

## 5. Proteção de Disco (Retenção do Binlog)

Ao contrário do PostgreSQL (que usa *Slots* que seguram o disco indefinidamente), a retenção do MySQL é controlada por tempo. Isso é uma vantagem para a estabilidade do servidor do banco de dados, mas requer alinhamento.

**Configuração Recomendada no Servidor:**

* `binlog_expire_logs_seconds = 259200` (MySQL 8.0+ / Equivale a 3 dias)
* `expire_logs_days = 3` (MySQL 5.7)

**Justificativa:**
O DBA deve configurar uma retenção que seja superior ao tempo máximo aceitável de indisponibilidade da nossa aplicação. Se o CDC ficar fora do ar num final de semana (ex: 48 horas) e a retenção for de apenas 24 horas, os logs serão apagados pelo MySQL e a aplicação não conseguirá retomar de onde parou, exigindo um novo Snapshot completo (recarga) das tabelas. Uma retenção de 3 a 7 dias é o padrão recomendado.

---

## 📝 Resumo DDL: Script de Provisionamento para o DBA

Abaixo, o script consolidado para preparação de um usuário MySQL para a plataforma de CDC no banco `rh_db`.

```sql
-- 1. Criação do Usuário
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'sua_senha_segura';

-- 2. Permissões de Leitura do Dicionário e Dados (Restrito ao DB alvo)
GRANT SELECT ON rh_db.* TO 'cdc_user'@'%';

-- 3. Permissões para captura do Log Transacional (Obrigatório ser Global)
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'cdc_user'@'%';

-- 4. Permissão para bloqueio de leitura no momento do Snapshot Inicial
GRANT RELOAD ON *.* TO 'cdc_user'@'%';

-- 5. Aplica as permissões
FLUSH PRIVILEGES;

```
