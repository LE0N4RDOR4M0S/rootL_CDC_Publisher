# Root L CDC Publisher

Uma plataforma de **Change Data Capture (CDC)** leve, modular e orientada à governança, construída em Java (Spring Boot) e desenhada com Arquitetura Hexagonal.

O CDC Publisher extrai mutações de dados (Inserts, Updates, Deletes) diretamente do log transacional de bancos de dados relacionais e as publica em tempo real em um barramento de eventos (Apache Kafka).

Diferente de soluções de prateleira (como Debezium ou Airbyte) que exigem permissões elevadas e acesso administrativo ao banco, este projeto foca no **Privilégio Mínimo** e na **Governança na Origem**. É a solução ideal para ambientes institucionais e corporativos (como órgãos governamentais) onde o controle do dado deve permanecer estritamente nas mãos do DBA.

## Por que este projeto existe?

Quando trabalhamos com grandes bases transacionais legadas ou de missão crítica, ferramentas padrão de CDC frequentemente causam atrito com as equipes de infraestrutura por exigirem permissões de criação de *slots*, tabelas temporárias e superusuários.

O **Root L CDC Publisher** resolve isso:

* **Leitura 100% Passiva:** Não cria tabelas, não altera configurações dinamicamente e não executa DDLs no banco de origem.
* **Orquestração por JSON:** Conectores são provisionados via arquivos de configuração (estilo Kafka Connect), permitindo adicionar novos bancos sem mexer no código ou recompilar a aplicação.
* **Buffer Transacional:** Agrupa eventos em memória e só os publica no Kafka mediante o sinal de COMMIT do banco, garantindo que *rollbacks* nunca sujem o seu Data Lake.
* **Snapshot Seguro sem Travar Produção:** A carga inicial usa o nível de isolamento REPEATABLE READ, congelando a visão dos dados no tempo sem aplicar bloqueios de escrita no banco da secretaria de origem.

## Arquitetura e Fluxo

O projeto utiliza **Arquitetura Hexagonal (Ports and Adapters)**, isolando completamente a lógica de processamento (CdcEngine) das tecnologias de infraestrutura.

1. **Engine Multi-Tenant:** A aplicação varre a pasta /connectors, carrega os arquivos .json e inicializa cada conector em sua própria *Thread* isolada.
2. **Snapshot (Carga Inicial):** Lê as tabelas publicadas e marca o ponteiro base.
3. **Replicação Lógica (Stream):** Conecta-se ao WAL do banco de dados (ex: *Logical Replication Slot* do PostgreSQL) e escuta as mutações.
4. **Processamento:** Normaliza os dados para o padrão agnóstico ChangeEvent e armazena os LSNs/Offsets localmente para garantir resiliência (*Crash Recovery*).
5. **Roteamento Dinâmico:** Publica no Kafka em tópicos nomeados automaticamente com base no *tenant* e *schema* (cdc.nome-do-conector.schema.tabela).

## Como Iniciar

### 1. Pré-requisitos

* Java 17+
* Kafka Rodando localmente ou remoto
* PostgreSQL 10+ configurado para Replicação Lógica (wal_level = logical)

### 2. Configuração do Conector

Crie um arquivo na pasta raiz /connectors/meu-banco.json:

```json
{
  "name": "postgres-financeiro",
  "class": "com.leonardoramos.rootl_cdcpublisher.adapters.inbound.postgres.PostgresReplicationAdapter",
  "config": {
    "jdbcUrl": "jdbc:postgresql://localhost:5432/financeiro?replication=database",
    "user": "cdc_user",
    "password": "sua-senha",
    "slotName": "cdc_slot",
    "database": "financeiro"
  }
}

```

### 3. Rodando o Projeto

Basta compilar e iniciar a aplicação Spring Boot:

```bash
mvn clean package
java -jar target/cdcpublisher-0.0.1-SNAPSHOT.jar

```

A aplicação detectará automaticamente o arquivo JSON e inicializará a extração em background.

## Stack Tecnológica

* **Java 17**
* **Spring Boot** (Injeção de Dependências e Bootstrap)
* **Apache Kafka** (Barramento de Eventos)
* **PostgreSQL JDBC Driver** (Com API nativa de *Logical Replication*)
* **Jackson** (Serialização e parsing dinâmico)