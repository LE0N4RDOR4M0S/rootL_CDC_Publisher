# RootL CDC Publisher: Especificação Arquitetural

## 1. Escopo e Topologia do Sistema

O **RootL CDC Publisher** é um motor de extração passiva de eventos transacionais (Change Data Capture) desenhado para replicação sub-segundo em barramentos Kafka. A arquitetura viabiliza a ingestão contínua para camadas de Data Lake, fornecendo abstração de infraestrutura e persistência de estado para processos de ETL.

O sistema adota o padrão de **Arquitetura Hexagonal (Ports & Adapters)**, garantindo o isolamento absoluto entre o Core Domain (motor de eventos) e os detalhes de implementação dos bancos de origem e barramentos de destino.

## **2. Contrato de Domínio (Core)**

As transações capturadas são traduzidas para um modelo canônico dentro do domínio da aplicação. O tráfego de dados obedece ao contrato `ChangeEvent`, garantindo padronização independente da fonte geradora.

**Estrutura do `ChangeEvent`:**

- `eventId`: UUID v4 para rastreabilidade e deduplicação no *downstream*.
- `operation`: Enumeração do ciclo transacional (`INSERT`, `UPDATE`, `DELETE`, `BEGIN`, `COMMIT`).
- `eventTime`: Timestamp de efetivação (*commit*) no banco de origem.
- `metadata`: Objeto de rastreabilidade contendo `connectorId`, `database`, `schema`, `table`, `transactionId` e ponteiros de *offset*.
- `before`: Estado pré-mutação da tupla (fotografia completa).
- `after`: Estado pós-mutação (apenas *delta* em operações parciais).

## **3. Adaptadores de Entrada (Inbound Adapters)**

### 3.1. Oracle LogMiner Adapter

Módulo responsável por varrer logs físicos binários no ecossistema Oracle Database (12c+ Multitenant), sem a injeção de DML, DDL ou *triggers* de controle.

**Mecânicas de Operação:**

- **Isolamento de Sessão:** A execução ocorre invariavelmente no contexto global (`CDB$ROOT`) para habilitar acesso irrestrito aos Redo Logs ativos na SGA. O particionamento de leitura é imposto via filtro sistêmico de PDB (`SRC_CON_NAME = ?`).
- **Descoberta Dinâmica de Arquivos:** O adaptador gerencia a própria injeção de logs físicos na sessão. Executa *queries* de mapeamento nas *views* `SYS.V_$LOG` e `SYS.V_$ARCHIVED_LOG` cruzando com o SCN (*System Change Number*) de partida.
- **Algoritmo de Deduplicação (*Fingerprinting*):** Uma vez que um SCN reflete o momento de *commit* de uma transação inteira, sobreposições de leitura nos limites do cursor (`SCN >= ?`) são mitigadas por um cache volátil de assinaturas (`operation:table:sql_redo`), purgado a cada avanço no eixo do tempo.
- **Processamento de Delta (JSqlParser):** Mutações de `UPDATE` geradas pelo LogMiner replicam colunas inalteradas na cláusula `SET`. O *parser* interno processa a AST (Abstract Syntax Tree) do comando, cruza com a cláusula `WHERE` (*Before Image*) e emite para o *payload* apenas as propriedades que sofreram mutação real.

### 3.2. PostgreSQL Logical Decoding Adapter

Módulo de ingestão contínua orientado à arquitetura de Replicação Lógica do PostgreSQL (versão 10+), interceptando mutações diretamente no Write-Ahead Log (WAL) através de *streaming* de rede.

**Mecânicas de Operação:**

- **Gestão de Replication Slots:** Estabelece e consome um *logical replication slot* no servidor. O *slot* atua como uma âncora no banco, garantindo que os segmentos do WAL não sejam expurgados até que o adaptador confirme o processamento bem-sucedido (*acknowledgement*).
- **Decodificação de Saída (Output Plugin):** O tráfego de extração é delegado a *plugins* nativos do servidor (`pgoutput`), que enviam o WAL em formato pré-decodificado via protocolo de replicação.
- **Resolução de Estado (LSN):** O ponteiro transacional é controlado estritamente pelo *Log Sequence Number* (LSN). O avanço de *offset* no armazenamento interno consolida o *flush LSN* para o servidor, mitigando reprocessamento.
- **Integridade de Before Image:** A extração do estado pré-mutação das linhas é estruturalmente dependente da configuração da tabela no servidor alvo. Exige a declaração de `REPLICA IDENTITY FULL` nas tabelas monitoradas para viabilizar a presença dos campos antigos nos eventos de `UPDATE` e `DELETE`.

### **3.3. MySQL Binlog Adapter**

Motor de extração projetado para o protocolo de Binary Log do ecossistema MySQL (5.7+ e 8.0+). O conector emula o comportamento de um nó secundário para o banco de origem, consumindo *streams* nativos de replicação.

**Mecânicas de Operação:**

- **Extração Orientada a Tuplas (Row-Based):** Operação estritamente vinculada às configurações globais do servidor `binlog_format=ROW` e `binlog_row_image=FULL`. A abordagem inibe a decodificação de instruções de *statement* (SQL bruto), recebendo exclusivamente mutações estruturadas de dados.
- **Protocolo de Emulação de Réplica:** O adaptador estabelece comunicação via comando `COM_BINLOG_DUMP` (ou `COM_BINLOG_DUMP_GTID`). Topologicamente, o MySQL Mestre reconhece a aplicação como um *Slave Node* autêntico.
- **Identidade Transacional e Rastreamento:** A coordenação de *offset* utiliza preferencialmente o GTID (*Global Transaction Identifier*), garantindo tolerância a falhas na retomada térmica em ambientes de alta disponibilidade. Opera em degradação programada para mapeamento via arquivo e posição (*Binlog File + Position*) em configurações legadas.
- **Reconstrução de Catálogo e Mapeamento de Eventos:** Como o *Binlog* não trafega nomes de colunas sistematicamente em todas as versões, o adaptador intercepta `TableMapEvents` contidos no fluxo para reconstruir o dicionário em cache de memória e desofuscar as *arrays* nos eventos de mutação (`WriteRows`, `UpdateRows`, `DeleteRows`).

## 4. Adaptador de Saída e Contrato Kafka (Outbound)

O barramento atua como log de *append-only*. A topologia de tópicos reflete o nível da tabela de origem. Os eventos JSON emitidos são otimizados para integração direta com *frameworks* de processamento distribuído, como PySpark, operando sobre formatos colunares (Apache Iceberg).

**Payload Canônico (Exemplo de Mutação Parcial):**

```jsx
{
  "eventId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "operation": "UPDATE",
  "timestamp": "2026-05-25T14:30:15Z",
  "source": {
    "connectorName": "oracle-rh",
    "connectorType": "oracle",
    "database": "ORCLPDB1",
    "schema": "RH",
    "table": "FUNCIONARIOS",
    "transactionId": "ora-tx-4500122",
    "offsetCoordinates": {
      "scn": "4500122"
    }
  },
  "before": {
    "ID": "158",
    "NOME": "Leonardo Ramos",
    "CARGO": "Estagiário",
    "DEPARTAMENTO": "Inteligência"
  },
  "after": {
    "CARGO": "Analista de Desenvolvimento"
  }
}
```

## 5. Observabilidade e Estado

A camada de operação segue diretrizes estritas de engenharia de *software*, não mantendo estado de *offset* em memória. Os ponteiros (*checkpoints*) são confirmados em volume de disco persistente. Em cenários de interrupção, o *worker* reinicia a mineração estritamente a partir da coordenada armazenada, garantindo semântica de entrega *At-Least-Once*.

A aplicação expõe métricas nativas formatadas pelo Micrometer, publicadas no *endpoint* `/actuator/prometheus` para agregação via Prometheus/Grafana.

| **Métrica** | **Tipo** | **Definição** |
| --- | --- | --- |
| `cdc.connector.status` | Gauge | Estado de operação da thread de *pooling* (`1.0` = Ativo, `0.0` = Inativo/Falha). Avaliado por `connectorId`. |
| `cdc.events.processed.total` | Counter | Volume absoluto acumulado de *payloads* extraídos. Contém sub-labels para agregação analítica por `table` e `operation`. |
| `cdc.replication.lag.seconds` | Gauge | Delta temporal mensurado entre o *timestamp* da instrução extraída e o *clock* da inserção no *buffer* do barramento. |
| `cdc.scn.current` | Gauge | Registro de avanço do ponteiro transacional de leitura (Oracle SCN). Indicador direto de vazão ou ociosidade no ciclo do banco. |

## 6. Tratamento de Exceções e Resiliência (Dead Letter Queue)

A arquitetura assume que falhas de rede, corrupção de pacotes binários e dessincronismos de catálogo ocorrerão. Para evitar o travamento da *pipeline* ou a perda silenciosa de eventos, o sistema implementa políticas de resiliência:

- **Tolerância a Falhas Transientes:** Desconexões com o banco de origem ou indisponibilidade temporária do barramento Kafka acionam políticas de *Exponential Backoff*. A *thread* de extração é pausada temporariamente sem expurgar o cache de estado.
- **Isolamento de Falhas Lógicas (DLQ):** Eventos transacionais malformados ou anomalias de *parsing* (ex: falhas na reconstrução da AST do SQL no Oracle) não derrubam o *worker*. O *payload* bruto ofuscado é roteado para um tópico Kafka de **Dead Letter Queue (DLQ)**. O ponteiro transacional avança normalmente, isolando a anomalia para posterior inspeção e reprocessamento manual, garantindo a vazão do tráfego saudável.

## 7. Gestão de Estado e Controle de Offsets (*Checkpointing*)

A resiliência operacional do motor CDC depende estritamente do rastreamento determinístico da posição de leitura nos logs transacionais da origem. O RootL CDC Publisher implementa um controle de estado desacoplado do ciclo de vida da aplicação, operando sob o paradigma de processamento *Stateful*.

**Mecânicas de Controle e Persistência:**

- **Abstração de Coordenadas (Offset Coordinates):** O *Core Domain* normaliza os diferentes mecanismos de rastreamento transacional dos bancos de dados sob uma estrutura única de *offset*. O sistema rastreia o `SCN` (*System Change Number*) para Oracle, o `LSN` (*Log Sequence Number*) para PostgreSQL e o `GTID` ou `Binlog Position` para MySQL.
- **Isolamento via Hexagonal Architecture (`OffsetStorePort`):** A leitura e gravação da coordenada não ocorrem de forma acoplada aos adaptadores de banco. A persistência é delegada a uma porta de saída dedicada (`OffsetStorePort`). Isso viabiliza a implementação de diferentes estratégias de armazenamento do estado (ex: arquivos de texto locais, tópicos de configuração no próprio Kafka ou armazenamentos chave-valor como Redis), variando conforme a topologia da implantação.
- **Semântica de Entrega *At-Least-Once* (Pelo Menos Uma Vez):** A arquitetura garante tolerância a falhas através da estratégia de *Checkpointing* Seguro. O *offset* só é efetivamente comitado e persistido pelo adaptador no armazenamento de estado **após** a confirmação (*acknowledgement*) de que a mutação foi publicada com sucesso no barramento *downstream* (Kafka).
- **Retomada Térmica (*Warm Recovery*):** Em cenários de reinicialização (proposital ou por *crash* do *container*), os *workers* de mineração iniciam o ciclo interceptando a última coordenada válida na `OffsetStorePort`. O adaptador injeta esse *offset* diretamente na API de leitura nativa da origem (ex: parâmetro `STARTSCN` no Oracle LogMiner), garantindo continuidade transacional sem intervenção manual.