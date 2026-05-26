package com.leonardoramos.rootl_cdcpublisher.domain.model;

/**
 * Enum de tipos de operações representando as diferentes naturezas de mudanças que podem ocorrer nos dados, incluindo:
 * - INSERT: Representa a inserção de novos dados.
 * - UPDATE: Representa a atualização de dados existentes.
 * - DELETE: Representa a exclusão de dados.
 * - BEGIN: Representa o início de uma transação.
 * - COMMIT: Representa a confirmação de uma transação.
 * - READ: Representa a leitura de dados.
 */
public enum OperationType {
    INSERT,
    UPDATE,
    DELETE,
    BEGIN,
    COMMIT,
    READ
}
