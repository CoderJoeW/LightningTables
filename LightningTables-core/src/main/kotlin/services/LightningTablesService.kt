package com.coderjoe.lightningtables.core.services

import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

class LightningTablesService {
    fun ltTablesExists(): Boolean {
        return transaction {
            SchemaUtils.listTables().any {
                it == "lt_tables" || it.endsWith(".lt_tables")
            }
        }
    }

    fun createLtTables() {
        transaction {
            val conn = this.connection.connection as java.sql.Connection
            conn.createStatement().use { stmt ->
                stmt.execute(
                    """
                    CREATE TABLE IF NOT EXISTS lt_tables (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        table_name VARCHAR(255) NOT NULL,
                        base_table_name VARCHAR(255) NOT NULL,
                        query TEXT NOT NULL,
                        insert_trigger_name VARCHAR(255) NOT NULL,
                        update_trigger_name VARCHAR(255) NOT NULL,
                        delete_trigger_name VARCHAR(255) NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                    """.trimIndent(),
                )
            }
        }
    }
}
