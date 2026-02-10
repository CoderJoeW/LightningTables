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
}
