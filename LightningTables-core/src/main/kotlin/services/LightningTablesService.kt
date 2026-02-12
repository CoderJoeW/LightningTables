package com.coderjoe.lightningtables.core.services

import com.coderjoe.lightningtables.core.database.model.LtTablesTable
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
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
            SchemaUtils.create(LtTablesTable)
        }
    }

    fun insert(result: TriggerGeneratorResult, query: String) {
        transaction {
            LtTablesTable.insert {
                it[LtTablesTable.ltTableName] = result.lightningTableName
                it[LtTablesTable.baseTableName] = result.backfillContext.baseTableName
                it[LtTablesTable.query] = query
                it[LtTablesTable.insertTriggerName] = "${result.lightningTableName}_after_insert_lightning"
                it[LtTablesTable.updateTriggerName] = "${result.lightningTableName}_after_update_lightning"
                it[LtTablesTable.deleteTriggerName] = "${result.lightningTableName}_after_delete_lightning"
            }
        }
    }
}
