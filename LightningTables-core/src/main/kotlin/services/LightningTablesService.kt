package com.coderjoe.lightningtables.core.services

import com.coderjoe.lightningtables.core.database.model.LtTablesTable
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
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

    fun insert(
        result: TriggerGeneratorResult,
        query: String,
    ) {
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

    fun list(): List<LtTableEntry> {
        return transaction {
            LtTablesTable.selectAll().map { row ->
                LtTableEntry(
                    id = row[LtTablesTable.id],
                    tableName = row[LtTablesTable.ltTableName],
                    baseTableName = row[LtTablesTable.baseTableName],
                    query = row[LtTablesTable.query],
                    insertTriggerName = row[LtTablesTable.insertTriggerName],
                    updateTriggerName = row[LtTablesTable.updateTriggerName],
                    deleteTriggerName = row[LtTablesTable.deleteTriggerName],
                )
            }
        }
    }

    fun createLightningTable(result: TriggerGeneratorResult) {
        transaction {
            exec(result.lightningTable)
        }
    }
}

data class LtTableEntry(
    val id: Int,
    val tableName: String,
    val baseTableName: String,
    val query: String,
    val insertTriggerName: String,
    val updateTriggerName: String,
    val deleteTriggerName: String,
)
