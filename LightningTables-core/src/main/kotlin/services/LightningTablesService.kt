package com.coderjoe.lightningtables.core.services

import com.coderjoe.lightningtables.core.database.model.LtTablesTable
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.deleteWhere
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
                it[LtTablesTable.insertTriggerName] = result.triggerNames["insert"]!!
                it[LtTablesTable.updateTriggerName] = result.triggerNames["update"]!!
                it[LtTablesTable.deleteTriggerName] = result.triggerNames["delete"]!!
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

    fun delete(entry: LtTableEntry): Boolean {
        return try {
            transaction {
                exec("DROP TRIGGER IF EXISTS `${entry.insertTriggerName}`")
                exec("DROP TRIGGER IF EXISTS `${entry.updateTriggerName}`")
                exec("DROP TRIGGER IF EXISTS `${entry.deleteTriggerName}`")
                exec("DROP TABLE IF EXISTS `${entry.tableName}`")
                LtTablesTable.deleteWhere { LtTablesTable.id eq entry.id }
            }
            true
        } catch (e: Exception) {
            false
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
