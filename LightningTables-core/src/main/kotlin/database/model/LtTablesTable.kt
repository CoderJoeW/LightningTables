package com.coderjoe.lightningtables.core.database.model

import org.jetbrains.exposed.v1.core.Table

object LtTablesTable : Table("lt_tables") {
    val id = integer("id").autoIncrement()
    val ltTableName = varchar("table_name", 255)
    val baseTableName = varchar("base_table_name", 255)
    val query = text("query")
    val insertTriggerName = varchar("insert_trigger_name", 255)
    val updateTriggerName = varchar("update_trigger_name", 255)
    val deleteTriggerName = varchar("delete_trigger_name", 255)
    override val primaryKey = PrimaryKey(id)
}
