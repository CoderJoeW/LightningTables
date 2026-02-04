package com.coderjoe.lightningtables.core.integration

import com.coderjoe.lightningtables.core.database.TransactionService
import com.coderjoe.lightningtables.core.database.TransactionType
import com.coderjoe.lightningtables.core.database.TransactionsTable
import com.coderjoe.lightningtables.core.queries
import com.coderjoe.lightningtables.core.services.LightningTableTriggerGeneratorSqlParser
import org.jetbrains.exposed.v1.jdbc.deleteAll
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class CustomTableNameIntegrationTest : DockerComposeTestBase() {
    private val parser = LightningTableTriggerGeneratorSqlParser()

    // --- Automatic table name generation tests with GROUP BY ---

    @Test
    fun `automatic naming for query with single GROUP BY column`() {
        val query = queries["sumCostByUser"]!!
        val result = parser.generate(query)

        assertEquals("transactions_user_id_lightning", result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `transactions_user_id_lightning`"))
    }

    @Test
    fun `automatic naming for query with COUNT and GROUP BY`() {
        val query = queries["totalRecordsByUserId"]!!
        val result = parser.generate(query)

        assertEquals("transactions_user_id_lightning", result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `transactions_user_id_lightning`"))
    }

    // --- Custom table names with automatic naming as comparison ---

    @Test
    fun `automatic naming creates expected table name for sumCostByUser query`() {
        val query = queries["sumCostByUser"]!!
        val result = parser.generate(query)

        assertEquals("transactions_user_id_lightning", result.lightningTableName)

        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
        }

        // Verify table was created with automatic name
        val tableExists = connect().use { conn ->
            val rs = conn.metaData.getTables(null, null, "transactions_user_id_lightning", null)
            rs.next()
        }
        assertEquals(true, tableExists)
    }

    @Test
    fun `custom table name creates table with specified name for sumCostByUser query`() {
        val query = queries["sumCostByUser"]!!
        val customName = "my_user_cost_summary"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)

        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
        }

        // Verify table was created with custom name
        val tableExists = connect().use { conn ->
            val rs = conn.metaData.getTables(null, null, customName, null)
            rs.next()
        }
        assertEquals(true, tableExists)

        // Clean up
        transaction {
            exec("DROP TABLE IF EXISTS `$customName`")
        }
    }

    @Test
    fun `custom table name works end-to-end with INSERT trigger`() {
        val query = queries["sumCostByUser"]!!
        val customName = "user_cost_totals"
        val result = parser.generate(query, lightningTable = customName)

        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
            result.triggers.values.forEach { exec(it) }
        }

        // Insert a transaction
        transaction {
            TransactionsTable.insert {
                it[userId] = 1
                it[type] = TransactionType.DEBIT.name
                it[service] = TransactionService.CALL.name
                it[cost] = 25.50
            }
        }

        // Verify data in custom table
        val customTableData = connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT * FROM `$customName`")
            val results = mutableMapOf<Int, BigDecimal>()
            while (rs.next()) {
                results[rs.getInt("user_id")] = rs.getBigDecimal("total_cost")
            }
            results
        }

        assertEquals(BigDecimal("25.50"), customTableData[1])

        // Clean up
        transaction {
            exec("DROP TRIGGER IF EXISTS transactions_after_insert_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_update_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_delete_lightning")
            exec("DROP TABLE IF EXISTS `$customName`")
        }
    }

    @Test
    fun `custom table name works with DELETE trigger`() {
        val query = queries["sumCostByUser"]!!
        val customName = "cost_by_user"
        val result = parser.generate(query, lightningTable = customName)

        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
            result.triggers.values.forEach { exec(it) }
        }

        // Insert and then delete a transaction
        val insertedId = transaction {
            TransactionsTable.insert {
                it[userId] = 1
                it[type] = TransactionType.DEBIT.name
                it[service] = TransactionService.CALL.name
                it[cost] = 15.00
            } get TransactionsTable.id
        }

        transaction {
            exec("DELETE FROM transactions WHERE id = $insertedId")
        }

        // Verify custom table is empty after delete
        val customTableCount = connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT COUNT(*) as cnt FROM `$customName`")
            rs.next()
            rs.getInt("cnt")
        }

        assertEquals(0, customTableCount)

        // Clean up
        transaction {
            exec("DROP TRIGGER IF EXISTS transactions_after_insert_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_update_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_delete_lightning")
            exec("DROP TABLE IF EXISTS `$customName`")
        }
    }

    @Test
    fun `custom table name works with COUNT aggregate`() {
        val query = queries["totalRecordsByUserId"]!!
        val customName = "tx_count_by_user"
        val result = parser.generate(query, lightningTable = customName)

        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
            result.triggers.values.forEach { exec(it) }
        }

        // Insert multiple transactions
        transaction {
            TransactionsTable.insert {
                it[userId] = 1
                it[type] = TransactionType.DEBIT.name
                it[service] = TransactionService.CALL.name
                it[cost] = 10.00
            }
            TransactionsTable.insert {
                it[userId] = 1
                it[type] = TransactionType.DEBIT.name
                it[service] = TransactionService.SMS.name
                it[cost] = 5.00
            }
            TransactionsTable.insert {
                it[userId] = 2
                it[type] = TransactionType.DEBIT.name
                it[service] = TransactionService.DATA.name
                it[cost] = 20.00
            }
        }

        // Verify counts in custom table
        val customTableData = connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT * FROM `$customName`")
            val results = mutableMapOf<Int, Long>()
            while (rs.next()) {
                results[rs.getInt("user_id")] = rs.getLong("record_count")
            }
            results
        }

        assertEquals(2L, customTableData[1])
        assertEquals(1L, customTableData[2])

        // Clean up
        transaction {
            exec("DROP TRIGGER IF EXISTS transactions_after_insert_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_update_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_delete_lightning")
            exec("DROP TABLE IF EXISTS `$customName`")
        }
    }

    @Test
    fun `custom table name with prefix convention`() {
        val query = queries["sumCostByUser"]!!
        val customName = "ltbl_user_cost_summary"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)

        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
            result.triggers.values.forEach { exec(it) }
        }

        // Insert transaction
        transaction {
            TransactionsTable.insert {
                it[userId] = 1
                it[type] = TransactionType.DEBIT.name
                it[service] = TransactionService.CALL.name
                it[cost] = 100.00
            }
        }

        // Verify custom table has data
        val hasData = connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT COUNT(*) as cnt FROM `$customName`")
            rs.next()
            rs.getInt("cnt") > 0
        }

        assertEquals(true, hasData)

        // Clean up
        transaction {
            exec("DROP TRIGGER IF EXISTS transactions_after_insert_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_update_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_delete_lightning")
            exec("DROP TABLE IF EXISTS `$customName`")
        }
    }

    @Test
    fun `custom table name matches original query results`() {
        val query = queries["sumCostByUser"]!!
        val customName = "summary_table"
        val result = parser.generate(query, lightningTable = customName)

        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
            result.triggers.values.forEach { exec(it) }
        }

        // Insert test data
        transaction {
            TransactionsTable.insert {
                it[userId] = 1
                it[type] = TransactionType.DEBIT.name
                it[service] = TransactionService.CALL.name
                it[cost] = 10.00
            }
            TransactionsTable.insert {
                it[userId] = 1
                it[type] = TransactionType.DEBIT.name
                it[service] = TransactionService.SMS.name
                it[cost] = 5.00
            }
            TransactionsTable.insert {
                it[userId] = 2
                it[type] = TransactionType.DEBIT.name
                it[service] = TransactionService.DATA.name
                it[cost] = 20.00
            }
        }

        // Query original aggregation
        val originalResults = connect().use { conn ->
            val rs = conn.createStatement().executeQuery(query)
            val results = mutableMapOf<Int, BigDecimal>()
            while (rs.next()) {
                results[rs.getInt("user_id")] = rs.getBigDecimal("total_cost")
            }
            results
        }

        // Query custom lightning table
        val lightningResults = connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT * FROM `$customName`")
            val results = mutableMapOf<Int, BigDecimal>()
            while (rs.next()) {
                results[rs.getInt("user_id")] = rs.getBigDecimal("total_cost")
            }
            results
        }

        // Verify they match
        assertEquals(originalResults, lightningResults)

        // Clean up
        transaction {
            exec("DROP TRIGGER IF EXISTS transactions_after_insert_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_update_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_delete_lightning")
            exec("DROP TABLE IF EXISTS `$customName`")
        }
    }

    @Test
    fun `custom table name works with camelCase naming`() {
        val query = queries["totalRecords"]!!
        val customName = "TransactionCount"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)

        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
            result.triggers.values.forEach { exec(it) }
        }

        // Insert transaction
        transaction {
            TransactionsTable.insert {
                it[userId] = 1
                it[type] = TransactionType.DEBIT.name
                it[service] = TransactionService.CALL.name
                it[cost] = 50.00
            }
        }

        // Verify count in custom table
        val count = connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT * FROM `$customName`")
            rs.next()
            rs.getLong("record_count")
        }

        assertEquals(1L, count)

        // Clean up
        transaction {
            exec("DROP TRIGGER IF EXISTS transactions_after_insert_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_update_lightning")
            exec("DROP TRIGGER IF EXISTS transactions_after_delete_lightning")
            exec("DROP TABLE IF EXISTS `$customName`")
        }
    }

    @Test
    fun `multiple custom tables can coexist for same base table`() {
        val query = queries["sumCostByUser"]!!

        val customName1 = "summary_v1"
        val customName2 = "summary_v2"

        val result1 = parser.generate(query, lightningTable = customName1)
        val result2 = parser.generate(query, lightningTable = customName2)

        transaction {
            TransactionsTable.deleteAll()
            exec(result1.lightningTable)
            exec(result2.lightningTable)
        }

        // Verify both tables exist
        val table1Exists = connect().use { conn ->
            val rs = conn.metaData.getTables(null, null, customName1, null)
            rs.next()
        }
        val table2Exists = connect().use { conn ->
            val rs = conn.metaData.getTables(null, null, customName2, null)
            rs.next()
        }

        assertEquals(true, table1Exists)
        assertEquals(true, table2Exists)

        // Clean up
        transaction {
            exec("DROP TABLE IF EXISTS `$customName1`")
            exec("DROP TABLE IF EXISTS `$customName2`")
        }
    }

    // --- Tests for custom table names with GROUP BY queries ---

    @Test
    fun `custom table name overrides automatic generation for grouped query`() {
        val query = queries["sumCostByUser"]!!
        val customName = "user_cost_summary"
        val result = parser.generate(query, lightningTable = customName)

        // Verify custom name is used instead of automatic "transactions_user_id_lightning"
        assertEquals(customName, result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `$customName`"))

        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
        }

        // Verify table was created with custom name
        val tableExists = connect().use { conn ->
            val rs = conn.metaData.getTables(null, null, customName, null)
            rs.next()
        }
        assertEquals(true, tableExists)

        // Clean up
        transaction {
            exec("DROP TABLE IF EXISTS `$customName`")
        }
    }

    @Test
    fun `custom table name with different naming convention for GROUP BY query`() {
        val query = queries["totalRecordsByUserId"]!!
        val customName = "TransactionCountByUser"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `$customName`"))

        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
        }

        // Verify table structure is correct with custom name
        val tableExists = connect().use { conn ->
            val rs = conn.metaData.getTables(null, null, customName, null)
            rs.next()
        }
        assertEquals(true, tableExists)

        // Clean up
        transaction {
            exec("DROP TABLE IF EXISTS `$customName`")
        }
    }

    @Test
    fun `custom table name works with triggers for GROUP BY query`() {
        val query = queries["sumCostByUser"]!!
        val customName = "custom_user_summary"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)

        // Verify all three triggers are generated
        assertTrue(result.triggers.containsKey("insert"))
        assertTrue(result.triggers.containsKey("update"))
        assertTrue(result.triggers.containsKey("delete"))
        assertEquals(3, result.triggers.size)
    }

    @Test
    fun `custom table name appears in backfill context for GROUP BY query`() {
        val query = queries["sumCostByUser"]!!
        val customName = "backfill_summary"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.backfillContext.lightningTableName)
        assertEquals("transactions", result.backfillContext.baseTableName)
        assertEquals(listOf("user_id"), result.backfillContext.groupByColumns)
    }

    @Test
    fun `custom table name with prefix for GROUP BY query`() {
        val query = queries["sumCostByUser"]!!
        val customName = "ltbl_transactions_user_summary"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `$customName`"))

        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
        }

        val tableExists = connect().use { conn ->
            val rs = conn.metaData.getTables(null, null, customName, null)
            rs.next()
        }
        assertEquals(true, tableExists)

        // Clean up
        transaction {
            exec("DROP TABLE IF EXISTS `$customName`")
        }
    }

    @Test
    fun `custom table name with numbers for GROUP BY query`() {
        val query = queries["totalRecordsByUserId"]!!
        val customName = "summary_2024_q1"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `$customName`"))
    }
}
