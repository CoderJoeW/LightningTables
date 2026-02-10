package com.coderjoe.lightningtables.core.unit

import com.coderjoe.lightningtables.core.services.LightningTableTriggerGeneratorSqlParser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Unit tests for lightning table naming - automatic generation and custom specification.
 * Note: Tests with GROUP BY require database access and are in CustomTableNameIntegrationTest.
 */
class LightningTableNamingTest {
    private val parser = LightningTableTriggerGeneratorSqlParser()

    // --- Automatic table name generation tests (no GROUP BY) ---

    @Test
    fun `automatic naming for query without GROUP BY`() {
        val query =
            """
            SELECT COUNT(*) as record_count
            FROM transactions
            """.trimIndent()

        val result = parser.generate(query)

        assertEquals("transactions_lightning", result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `transactions_lightning`"))
    }

    @Test
    fun `automatic naming converts camelCase table names to snake_case`() {
        val query =
            """
            SELECT COUNT(*) as total
            FROM userTransactions
            """.trimIndent()

        val result = parser.generate(query)

        assertEquals("user_transactions_lightning", result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `user_transactions_lightning`"))
    }

    @Test
    fun `automatic naming handles table names with special characters`() {
        val query =
            """
            SELECT COUNT(*) as total
            FROM user_transactions_2024
            """.trimIndent()

        val result = parser.generate(query)

        assertEquals("user_transactions_2024_lightning", result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `user_transactions_2024_lightning`"))
    }

    @Test
    fun `automatic naming handles uppercase table names`() {
        val query =
            """
            SELECT COUNT(*) as total
            FROM TRANSACTIONS
            """.trimIndent()

        val result = parser.generate(query)

        assertEquals("transactions_lightning", result.lightningTableName)
    }

    // --- Custom table name specification tests (no GROUP BY) ---

    @Test
    fun `custom table name is used when specified`() {
        val query =
            """
            SELECT COUNT(*) as record_count
            FROM transactions
            """.trimIndent()

        val customName = "my_custom_summary_table"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `$customName`"))
    }

    @Test
    fun `custom table name with schema prefix`() {
        val query =
            """
            SELECT COUNT(*) as total
            FROM transactions
            """.trimIndent()

        val customName = "analytics.transaction_summary"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `$customName`"))
    }

    @Test
    fun `custom table name with underscores`() {
        val query =
            """
            SELECT COUNT(*) as total
            FROM transactions
            """.trimIndent()

        val customName = "lightning_table_summary_v2"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `$customName`"))
    }

    @Test
    fun `custom table name with COUNT aggregate generates correct structure`() {
        val query =
            """
            SELECT COUNT(*) as record_count
            FROM transactions
            """.trimIndent()

        val customName = "tx_count"
        val result = parser.generate(query, lightningTable = customName)

        // Verify table structure is correct
        assertTrue(result.lightningTable.contains("`lightning_id` TINYINT UNSIGNED NOT NULL DEFAULT 1"))
        assertTrue(result.lightningTable.contains("`record_count` BIGINT NOT NULL DEFAULT 0"))
        assertTrue(result.lightningTable.contains("PRIMARY KEY (`lightning_id`)"))
    }

    @Test
    fun `custom table name appears in backfill context for non-grouped query`() {
        val query =
            """
            SELECT COUNT(*) as record_count
            FROM transactions
            """.trimIndent()

        val customName = "backfill_test_summary"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.backfillContext.lightningTableName)
    }

    @Test
    fun `custom table name with numbers and no GROUP BY`() {
        val query =
            """
            SELECT COUNT(*) as total
            FROM transactions
            """.trimIndent()

        val customName = "summary_2024_q1"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `$customName`"))
    }

    @Test
    fun `custom table name with camelCase for non-grouped query`() {
        val query =
            """
            SELECT COUNT(*) as total
            FROM transactions
            """.trimIndent()

        val customName = "TransactionCount"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `$customName`"))
    }

    @Test
    fun `custom table name with prefix for non-grouped query`() {
        val query =
            """
            SELECT COUNT(*) as total
            FROM transactions
            """.trimIndent()

        val customName = "ltbl_transaction_count"
        val result = parser.generate(query, lightningTable = customName)

        assertEquals(customName, result.lightningTableName)
        assertTrue(result.lightningTable.contains("CREATE TABLE IF NOT EXISTS `$customName`"))
    }
}
