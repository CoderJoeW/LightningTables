package com.coderjoe.lightningtables.core.integration

import com.coderjoe.lightningtables.core.queries
import com.coderjoe.lightningtables.core.services.LightningTableTriggerGeneratorSqlParser
import org.jetbrains.exposed.v1.jdbc.deleteAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import com.coderjoe.lightningtables.core.database.TransactionsTable
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class SumCostByUserAtTimestampQueryTest : DockerComposeTestBase() {
    private val parser = LightningTableTriggerGeneratorSqlParser()
    private val query = queries["sumCostByUserAtTimestamp"]!!
    private val lightningTableName = "transactions_user_id_lightning"
    private val targetTimestamp = "2025-01-12 06:20:01"

    private fun setupTriggersAndLightningTable() {
        val result = parser.generate(query)
        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
            result.triggers.values.forEach { exec(it) }
        }
    }

    /** Inserts a transaction with updated_at explicitly set to the target timestamp (matches WHERE clause). */
    private fun insertTransactionAtTimestamp(userId: Int, cost: Double) {
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "INSERT INTO transactions (user_id, type, service, cost, updated_at) " +
                    "VALUES ($userId, 'DEBIT', 'CALL', $cost, '$targetTimestamp')",
            )
        }
    }

    /** Inserts a transaction with default updated_at (NOW()), which will NOT match the WHERE clause. */
    private fun insertTransaction(userId: Int, cost: Double) {
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "INSERT INTO transactions (user_id, type, service, cost) " +
                    "VALUES ($userId, 'DEBIT', 'CALL', $cost)",
            )
        }
    }

    private fun queryOriginalTable(): Map<Int, BigDecimal> {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery(query)
            val results = mutableMapOf<Int, BigDecimal>()
            while (rs.next()) {
                results[rs.getInt("user_id")] = rs.getBigDecimal("total_cost")
            }
            return results
        }
    }

    private fun queryLightningTable(): Map<Int, BigDecimal> {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT * FROM $lightningTableName")
            val results = mutableMapOf<Int, BigDecimal>()
            while (rs.next()) {
                results[rs.getInt("user_id")] = rs.getBigDecimal("total_cost")
            }
            return results
        }
    }

    private fun assertTablesMatch(context: String = "") {
        val original = queryOriginalTable()
        val lightning = queryLightningTable()
        assertEquals(original, lightning, "Lightning table should match original query $context".trim())
    }

    // --- Matching inserts (explicitly set updated_at to target timestamp) ---

    @Test
    fun `tables match after a single matching insert`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 25.00)
        assertTablesMatch("after single matching insert")
    }

    @Test
    fun `tables match after matching inserts for multiple users`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransactionAtTimestamp(2, 20.00)
        assertTablesMatch("after matching inserts for two users")
    }

    @Test
    fun `tables match after multiple matching inserts for same user`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransactionAtTimestamp(1, 20.00)
        insertTransactionAtTimestamp(1, 30.00)
        assertTablesMatch("after 3 matching inserts for same user")
    }

    // --- Non-matching inserts (default timestamp, should be invisible) ---

    @Test
    fun `tables match after a non-matching insert - both empty`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 25.00)
        assertTablesMatch("after non-matching insert (both should be empty)")
    }

    @Test
    fun `non-matching inserts do not appear in either table`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)
        insertTransaction(3, 30.00)
        val original = queryOriginalTable()
        val lightning = queryLightningTable()
        assertTrue(original.isEmpty(), "Original query should return nothing for non-matching inserts")
        assertTrue(lightning.isEmpty(), "Lightning table should return nothing for non-matching inserts")
    }

    // --- Mixed inserts (matching and non-matching across users) ---

    @Test
    fun `tables match with mix of matching and non-matching inserts for same user`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransaction(1, 50.00) // non-matching, should be invisible
        insertTransactionAtTimestamp(1, 20.00)
        assertTablesMatch("after mixed inserts for same user")
    }

    @Test
    fun `tables match with matching inserts for some users and non-matching for others`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransaction(2, 99.00) // non-matching
        insertTransactionAtTimestamp(3, 30.00)
        assertTablesMatch("after matching for users 1,3 and non-matching for user 2")
    }

    @Test
    fun `tables match with interleaved matching and non-matching across users`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransaction(1, 50.00)
        insertTransactionAtTimestamp(2, 20.00)
        insertTransaction(2, 60.00)
        insertTransactionAtTimestamp(1, 5.00)
        insertTransaction(3, 100.00)
        assertTablesMatch("after interleaved matching/non-matching across users")
    }

    // --- Delete matching rows ---

    @Test
    fun `tables match after deleting a matching row`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransactionAtTimestamp(1, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 1 AND updated_at = '$targetTimestamp' LIMIT 1",
            )
        }
        assertTablesMatch("after deleting one matching row")
    }

    @Test
    fun `tables match after deleting all matching rows for a user`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransactionAtTimestamp(1, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 1 AND updated_at = '$targetTimestamp'",
            )
        }
        assertTablesMatch("after deleting all matching rows (user should vanish)")
    }

    @Test
    fun `tables match after deleting matching rows from one user among many`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransactionAtTimestamp(2, 20.00)
        insertTransactionAtTimestamp(3, 30.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 2 AND updated_at = '$targetTimestamp'",
            )
        }
        assertTablesMatch("after deleting user 2's matching rows")
    }

    // --- Delete non-matching rows (lightning table should be unchanged) ---

    @Test
    fun `tables match after deleting a non-matching row`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransaction(1, 50.00) // non-matching

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 1 AND updated_at != '$targetTimestamp'",
            )
        }
        assertTablesMatch("after deleting non-matching row (lightning unchanged)")
    }

    @Test
    fun `tables match after deleting non-matching rows with matching rows present`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransaction(1, 50.00)
        insertTransaction(2, 60.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE updated_at != '$targetTimestamp'",
            )
        }
        assertTablesMatch("after deleting all non-matching rows")
    }

    // --- Update matching row (ON UPDATE CURRENT_TIMESTAMP causes row to leave filter) ---

    @Test
    fun `tables match after updating cost on a matching row - row exits filter`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransactionAtTimestamp(1, 20.00)

        // Updating cost via ORM triggers ON UPDATE CURRENT_TIMESTAMP, changing updated_at to NOW()
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET cost = 99.00 WHERE user_id = 1 AND updated_at = '$targetTimestamp' LIMIT 1",
            )
        }
        assertTablesMatch("after updating matching row (row exits filter due to ON UPDATE CURRENT_TIMESTAMP)")
    }

    @Test
    fun `tables match after updating all matching rows for a user - all exit filter`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransactionAtTimestamp(1, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET cost = 99.00 WHERE user_id = 1 AND updated_at = '$targetTimestamp'",
            )
        }
        assertTablesMatch("after updating all matching rows (user should vanish from results)")
    }

    // --- Update non-matching row's timestamp to target (row enters filter) ---

    @Test
    fun `tables match after setting non-matching row timestamp to target`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 50.00) // non-matching

        // Raw SQL to set updated_at to target, row enters filter
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET updated_at = '$targetTimestamp' WHERE user_id = 1",
            )
        }
        assertTablesMatch("after setting non-matching row's timestamp to target (row enters filter)")
    }

    @Test
    fun `tables match after setting multiple non-matching rows timestamp to target`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)
        insertTransaction(2, 30.00)

        // Make user 1's rows match the filter
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET updated_at = '$targetTimestamp' WHERE user_id = 1",
            )
        }
        assertTablesMatch("after making user 1's rows match the filter")
    }

    @Test
    fun `tables match after row enters then exits filter`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 50.00) // non-matching

        // Enter filter
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET updated_at = '$targetTimestamp' WHERE user_id = 1",
            )
        }
        assertTablesMatch("after row enters filter")

        // Exit filter by updating cost (ON UPDATE CURRENT_TIMESTAMP changes updated_at)
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET cost = 75.00 WHERE user_id = 1",
            )
        }
        assertTablesMatch("after row exits filter again")
    }

    // --- Bulk and edge cases ---

    @Test
    fun `tables match with zero cost matching insert`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 0.00)
        assertTablesMatch("after zero-cost matching insert")
    }

    @Test
    fun `tables match with large cost matching insert`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 99999999.99)
        assertTablesMatch("after large cost matching insert")
    }

    @Test
    fun `tables match after bulk matching inserts across users`() {
        setupTriggersAndLightningTable()
        repeat(20) { insertTransactionAtTimestamp(1, 1.00) }
        repeat(20) { insertTransactionAtTimestamp(2, 2.00) }
        repeat(20) { insertTransactionAtTimestamp(3, 3.00) }
        assertTablesMatch("after bulk matching inserts across users")
    }

    @Test
    fun `tables match after deleting all matching rows across all users`() {
        setupTriggersAndLightningTable()
        insertTransactionAtTimestamp(1, 10.00)
        insertTransactionAtTimestamp(2, 20.00)
        insertTransactionAtTimestamp(3, 30.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE updated_at = '$targetTimestamp'",
            )
        }
        assertTablesMatch("after deleting all matching rows (both should be empty)")
    }

    @Test
    fun `tables match after full cycle with matching and non-matching operations`() {
        setupTriggersAndLightningTable()

        // Phase 1: matching inserts
        insertTransactionAtTimestamp(1, 10.00)
        insertTransactionAtTimestamp(2, 20.00)
        assertTablesMatch("after initial matching inserts")

        // Phase 2: non-matching inserts (should not affect results)
        insertTransaction(1, 100.00)
        insertTransaction(3, 200.00)
        assertTablesMatch("after non-matching inserts")

        // Phase 3: more matching inserts
        insertTransactionAtTimestamp(1, 5.00)
        insertTransactionAtTimestamp(3, 15.00)
        assertTablesMatch("after additional matching inserts")

        // Phase 4: delete matching rows for user 2
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 2 AND updated_at = '$targetTimestamp'",
            )
        }
        assertTablesMatch("after deleting user 2's matching rows")

        // Phase 5: make a non-matching row enter the filter
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET updated_at = '$targetTimestamp' WHERE user_id = 1 AND updated_at != '$targetTimestamp'",
            )
        }
        assertTablesMatch("after making non-matching row enter filter")
    }

    @Test
    fun `tables match after inserting and deleting matching rows repeatedly`() {
        setupTriggersAndLightningTable()

        repeat(5) {
            insertTransactionAtTimestamp(1, 5.00)
            assertTablesMatch("after matching insert #${it + 1}")

            connect().use { conn ->
                conn.createStatement().executeUpdate(
                    "DELETE FROM transactions WHERE user_id = 1 AND updated_at = '$targetTimestamp' LIMIT 1",
                )
            }
            assertTablesMatch("after delete #${it + 1}")
        }
    }
}
