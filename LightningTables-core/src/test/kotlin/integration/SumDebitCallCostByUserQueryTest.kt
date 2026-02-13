package com.coderjoe.lightningtables.core.integration

import com.coderjoe.lightningtables.core.database.TransactionsTable
import com.coderjoe.lightningtables.core.queries
import com.coderjoe.lightningtables.core.services.LightningTableTriggerGeneratorSqlParser
import org.jetbrains.exposed.v1.jdbc.deleteAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class SumDebitCallCostByUserQueryTest : DockerComposeTestBase() {
    private val parser = LightningTableTriggerGeneratorSqlParser()
    private val query = queries["sumDebitCallCostByUser"]!!
    private val lightningTableName = "transactions_user_id_lightning"

    private fun setupTriggersAndLightningTable() {
        val result = parser.generate(query)
        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
            result.triggers.values.forEach { exec(it) }
        }
    }

    /** Inserts a transaction matching both WHERE conditions: type='DEBIT' AND service='CALL'. */
    private fun insertMatchingTransaction(
        userId: Int,
        cost: Double,
    ) {
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "INSERT INTO transactions (user_id, type, service, cost) " +
                    "VALUES ($userId, 'DEBIT', 'CALL', $cost)",
            )
        }
    }

    /** Inserts a transaction that fails the type condition (CREDIT instead of DEBIT). */
    private fun insertNonMatchingType(
        userId: Int,
        cost: Double,
    ) {
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "INSERT INTO transactions (user_id, type, service, cost) " +
                    "VALUES ($userId, 'CREDIT', 'CALL', $cost)",
            )
        }
    }

    /** Inserts a transaction that fails the service condition (SMS instead of CALL). */
    private fun insertNonMatchingService(
        userId: Int,
        cost: Double,
    ) {
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "INSERT INTO transactions (user_id, type, service, cost) " +
                    "VALUES ($userId, 'DEBIT', 'SMS', $cost)",
            )
        }
    }

    /** Inserts a transaction that fails both WHERE conditions. */
    private fun insertNonMatchingBoth(
        userId: Int,
        cost: Double,
    ) {
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "INSERT INTO transactions (user_id, type, service, cost) " +
                    "VALUES ($userId, 'CREDIT', 'DATA', $cost)",
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

    // --- Matching inserts (type='DEBIT' AND service='CALL') ---

    @Test
    fun `tables match after a single matching insert`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 25.00)
        assertTablesMatch("after single matching insert")
    }

    @Test
    fun `tables match after matching inserts for multiple users`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(2, 20.00)
        insertMatchingTransaction(3, 30.00)
        assertTablesMatch("after matching inserts for three users")
    }

    @Test
    fun `tables match after multiple matching inserts for same user`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(1, 20.00)
        insertMatchingTransaction(1, 30.00)
        assertTablesMatch("after 3 matching inserts for same user")
    }

    // --- Non-matching inserts (should not affect lightning table) ---

    @Test
    fun `tables match after non-matching type insert - both empty`() {
        setupTriggersAndLightningTable()
        insertNonMatchingType(1, 25.00)
        assertTablesMatch("after non-matching type insert (CREDIT instead of DEBIT)")
    }

    @Test
    fun `tables match after non-matching service insert - both empty`() {
        setupTriggersAndLightningTable()
        insertNonMatchingService(1, 25.00)
        assertTablesMatch("after non-matching service insert (SMS instead of CALL)")
    }

    @Test
    fun `tables match after non-matching both insert - both empty`() {
        setupTriggersAndLightningTable()
        insertNonMatchingBoth(1, 25.00)
        assertTablesMatch("after non-matching both conditions insert")
    }

    @Test
    fun `non-matching inserts across users do not appear in either table`() {
        setupTriggersAndLightningTable()
        insertNonMatchingType(1, 10.00)
        insertNonMatchingService(2, 20.00)
        insertNonMatchingBoth(3, 30.00)
        val original = queryOriginalTable()
        val lightning = queryLightningTable()
        assertTrue(original.isEmpty(), "Original query should return nothing for non-matching inserts")
        assertTrue(lightning.isEmpty(), "Lightning table should return nothing for non-matching inserts")
    }

    // --- Mixed matching and non-matching inserts ---

    @Test
    fun `tables match with mix of matching and non-matching for same user`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertNonMatchingType(1, 50.00)
        insertNonMatchingService(1, 60.00)
        insertMatchingTransaction(1, 20.00)
        assertTablesMatch("after mixed matching/non-matching for same user")
    }

    @Test
    fun `tables match with matching for some users and non-matching for others`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertNonMatchingType(2, 99.00)
        insertNonMatchingService(2, 88.00)
        insertMatchingTransaction(3, 30.00)
        assertTablesMatch("after matching for users 1,3 and non-matching for user 2")
    }

    @Test
    fun `tables match with interleaved matching and non-matching across users`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertNonMatchingType(1, 50.00)
        insertMatchingTransaction(2, 20.00)
        insertNonMatchingService(2, 60.00)
        insertNonMatchingBoth(3, 100.00)
        insertMatchingTransaction(1, 5.00)
        assertTablesMatch("after interleaved matching/non-matching across users")
    }

    // --- Delete matching rows ---

    @Test
    fun `tables match after deleting a matching row`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(1, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 1 AND type = 'DEBIT' AND service = 'CALL' LIMIT 1",
            )
        }
        assertTablesMatch("after deleting one matching row")
    }

    @Test
    fun `tables match after deleting all matching rows for a user`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(1, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 1 AND type = 'DEBIT' AND service = 'CALL'",
            )
        }
        assertTablesMatch("after deleting all matching rows (user should vanish)")
    }

    @Test
    fun `tables match after deleting matching rows from one user among many`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(2, 20.00)
        insertMatchingTransaction(3, 30.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 2 AND type = 'DEBIT' AND service = 'CALL'",
            )
        }
        assertTablesMatch("after deleting user 2's matching rows")
    }

    @Test
    fun `tables match after deleting all matching rows across all users`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(2, 20.00)
        insertMatchingTransaction(3, 30.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE type = 'DEBIT' AND service = 'CALL'",
            )
        }
        assertTablesMatch("after deleting all matching rows (both should be empty)")
    }

    // --- Delete non-matching rows (lightning table should be unchanged) ---

    @Test
    fun `tables match after deleting a non-matching row`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertNonMatchingType(1, 50.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 1 AND type = 'CREDIT'",
            )
        }
        assertTablesMatch("after deleting non-matching row (lightning unchanged)")
    }

    @Test
    fun `tables match after deleting non-matching service rows`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertNonMatchingService(1, 50.00)
        insertNonMatchingService(2, 60.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE service = 'SMS'",
            )
        }
        assertTablesMatch("after deleting all non-matching service rows")
    }

    // --- Update type: row exits filter ---

    @Test
    fun `tables match after changing type from DEBIT to CREDIT - row exits filter`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(1, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET type = 'CREDIT' WHERE user_id = 1 AND type = 'DEBIT' AND service = 'CALL' LIMIT 1",
            )
        }
        assertTablesMatch("after changing type to CREDIT (row exits filter)")
    }

    @Test
    fun `tables match after changing all matching rows type to CREDIT - user vanishes`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(1, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET type = 'CREDIT' WHERE user_id = 1 AND type = 'DEBIT' AND service = 'CALL'",
            )
        }
        assertTablesMatch("after changing all matching rows to CREDIT (user should vanish)")
    }

    // --- Update service: row exits filter ---

    @Test
    fun `tables match after changing service from CALL to SMS - row exits filter`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(1, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET service = 'SMS' WHERE user_id = 1 AND type = 'DEBIT' AND service = 'CALL' LIMIT 1",
            )
        }
        assertTablesMatch("after changing service to SMS (row exits filter)")
    }

    // --- Update type: row enters filter ---

    @Test
    fun `tables match after changing type from CREDIT to DEBIT - row enters filter`() {
        setupTriggersAndLightningTable()
        insertNonMatchingType(1, 50.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET type = 'DEBIT' WHERE user_id = 1",
            )
        }
        assertTablesMatch("after changing type to DEBIT (row enters filter)")
    }

    // --- Update service: row enters filter ---

    @Test
    fun `tables match after changing service from SMS to CALL - row enters filter`() {
        setupTriggersAndLightningTable()
        insertNonMatchingService(1, 50.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET service = 'CALL' WHERE user_id = 1",
            )
        }
        assertTablesMatch("after changing service to CALL (row enters filter)")
    }

    // --- Update both conditions simultaneously ---

    @Test
    fun `tables match after changing both type and service to matching - row enters filter`() {
        setupTriggersAndLightningTable()
        insertNonMatchingBoth(1, 50.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET type = 'DEBIT', service = 'CALL' WHERE user_id = 1",
            )
        }
        assertTablesMatch("after changing both to matching values (row enters filter)")
    }

    @Test
    fun `tables match after changing both type and service to non-matching - row exits filter`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 50.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET type = 'CREDIT', service = 'DATA' WHERE user_id = 1",
            )
        }
        assertTablesMatch("after changing both to non-matching values (row exits filter)")
    }

    // --- Update cost on matching row ---

    @Test
    fun `tables match after updating cost on a matching row`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(1, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET cost = 99.00 WHERE user_id = 1 AND type = 'DEBIT' AND service = 'CALL' LIMIT 1",
            )
        }
        assertTablesMatch("after updating cost on matching row")
    }

    // --- Update user_id on matching row (moves cost between groups) ---

    @Test
    fun `tables match after changing user_id on a matching row`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(2, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 2 WHERE user_id = 1 AND type = 'DEBIT' AND service = 'CALL'",
            )
        }
        assertTablesMatch("after moving matching row from user 1 to user 2")
    }

    // --- Row enters then exits filter ---

    @Test
    fun `tables match after row enters then exits filter via type change`() {
        setupTriggersAndLightningTable()
        insertNonMatchingType(1, 50.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET type = 'DEBIT' WHERE user_id = 1",
            )
        }
        assertTablesMatch("after row enters filter")

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET type = 'CREDIT' WHERE user_id = 1",
            )
        }
        assertTablesMatch("after row exits filter again")
    }

    @Test
    fun `tables match after row enters then exits filter via service change`() {
        setupTriggersAndLightningTable()
        insertNonMatchingService(1, 50.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET service = 'CALL' WHERE user_id = 1",
            )
        }
        assertTablesMatch("after row enters filter via service change")

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET service = 'DATA' WHERE user_id = 1",
            )
        }
        assertTablesMatch("after row exits filter via service change")
    }

    // --- Bulk and edge cases ---

    @Test
    fun `tables match with zero cost matching insert`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 0.00)
        assertTablesMatch("after zero-cost matching insert")
    }

    @Test
    fun `tables match with large cost matching insert`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 99999999.99)
        assertTablesMatch("after large cost matching insert")
    }

    @Test
    fun `tables match after bulk matching inserts across users`() {
        setupTriggersAndLightningTable()
        repeat(20) { insertMatchingTransaction(1, 1.00) }
        repeat(20) { insertMatchingTransaction(2, 2.00) }
        repeat(20) { insertMatchingTransaction(3, 3.00) }
        assertTablesMatch("after bulk matching inserts across users")
    }

    @Test
    fun `tables match after bulk matching inserts with non-matching noise`() {
        setupTriggersAndLightningTable()
        repeat(20) {
            insertMatchingTransaction(1, 1.00)
            insertNonMatchingType(1, 10.00)
            insertNonMatchingService(1, 10.00)
        }
        assertTablesMatch("after bulk matching inserts interleaved with non-matching")
    }

    // --- Full lifecycle ---

    @Test
    fun `tables match after full cycle with matching and non-matching operations`() {
        setupTriggersAndLightningTable()

        // Phase 1: matching inserts
        insertMatchingTransaction(1, 10.00)
        insertMatchingTransaction(2, 20.00)
        assertTablesMatch("after initial matching inserts")

        // Phase 2: non-matching inserts (should not affect results)
        insertNonMatchingType(1, 100.00)
        insertNonMatchingService(3, 200.00)
        insertNonMatchingBoth(2, 300.00)
        assertTablesMatch("after non-matching inserts")

        // Phase 3: more matching inserts
        insertMatchingTransaction(1, 5.00)
        insertMatchingTransaction(3, 15.00)
        assertTablesMatch("after additional matching inserts")

        // Phase 4: delete matching rows for user 2
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE user_id = 2 AND type = 'DEBIT' AND service = 'CALL'",
            )
        }
        assertTablesMatch("after deleting user 2's matching rows")

        // Phase 5: make a non-matching row enter the filter by changing type
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET type = 'DEBIT', service = 'CALL' WHERE user_id = 2 LIMIT 1",
            )
        }
        assertTablesMatch("after making non-matching row enter filter")

        // Phase 6: update cost on matching rows
        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET cost = 50.00 WHERE user_id = 1 AND type = 'DEBIT' AND service = 'CALL' LIMIT 1",
            )
        }
        assertTablesMatch("after updating cost on matching row")
    }

    @Test
    fun `tables match after inserting and deleting matching rows repeatedly`() {
        setupTriggersAndLightningTable()

        repeat(5) {
            insertMatchingTransaction(1, 5.00)
            assertTablesMatch("after matching insert #${it + 1}")

            connect().use { conn ->
                conn.createStatement().executeUpdate(
                    "DELETE FROM transactions WHERE user_id = 1 AND type = 'DEBIT' AND service = 'CALL' LIMIT 1",
                )
            }
            assertTablesMatch("after delete #${it + 1}")
        }
    }

    // --- Only one condition fails ---

    @Test
    fun `tables match when only type condition fails across many rows`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertNonMatchingType(1, 20.00)
        insertNonMatchingType(1, 30.00)
        insertNonMatchingType(1, 40.00)
        assertTablesMatch("only the DEBIT+CALL row should count")
    }

    @Test
    fun `tables match when only service condition fails across many rows`() {
        setupTriggersAndLightningTable()
        insertMatchingTransaction(1, 10.00)
        insertNonMatchingService(1, 20.00)
        insertNonMatchingService(1, 30.00)
        insertNonMatchingService(1, 40.00)
        assertTablesMatch("only the DEBIT+CALL row should count")
    }
}
