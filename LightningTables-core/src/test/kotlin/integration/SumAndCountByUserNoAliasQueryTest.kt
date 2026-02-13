package com.coderjoe.lightningtables.core.integration

import com.coderjoe.lightningtables.core.database.TransactionService
import com.coderjoe.lightningtables.core.database.TransactionType
import com.coderjoe.lightningtables.core.database.TransactionsTable
import com.coderjoe.lightningtables.core.database.seeders.TransactionsSeeder
import com.coderjoe.lightningtables.core.queries
import com.coderjoe.lightningtables.core.services.LightningTableTriggerGeneratorSqlParser
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.jdbc.deleteAll
import org.jetbrains.exposed.v1.jdbc.deleteWhere
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.jetbrains.exposed.v1.jdbc.update
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal

/**
 * Tests that aggregates without explicit AS aliases work correctly.
 * The query uses SUM(cost) and COUNT(*) without aliases, which should
 * generate default column names: sum_cost and row_count.
 */
class SumAndCountByUserNoAliasQueryTest : DockerComposeTestBase() {
    private val parser = LightningTableTriggerGeneratorSqlParser()
    private val query = queries["sumAndCountByUserNoAlias"]!!
    private val lightningTableName = "transactions_user_id_lightning"

    private data class UserAggregates(
        val totalCost: BigDecimal,
        val recordCount: Long,
    )

    private fun setupTriggersAndLightningTable() {
        val result = parser.generate(query)
        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
            result.triggers.values.forEach { exec(it) }
        }
    }

    private fun queryOriginalTable(): Map<Int, UserAggregates> {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery(query)
            val results = mutableMapOf<Int, UserAggregates>()
            while (rs.next()) {
                results[rs.getInt("user_id")] =
                    UserAggregates(
                        totalCost = rs.getBigDecimal("SUM(cost)"),
                        recordCount = rs.getLong("COUNT(*)"),
                    )
            }
            return results
        }
    }

    private fun queryLightningTable(): Map<Int, UserAggregates> {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT * FROM $lightningTableName")
            val results = mutableMapOf<Int, UserAggregates>()
            while (rs.next()) {
                results[rs.getInt("user_id")] =
                    UserAggregates(
                        totalCost = rs.getBigDecimal("sum_cost"),
                        recordCount = rs.getLong("row_count"),
                    )
            }
            return results
        }
    }

    private fun assertTablesMatch(context: String = "") {
        val original = queryOriginalTable()
        val lightning = queryLightningTable()
        assertEquals(original, lightning, "Lightning table should match original query $context".trim())
    }

    private fun insertTransaction(
        userId: Int,
        cost: Double,
    ) {
        transaction {
            TransactionsTable.insert {
                it[TransactionsTable.userId] = userId
                it[TransactionsTable.type] = TransactionType.DEBIT.name
                it[TransactionsTable.service] = TransactionService.CALL.name
                it[TransactionsTable.cost] = cost
            }
        }
    }

    // --- Single insert scenarios ---

    @Test
    fun `tables match after a single insert`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 25.00)
        assertTablesMatch("after single insert")
    }

    @Test
    fun `tables match after inserting for different users`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)
        insertTransaction(3, 30.00)
        assertTablesMatch("after inserting for three different users")
    }

    // --- Multiple inserts for same user ---

    @Test
    fun `tables match after multiple inserts for the same user`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)
        insertTransaction(1, 30.00)
        assertTablesMatch("after 3 inserts for same user (sum=60, count=3)")
    }

    @Test
    fun `tables match after many small inserts accumulating`() {
        setupTriggersAndLightningTable()
        repeat(100) {
            insertTransaction(1, 0.01)
        }
        assertTablesMatch("after 100 x 0.01 inserts (sum=1.00, count=100)")
    }

    // --- Delete scenarios ---

    @Test
    fun `tables match after deleting one transaction from a user with multiple`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)
        insertTransaction(1, 30.00)

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after deleting one of three transactions")
    }

    @Test
    fun `tables match after deleting the only transaction for a user`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 50.00)

        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after deleting only transaction (user should vanish)")
    }

    @Test
    fun `tables match after deleting all transactions`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)
        insertTransaction(3, 30.00)

        transaction {
            TransactionsTable.deleteAll()
        }
        assertTablesMatch("after deleting everything (both should be empty)")
    }

    // --- Update scenarios (both SUM and COUNT should be correct) ---

    @Test
    fun `tables match after updating a transaction cost`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }, limit = 1) {
                it[cost] = 99.00
            }
        }
        assertTablesMatch("after updating one cost (sum changes, count unchanged)")
    }

    @Test
    fun `tables match after changing user_id on a transaction`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 2 WHERE user_id = 1 LIMIT 1",
            )
        }
        assertTablesMatch("after moving transaction from user 1 to user 2")
    }

    @Test
    fun `tables match after moving all transactions from one user to another`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)
        insertTransaction(2, 5.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 2 WHERE user_id = 1",
            )
        }
        assertTablesMatch("after moving all transactions from user 1 to user 2")
    }

    // --- Mixed operations ---

    @Test
    fun `tables match after full cycle of insert, update, delete across users`() {
        setupTriggersAndLightningTable()

        // Phase 1: inserts
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)
        insertTransaction(3, 30.00)
        assertTablesMatch("after initial inserts")

        // Phase 2: more inserts for same users
        insertTransaction(1, 5.00)
        insertTransaction(2, 10.00)
        assertTablesMatch("after additional inserts")

        // Phase 3: update cost
        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 15.00
            }
        }
        assertTablesMatch("after update on user 1")

        // Phase 4: deletes
        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 3 }
        }
        assertTablesMatch("after deleting user 3")

        // Phase 5: insert for deleted user
        insertTransaction(3, 100.00)
        assertTablesMatch("after re-inserting for user 3")
    }

    @Test
    fun `tables match after rapid insert-update-delete sequence`() {
        setupTriggersAndLightningTable()

        insertTransaction(1, 10.00)
        assertTablesMatch("after insert")

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 50.00
            }
        }
        assertTablesMatch("after update")

        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after delete (should be empty)")

        insertTransaction(1, 77.00)
        assertTablesMatch("after re-insert")
    }

    // --- Bulk operations ---

    @Test
    fun `tables match after bulk inserts then bulk deletes`() {
        setupTriggersAndLightningTable()

        repeat(50) { insertTransaction(1, 1.00) }
        repeat(50) { insertTransaction(2, 2.00) }
        assertTablesMatch("after bulk inserts")

        repeat(25) {
            transaction {
                TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
            }
        }
        assertTablesMatch("after deleting half of user 1's rows")
    }

    // --- Seeder-based tests ---

    @Test
    fun `tables match after seeding many transactions`() {
        setupTriggersAndLightningTable()
        TransactionsSeeder().seed(100)
        assertTablesMatch("after seeding 100 transactions")
    }

    @Test
    fun `tables match after seeding then deleting some`() {
        setupTriggersAndLightningTable()
        TransactionsSeeder().seed(50)
        assertTablesMatch("after seeding 50")

        transaction {
            TransactionsTable.deleteWhere(limit = 10) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after deleting 10 of user 1's seeded rows")
    }

    @Test
    fun `tables match after seeding then updating some`() {
        setupTriggersAndLightningTable()
        TransactionsSeeder().seed(50)
        assertTablesMatch("after seeding 50")

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }, limit = 10) {
                it[cost] = 999.99
            }
        }
        assertTablesMatch("after updating 10 of user 1's seeded rows")
    }

    // --- Edge cases ---

    @Test
    fun `tables match with zero-cost transactions`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 0.00)
        insertTransaction(1, 0.00)
        insertTransaction(1, 0.00)
        assertTablesMatch("with only zero-cost transactions (sum=0, count=3)")
    }

    @Test
    fun `tables match after inserting and deleting repeatedly`() {
        setupTriggersAndLightningTable()

        repeat(10) {
            insertTransaction(1, 5.00)
            assertTablesMatch("after insert #${it + 1}")

            transaction {
                TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
            }
            assertTablesMatch("after delete #${it + 1}")
        }
    }
}
