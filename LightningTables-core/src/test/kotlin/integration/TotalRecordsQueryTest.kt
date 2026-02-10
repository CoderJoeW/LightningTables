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

class TotalRecordsQueryTest : DockerComposeTestBase() {
    private val parser = LightningTableTriggerGeneratorSqlParser()
    private val query = queries["totalRecords"]!!
    private val lightningTableName = "transactions_lightning"

    private fun setupTriggersAndLightningTable() {
        val result = parser.generate(query)
        transaction {
            TransactionsTable.deleteAll()
            exec(result.lightningTable)
            result.triggers.values.forEach { exec(it) }
        }
    }

    private fun queryOriginalTable(): Long {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery(query)
            return if (rs.next()) {
                rs.getLong("record_count")
            } else {
                0L
            }
        }
    }

    private fun queryLightningTable(): Long {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT * FROM $lightningTableName")
            return if (rs.next()) {
                rs.getLong("record_count")
            } else {
                0L
            }
        }
    }

    private fun assertTablesMatch(context: String = "") {
        val original = queryOriginalTable()
        val lightning = queryLightningTable()
        assertEquals(original, lightning, "Lightning table should match original query $context".trim())
    }

    private fun insertTransaction(
        userId: Int,
        cost: Double = 10.00,
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

    // --- Initial state ---

    @Test
    fun `tables match with zero records`() {
        setupTriggersAndLightningTable()
        assertTablesMatch("with zero records")
    }

    // --- Single insert scenarios ---

    @Test
    fun `tables match after a single insert`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)
        assertTablesMatch("after single insert")
    }

    @Test
    fun `tables match after inserting for different users`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)
        insertTransaction(2)
        insertTransaction(3)
        assertTablesMatch("after inserting for three different users")
    }

    @Test
    fun `tables match after inserting zero-cost transaction`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 0.00)
        assertTablesMatch("after zero-cost insert")
    }

    @Test
    fun `tables match after inserting large cost`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 99999999.99)
        assertTablesMatch("after max decimal value insert")
    }

    // --- Multiple inserts ---

    @Test
    fun `tables match after multiple inserts for the same user`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)
        insertTransaction(1)
        insertTransaction(1)
        assertTablesMatch("after 3 inserts for same user")
    }

    @Test
    fun `tables match after many small inserts`() {
        setupTriggersAndLightningTable()
        repeat(100) {
            insertTransaction(1, 0.01)
        }
        assertTablesMatch("after 100 inserts")
    }

    @Test
    fun `tables match after interleaved inserts across multiple users`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)
        insertTransaction(2)
        insertTransaction(3)
        insertTransaction(1)
        insertTransaction(2)
        insertTransaction(3)
        assertTablesMatch("after interleaved multi-user inserts")
    }

    @Test
    fun `tables match after bulk inserts across users`() {
        setupTriggersAndLightningTable()
        val usersAndCounts = mapOf(1 to 50, 2 to 100, 3 to 25)
        usersAndCounts.forEach { (userId, count) ->
            repeat(count) {
                insertTransaction(userId, 2.00)
            }
        }
        assertTablesMatch("after bulk multi-user inserts (175 total)")
    }

    // --- Single delete scenarios ---

    @Test
    fun `tables match after deleting one transaction`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)
        insertTransaction(1)
        insertTransaction(1)

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after deleting one of three transactions")
    }

    @Test
    fun `tables match after deleting the only transaction`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)

        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after deleting only transaction (count should be 0)")
    }

    @Test
    fun `tables match after deleting all transactions for one user among many`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)
        insertTransaction(2)
        insertTransaction(2)
        insertTransaction(3)

        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 2 }
        }
        assertTablesMatch("after removing one user's transactions entirely")
    }

    @Test
    fun `tables match after deleting all transactions`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)
        insertTransaction(2)
        insertTransaction(3)

        transaction {
            TransactionsTable.deleteAll()
        }
        assertTablesMatch("after deleting everything (count should be 0)")
    }

    @Test
    fun `tables match after multiple sequential deletes`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)
        insertTransaction(1)
        insertTransaction(1)
        insertTransaction(1)

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after first delete")

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after second delete")

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after third delete")
    }

    @Test
    fun `tables match after deleting by cost filter`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 1.00)
        insertTransaction(1, 50.00)
        insertTransaction(2, 2.00)
        insertTransaction(2, 60.00)
        insertTransaction(3, 3.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE cost < 5.00",
            )
        }
        assertTablesMatch("after deleting low-cost rows")
    }

    // --- Update scenarios (updates don't affect count, but test for correctness) ---

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
        assertTablesMatch("after updating one cost (count unchanged)")
    }

    @Test
    fun `tables match after updating cost to zero`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 50.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 0.00
            }
        }
        assertTablesMatch("after updating cost to zero (count unchanged)")
    }

    @Test
    fun `tables match after updating cost to large value`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 1.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 99999999.99
            }
        }
        assertTablesMatch("after updating to max decimal value (count unchanged)")
    }

    @Test
    fun `tables match after updating all transactions`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)
        insertTransaction(1, 20.00)
        insertTransaction(2, 30.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 5.00
            }
        }
        assertTablesMatch("after updating all of a user's transactions (count unchanged)")
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
        assertTablesMatch("after moving a transaction from user 1 to user 2 (count unchanged)")
    }

    @Test
    fun `tables match after updating both cost and user_id`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)
        insertTransaction(2, 20.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "UPDATE transactions SET user_id = 2, cost = 50.00 WHERE user_id = 1 LIMIT 1",
            )
        }
        assertTablesMatch("after updating both cost and user_id (count unchanged)")
    }

    // --- Mixed operations ---

    @Test
    fun `tables match after insert then delete`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)
        insertTransaction(1)

        transaction {
            TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after insert then delete")
    }

    @Test
    fun `tables match after insert then update`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 50.00
            }
        }
        assertTablesMatch("after insert then update")
    }

    @Test
    fun `tables match after delete then insert`() {
        setupTriggersAndLightningTable()
        insertTransaction(1)
        insertTransaction(1)

        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after deleting all")

        insertTransaction(1)
        assertTablesMatch("after re-inserting")
    }

    @Test
    fun `tables match after full cycle of insert, update, delete`() {
        setupTriggersAndLightningTable()

        // Phase 1: inserts
        insertTransaction(1)
        insertTransaction(2)
        insertTransaction(3)
        assertTablesMatch("after initial inserts (3 records)")

        // Phase 2: updates (count unchanged)
        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 15.00
            }
        }
        assertTablesMatch("after update (still 3 records)")

        // Phase 3: more inserts
        insertTransaction(1)
        insertTransaction(2)
        assertTablesMatch("after additional inserts (5 records)")

        // Phase 4: deletes
        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 3 }
        }
        assertTablesMatch("after deleting user 3's transactions (4 records)")

        // Phase 5: insert after delete
        insertTransaction(3)
        assertTablesMatch("after re-inserting for user 3 (5 records)")
    }

    @Test
    fun `tables match after rapid insert-update-delete sequence`() {
        setupTriggersAndLightningTable()

        insertTransaction(1)
        assertTablesMatch("after insert (1 record)")

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 50.00
            }
        }
        assertTablesMatch("after update (still 1 record)")

        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 1 }
        }
        assertTablesMatch("after delete (0 records)")

        insertTransaction(1)
        assertTablesMatch("after re-insert (1 record)")
    }

    // --- Bulk operations ---

    @Test
    fun `tables match after bulk inserts then bulk deletes`() {
        setupTriggersAndLightningTable()

        repeat(50) { insertTransaction(1) }
        repeat(50) { insertTransaction(2) }
        assertTablesMatch("after bulk inserts (100 records)")

        // Delete half of user 1's transactions
        repeat(25) {
            transaction {
                TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
            }
        }
        assertTablesMatch("after deleting 25 records (75 remaining)")
    }

    @Test
    fun `tables match after bulk inserts then bulk updates`() {
        setupTriggersAndLightningTable()

        repeat(30) { insertTransaction(1) }
        repeat(30) { insertTransaction(2) }
        assertTablesMatch("after bulk inserts (60 records)")

        // Update all of user 1's costs (count unchanged)
        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 5.00
            }
        }
        assertTablesMatch("after bulk update (still 60 records)")
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
        assertTablesMatch("after deleting 10 seeded rows")
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
        assertTablesMatch("after updating 10 seeded rows (count unchanged)")
    }

    @Test
    fun `tables match after large seeding operation`() {
        setupTriggersAndLightningTable()
        TransactionsSeeder().seed(1000)
        assertTablesMatch("after seeding 1000 transactions")
    }

    // --- Edge cases ---

    @Test
    fun `tables match after inserting and deleting repeatedly to reach zero`() {
        setupTriggersAndLightningTable()

        repeat(10) {
            insertTransaction(1)
            assertTablesMatch("after insert #${it + 1}")

            transaction {
                TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
            }
            assertTablesMatch("after delete #${it + 1} (back to 0)")
        }
    }

    @Test
    fun `tables match after inserting many then deleting all at once`() {
        setupTriggersAndLightningTable()
        repeat(50) { insertTransaction(1) }
        repeat(50) { insertTransaction(2) }
        assertTablesMatch("after 100 inserts")

        transaction {
            TransactionsTable.deleteAll()
        }
        assertTablesMatch("after deleting all (0 records)")
    }

    @Test
    fun `tables match with only zero-cost transactions`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 0.00)
        insertTransaction(1, 0.00)
        insertTransaction(2, 0.00)
        assertTablesMatch("with only zero-cost transactions (3 records)")
    }

    @Test
    fun `tables match after deleting zero-cost transactions`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 0.00)
        insertTransaction(1, 10.00)
        insertTransaction(2, 0.00)
        insertTransaction(3, 5.00)

        connect().use { conn ->
            conn.createStatement().executeUpdate(
                "DELETE FROM transactions WHERE cost = 0.00",
            )
        }
        assertTablesMatch("after deleting zero-cost rows (2 records remaining)")
    }

    @Test
    fun `tables match after update that does not change the value`() {
        setupTriggersAndLightningTable()
        insertTransaction(1, 10.00)

        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 1 }) {
                it[cost] = 10.00
            }
        }
        assertTablesMatch("after no-op update (still 1 record)")
    }

    // --- Stress test ---

    @Test
    fun `tables match after many mixed operations`() {
        setupTriggersAndLightningTable()

        // Insert phase
        repeat(100) { insertTransaction(1) }
        repeat(100) { insertTransaction(2) }
        repeat(100) { insertTransaction(3) }
        assertTablesMatch("after 300 inserts")

        // Delete phase
        repeat(50) {
            transaction {
                TransactionsTable.deleteWhere(limit = 1) { TransactionsTable.userId eq 1 }
            }
        }
        assertTablesMatch("after deleting 50 records (250 remaining)")

        // Update phase (doesn't affect count)
        transaction {
            TransactionsTable.update({ TransactionsTable.userId eq 2 }) {
                it[cost] = 99.99
            }
        }
        assertTablesMatch("after bulk update (still 250 records)")

        // More inserts
        repeat(50) { insertTransaction(1) }
        assertTablesMatch("after adding 50 more (300 total)")

        // Delete all of user 3
        transaction {
            TransactionsTable.deleteWhere { TransactionsTable.userId eq 3 }
        }
        assertTablesMatch("after removing all user 3 transactions (200 remaining)")
    }
}
