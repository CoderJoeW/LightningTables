package com.coderjoe

import com.coderjoe.database.TransactionService
import com.coderjoe.database.TransactionType
import com.coderjoe.database.TransactionsTable
import com.coderjoe.services.BackfillService
import com.coderjoe.services.SummaryTriggerGeneratorSqlParser
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class BackfillServiceTest : DockerComposeTestBase() {
    private val parser = SummaryTriggerGeneratorSqlParser()
    private val query = queries["sumCostByUser"]!!
    private val summaryTableName = "transactions_user_id_summary"

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    private fun generateResult() = parser.generate(query)

    private fun setupSummaryTableAndBackfill(chunkSize: Int = 10, threadCount: Int = 2) {
        val result = generateResult()
        transaction { exec(result.summaryTable) }
        BackfillService(chunkSize = chunkSize, threadCount = threadCount)
            .backfill(result.backfillContext, result.triggers.values.toList())
    }

    private fun reBackfill(chunkSize: Int = 10, threadCount: Int = 2) {
        val result = generateResult()
        BackfillService(chunkSize = chunkSize, threadCount = threadCount)
            .backfill(result.backfillContext, emptyList())
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

    private fun querySummaryTable(): Map<Int, BigDecimal> {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT * FROM $summaryTableName")
            val results = mutableMapOf<Int, BigDecimal>()
            while (rs.next()) {
                results[rs.getInt("user_id")] = rs.getBigDecimal("total_cost")
            }
            return results
        }
    }

    private fun summaryRowCount(): Int {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT COUNT(*) AS cnt FROM $summaryTableName")
            rs.next()
            return rs.getInt("cnt")
        }
    }

    private fun baseTableRowCount(): Int {
        connect().use { conn ->
            val rs = conn.createStatement().executeQuery("SELECT COUNT(*) AS cnt FROM transactions")
            rs.next()
            return rs.getInt("cnt")
        }
    }

    private fun insertTransaction(userId: Int, cost: Double) {
        transaction {
            TransactionsTable.insert {
                it[TransactionsTable.userId] = userId
                it[TransactionsTable.type] = TransactionType.DEBIT.name
                it[TransactionsTable.service] = TransactionService.CALL.name
                it[TransactionsTable.cost] = cost
            }
        }
    }

    private fun bulkInsertJdbc(rows: List<Triple<Int, String, Double>>) {
        connect().use { conn ->
            conn.prepareStatement(
                "INSERT INTO transactions (user_id, type, service, cost) VALUES (?, ?, ?, ?)"
            ).use { stmt ->
                rows.forEach { (userId, service, cost) ->
                    stmt.setInt(1, userId)
                    stmt.setString(2, "DEBIT")
                    stmt.setString(3, service)
                    stmt.setDouble(4, cost)
                    stmt.addBatch()
                }
                stmt.executeBatch()
            }
        }
    }

    /**
     * Resets users table and creates [count] users with IDs 1..[count].
     */
    private fun resetUsersTo(count: Int) {
        executeSQL("DELETE FROM transactions")
        executeSQL("DELETE FROM users")
        executeSQL("ALTER TABLE users AUTO_INCREMENT = 1")
        executeSQL("ALTER TABLE transactions AUTO_INCREMENT = 1")
        for (i in 1..count) {
            executeSQL("INSERT INTO users (first_name, last_name) VALUES ('User', '$i')")
        }
    }

    private fun assertSummaryMatchesOriginal(context: String = "") {
        val original = queryOriginalTable()
        val summary = querySummaryTable()
        assertEquals(original, summary, "Summary should match original query $context".trim())
    }

    // ---------------------------------------------------------------------------
    // 1. Basic correctness
    // ---------------------------------------------------------------------------

    @Test
    fun `backfill matches original query with seed data`() {
        setupSummaryTableAndBackfill()
        assertSummaryMatchesOriginal("after basic backfill")
    }

    @Test
    fun `backfill on empty table produces empty summary`() {
        executeSQL("DELETE FROM transactions")
        setupSummaryTableAndBackfill()
        assertEquals(0, summaryRowCount())
    }

    @Test
    fun `backfill single row table`() {
        executeSQL("DELETE FROM transactions")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'CALL', 42.50)")
        setupSummaryTableAndBackfill()

        val summary = querySummaryTable()
        assertEquals(1, summary.size)
        assertEquals(BigDecimal("42.50"), summary[1])
    }

    @Test
    fun `backfill all rows same user produces single group`() {
        executeSQL("DELETE FROM transactions")
        repeat(20) {
            executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'CALL', 1.00)")
        }
        setupSummaryTableAndBackfill(chunkSize = 5)

        assertEquals(1, summaryRowCount())
        assertEquals(BigDecimal("20.00"), querySummaryTable()[1])
    }

    @Test
    fun `backfill with zero cost transactions`() {
        executeSQL("DELETE FROM transactions")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'CALL', 0.00)")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'SMS', 5.00)")
        setupSummaryTableAndBackfill()

        assertEquals(BigDecimal("5.00"), querySummaryTable()[1])
    }

    @Test
    fun `backfill preserves fractional precision`() {
        executeSQL("DELETE FROM transactions")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'CALL', 0.01)")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'SMS', 0.02)")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'DATA', 0.03)")
        setupSummaryTableAndBackfill()

        assertEquals(BigDecimal("0.06"), querySummaryTable()[1])
    }

    @Test
    fun `backfill correct number of groups`() {
        setupSummaryTableAndBackfill()
        val originalGroups = queryOriginalTable().size
        val summaryGroups = summaryRowCount()
        assertEquals(originalGroups, summaryGroups)
        assertTrue(summaryGroups > 0)
    }

    // ---------------------------------------------------------------------------
    // 2. Chunk size variations
    // ---------------------------------------------------------------------------

    @Test
    fun `chunk size of 1 processes every row individually`() {
        setupSummaryTableAndBackfill(chunkSize = 1)
        assertSummaryMatchesOriginal("chunk size 1")
    }

    @Test
    fun `chunk size of 2`() {
        setupSummaryTableAndBackfill(chunkSize = 2)
        assertSummaryMatchesOriginal("chunk size 2")
    }

    @Test
    fun `chunk size equals row count`() {
        val rowCount = baseTableRowCount()
        setupSummaryTableAndBackfill(chunkSize = rowCount)
        assertSummaryMatchesOriginal("chunk size = row count")
    }

    @Test
    fun `chunk size exceeds row count`() {
        setupSummaryTableAndBackfill(chunkSize = 100_000)
        assertSummaryMatchesOriginal("chunk size >> row count")
    }

    @Test
    fun `chunk size of 3 with 10 rows creates uneven chunks`() {
        setupSummaryTableAndBackfill(chunkSize = 3)
        assertSummaryMatchesOriginal("uneven chunks")
    }

    // ---------------------------------------------------------------------------
    // 3. PK gaps and non-contiguous IDs
    // ---------------------------------------------------------------------------

    @Test
    fun `backfill handles PK gaps from deleted rows`() {
        executeSQL("DELETE FROM transactions WHERE id IN (2, 4, 6, 8)")
        setupSummaryTableAndBackfill(chunkSize = 2)
        assertSummaryMatchesOriginal("after PK gaps")
    }

    @Test
    fun `backfill handles large PK gap at start`() {
        executeSQL("DELETE FROM transactions")
        executeSQL("ALTER TABLE transactions AUTO_INCREMENT = 1000")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'CALL', 10.00)")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (2, 'DEBIT', 'SMS', 20.00)")
        setupSummaryTableAndBackfill(chunkSize = 5)
        assertSummaryMatchesOriginal("large PK gap at start")
    }

    @Test
    fun `backfill handles sparse PKs across wide range`() {
        executeSQL("DELETE FROM transactions")
        executeSQL("ALTER TABLE transactions AUTO_INCREMENT = 1")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'CALL', 5.00)")
        executeSQL("ALTER TABLE transactions AUTO_INCREMENT = 500")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'SMS', 15.00)")
        executeSQL("ALTER TABLE transactions AUTO_INCREMENT = 1000")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (2, 'DEBIT', 'DATA', 25.00)")

        setupSummaryTableAndBackfill(chunkSize = 10)
        assertSummaryMatchesOriginal("sparse PKs")
    }

    // ---------------------------------------------------------------------------
    // 4. Idempotency and re-backfill
    // ---------------------------------------------------------------------------

    @Test
    fun `backfill is idempotent - running twice produces same result`() {
        setupSummaryTableAndBackfill()
        val firstRun = querySummaryTable()

        reBackfill()
        val secondRun = querySummaryTable()

        assertEquals(firstRun, secondRun, "Two consecutive backfills should produce identical results")
        assertSummaryMatchesOriginal("after double backfill")
    }

    @Test
    fun `backfill three times in a row`() {
        setupSummaryTableAndBackfill()
        reBackfill()
        reBackfill()
        assertSummaryMatchesOriginal("after triple backfill")
    }

    @Test
    fun `re-backfill after trigger inserts resets to correct totals`() {
        setupSummaryTableAndBackfill()

        insertTransaction(1, 100.00)
        insertTransaction(2, 200.00)
        assertSummaryMatchesOriginal("after trigger inserts")

        reBackfill()
        assertSummaryMatchesOriginal("after re-backfill")
    }

    // ---------------------------------------------------------------------------
    // 5. Post-backfill trigger operations
    // ---------------------------------------------------------------------------

    @Test
    fun `triggers work after backfill - insert`() {
        setupSummaryTableAndBackfill()

        insertTransaction(1, 100.00)
        insertTransaction(2, 200.00)
        insertTransaction(3, 300.00)
        assertSummaryMatchesOriginal("after post-backfill inserts")
    }

    @Test
    fun `triggers work after backfill - insert for new user`() {
        executeSQL("INSERT INTO users (first_name, last_name) VALUES ('New', 'User')")
        setupSummaryTableAndBackfill()

        insertTransaction(4, 50.00)
        assertSummaryMatchesOriginal("after insert for new user")
    }

    @Test
    fun `triggers work after backfill - update single row`() {
        setupSummaryTableAndBackfill()

        transaction { exec("UPDATE transactions SET cost = 999.99 WHERE id = 1") }
        assertSummaryMatchesOriginal("after update")
    }

    @Test
    fun `triggers work after backfill - update all rows for one user`() {
        setupSummaryTableAndBackfill()

        transaction { exec("UPDATE transactions SET cost = 0.01 WHERE user_id = 1") }
        assertSummaryMatchesOriginal("after bulk update for user 1")
    }

    @Test
    fun `triggers work after backfill - delete single row`() {
        setupSummaryTableAndBackfill()

        transaction { exec("DELETE FROM transactions WHERE id = 1") }
        assertSummaryMatchesOriginal("after delete")
    }

    @Test
    fun `triggers work after backfill - delete all rows for one user`() {
        setupSummaryTableAndBackfill()

        transaction { exec("DELETE FROM transactions WHERE user_id = 3") }
        assertSummaryMatchesOriginal("after bulk delete of user 3")
    }

    @Test
    fun `triggers work after backfill - mixed insert update delete`() {
        setupSummaryTableAndBackfill()

        insertTransaction(1, 50.00)
        transaction { exec("UPDATE transactions SET cost = 0.01 WHERE id = 2") }
        transaction { exec("DELETE FROM transactions WHERE id = 3") }
        insertTransaction(2, 75.00)
        assertSummaryMatchesOriginal("after mixed operations")
    }

    @Test
    fun `triggers work after backfill - rapid sequential inserts`() {
        setupSummaryTableAndBackfill()

        for (i in 1..50) {
            insertTransaction((i % 3) + 1, 1.00)
        }
        assertSummaryMatchesOriginal("after 50 sequential inserts")
    }

    @Test
    fun `triggers work after backfill - update cost to zero`() {
        setupSummaryTableAndBackfill()

        transaction { exec("UPDATE transactions SET cost = 0.00 WHERE id = 1") }
        assertSummaryMatchesOriginal("after update to zero")
    }

    @Test
    fun `triggers work after backfill - delete then re-insert same user`() {
        setupSummaryTableAndBackfill()

        transaction { exec("DELETE FROM transactions WHERE user_id = 1") }
        assertSummaryMatchesOriginal("after deleting user 1")

        insertTransaction(1, 42.00)
        assertSummaryMatchesOriginal("after re-inserting for user 1")
    }

    // ---------------------------------------------------------------------------
    // 6. Scale - larger datasets
    // ---------------------------------------------------------------------------

    @Test
    fun `backfill 500 rows with tiny chunks`() {
        executeSQL("DELETE FROM transactions")
        val rows = (1..500).map { i ->
            Triple((i % 3) + 1, TransactionService.entries[i % 3].name, 1.00 + (i % 100) * 0.01)
        }
        bulkInsertJdbc(rows)

        setupSummaryTableAndBackfill(chunkSize = 7, threadCount = 4)
        assertSummaryMatchesOriginal("500 rows, chunk=7")
    }

    @Test
    fun `backfill 1000 rows distributed across 3 users`() {
        executeSQL("DELETE FROM transactions")
        val rows = (1..1000).map { i ->
            Triple((i % 3) + 1, "CALL", (i % 50).toDouble() + 0.50)
        }
        bulkInsertJdbc(rows)

        setupSummaryTableAndBackfill(chunkSize = 50, threadCount = 4)
        assertSummaryMatchesOriginal("1000 rows, 3 users")
    }

    @Test
    fun `backfill 1000 rows across 50 users`() {
        resetUsersTo(50)
        val rows = (1..1000).map { i ->
            Triple((i % 50) + 1, "SMS", 2.00 + (i % 10) * 0.10)
        }
        bulkInsertJdbc(rows)

        setupSummaryTableAndBackfill(chunkSize = 25, threadCount = 4)
        assertSummaryMatchesOriginal("1000 rows, 50 users")
        assertEquals(50, summaryRowCount(), "Should have 50 groups")
    }

    @Test
    fun `backfill 2000 rows with chunk size 1 single threaded`() {
        executeSQL("DELETE FROM transactions")
        val rows = (1..2000).map { i ->
            Triple((i % 3) + 1, "DATA", 0.50)
        }
        bulkInsertJdbc(rows)

        setupSummaryTableAndBackfill(chunkSize = 1, threadCount = 1)
        assertSummaryMatchesOriginal("2000 rows, chunk=1, threads=1")
    }

    @Test
    fun `backfill with maximum parallelism`() {
        executeSQL("DELETE FROM transactions")
        val rows = (1..500).map { i ->
            Triple((i % 3) + 1, "CALL", 1.00)
        }
        bulkInsertJdbc(rows)

        setupSummaryTableAndBackfill(chunkSize = 10, threadCount = 8)
        assertSummaryMatchesOriginal("500 rows, chunk=10, threads=8")
    }

    // ---------------------------------------------------------------------------
    // 7. Concurrent inserts DURING backfill (race condition tests)
    //
    // New inserts get PK > maxPk (auto-increment), so the backfill's
    // PK-bounded SELECT never includes them. Triggers handle them independently.
    // This is the safe concurrent DML pattern during backfill.
    //
    // NOTE: Concurrent UPDATEs/DELETEs of rows within the snapshot PK range are
    // NOT safe during backfill. The backfill reads current state (not a snapshot),
    // so a row deleted or updated between snapshot capture and chunk processing
    // causes the trigger delta and the backfill SELECT to disagree. This is an
    // inherent limitation without MVCC snapshot reads across connections. Backfill
    // should be run during a maintenance window if updates/deletes are expected.
    // ---------------------------------------------------------------------------

    @Test
    fun `concurrent inserts during backfill - steady stream`() {
        executeSQL("DELETE FROM transactions")
        val seedRows = (1..500).map { i ->
            Triple((i % 3) + 1, "CALL", 1.00)
        }
        bulkInsertJdbc(seedRows)

        val result = generateResult()
        transaction { exec(result.summaryTable) }

        val insertsCompleted = AtomicInteger(0)
        val backfillDone = AtomicBoolean(false)

        val inserter = Thread {
            while (!backfillDone.get()) {
                try {
                    connect().use { conn ->
                        conn.prepareStatement(
                            "INSERT INTO transactions (user_id, type, service, cost) VALUES (?, 'DEBIT', 'CALL', ?)"
                        ).use { stmt ->
                            stmt.setInt(1, (insertsCompleted.get() % 3) + 1)
                            stmt.setDouble(2, 0.10)
                            stmt.executeUpdate()
                        }
                    }
                    insertsCompleted.incrementAndGet()
                    Thread.sleep(1)
                } catch (_: Exception) {
                    // Lock may block temporarily
                }
            }
        }

        inserter.start()

        BackfillService(chunkSize = 10, threadCount = 2)
            .backfill(result.backfillContext, result.triggers.values.toList())

        backfillDone.set(true)
        inserter.join(5000)

        println("Concurrent inserts completed during backfill: ${insertsCompleted.get()}")
        assertTrue(insertsCompleted.get() > 0, "Should have inserted rows during backfill")

        assertSummaryMatchesOriginal("after concurrent inserts during backfill")
    }

    @Test
    fun `concurrent inserts from multiple writers during backfill`() {
        executeSQL("DELETE FROM transactions")
        val seedRows = (1..500).map { i ->
            Triple((i % 3) + 1, "CALL", 1.00)
        }
        bulkInsertJdbc(seedRows)

        val result = generateResult()
        transaction { exec(result.summaryTable) }

        val writerCount = 4
        val backfillDone = AtomicBoolean(false)
        val totalInserts = AtomicInteger(0)

        val writers = (1..writerCount).map { writerId ->
            Thread {
                while (!backfillDone.get()) {
                    try {
                        connect().use { conn ->
                            conn.prepareStatement(
                                "INSERT INTO transactions (user_id, type, service, cost) VALUES (?, 'DEBIT', 'SMS', ?)"
                            ).use { stmt ->
                                stmt.setInt(1, (writerId % 3) + 1)
                                stmt.setDouble(2, 0.25)
                                stmt.executeUpdate()
                            }
                        }
                        totalInserts.incrementAndGet()
                        Thread.sleep(1)
                    } catch (_: Exception) {
                        // Expected during lock
                    }
                }
            }
        }

        writers.forEach { it.start() }

        BackfillService(chunkSize = 10, threadCount = 2)
            .backfill(result.backfillContext, result.triggers.values.toList())

        backfillDone.set(true)
        writers.forEach { it.join(5000) }

        println("Total concurrent inserts from $writerCount writers: ${totalInserts.get()}")
        assertTrue(totalInserts.get() > 0, "Writers should have inserted rows")

        assertSummaryMatchesOriginal("after multi-writer concurrent inserts")
    }

    @Test
    fun `burst of inserts immediately after backfill lock releases`() {
        executeSQL("DELETE FROM transactions")
        val seedRows = (1..200).map { i ->
            Triple((i % 3) + 1, "CALL", 2.00)
        }
        bulkInsertJdbc(seedRows)

        val result = generateResult()
        transaction { exec(result.summaryTable) }

        val burstReady = CountDownLatch(1)
        val burstInserts = AtomicInteger(0)

        val burstWriters = (1..4).map {
            Thread {
                burstReady.await()
                try {
                    connect().use { conn ->
                        conn.prepareStatement(
                            "INSERT INTO transactions (user_id, type, service, cost) VALUES (?, 'DEBIT', 'DATA', ?)"
                        ).use { stmt ->
                            for (j in 1..50) {
                                stmt.setInt(1, (j % 3) + 1)
                                stmt.setDouble(2, 1.00)
                                stmt.executeUpdate()
                                burstInserts.incrementAndGet()
                            }
                        }
                    }
                } catch (_: Exception) {
                    // Acceptable
                }
            }
        }

        burstWriters.forEach { it.start() }

        val backfillThread = Thread {
            BackfillService(chunkSize = 5, threadCount = 2)
                .backfill(result.backfillContext, result.triggers.values.toList())
        }
        backfillThread.start()
        // Signal writers immediately — they block on the write lock,
        // then burst during chunk processing after unlock
        burstReady.countDown()

        backfillThread.join(30000)
        burstWriters.forEach { it.join(5000) }

        println("Burst inserts completed: ${burstInserts.get()}")
        assertSummaryMatchesOriginal("after burst inserts during backfill")
    }

    @Test
    fun `concurrent inserts for new users during backfill`() {
        // Add extra users so concurrent inserts can target them
        executeSQL("INSERT INTO users (first_name, last_name) VALUES ('Extra', 'One')")
        executeSQL("INSERT INTO users (first_name, last_name) VALUES ('Extra', 'Two')")

        val result = generateResult()
        transaction { exec(result.summaryTable) }

        val backfillDone = AtomicBoolean(false)
        val insertsCompleted = AtomicInteger(0)

        // Insert for user_ids 4 and 5 — these users have no pre-existing transactions
        val inserter = Thread {
            while (!backfillDone.get()) {
                try {
                    connect().use { conn ->
                        conn.prepareStatement(
                            "INSERT INTO transactions (user_id, type, service, cost) VALUES (?, 'DEBIT', 'CALL', ?)"
                        ).use { stmt ->
                            val userId = if (insertsCompleted.get() % 2 == 0) 4 else 5
                            stmt.setInt(1, userId)
                            stmt.setDouble(2, 10.00)
                            stmt.executeUpdate()
                        }
                    }
                    insertsCompleted.incrementAndGet()
                    Thread.sleep(1)
                } catch (_: Exception) {}
            }
        }

        inserter.start()

        BackfillService(chunkSize = 3, threadCount = 2)
            .backfill(result.backfillContext, result.triggers.values.toList())

        backfillDone.set(true)
        inserter.join(5000)

        println("Inserts for new users during backfill: ${insertsCompleted.get()}")
        assertTrue(insertsCompleted.get() > 0)
        assertSummaryMatchesOriginal("after concurrent inserts for new users")
    }

    // ---------------------------------------------------------------------------
    // 8. Stress: sustained high-throughput inserts during backfill
    // ---------------------------------------------------------------------------

    @Test
    fun `stress - high throughput inserts during backfill with many chunks`() {
        executeSQL("DELETE FROM transactions")
        val seedRows = (1..1000).map { i ->
            Triple((i % 3) + 1, "CALL", 1.00)
        }
        bulkInsertJdbc(seedRows)

        val result = generateResult()
        transaction { exec(result.summaryTable) }

        val backfillDone = AtomicBoolean(false)
        val totalInserted = AtomicInteger(0)

        val writers = (1..4).map { writerId ->
            Thread {
                while (!backfillDone.get()) {
                    try {
                        connect().use { conn ->
                            conn.prepareStatement(
                                "INSERT INTO transactions (user_id, type, service, cost) VALUES (?, 'DEBIT', 'DATA', ?)"
                            ).use { stmt ->
                                repeat(10) {
                                    stmt.setInt(1, (writerId % 3) + 1)
                                    stmt.setDouble(2, 0.10)
                                    stmt.addBatch()
                                }
                                stmt.executeBatch()
                            }
                        }
                        totalInserted.addAndGet(10)
                    } catch (_: Exception) {}
                }
            }
        }

        writers.forEach { it.start() }

        BackfillService(chunkSize = 5, threadCount = 4)
            .backfill(result.backfillContext, result.triggers.values.toList())

        backfillDone.set(true)
        writers.forEach { it.join(5000) }

        println("Stress test: ${totalInserted.get()} rows inserted during backfill")
        assertTrue(totalInserted.get() > 0, "Writers should have inserted rows")

        assertSummaryMatchesOriginal("after high-throughput stress test")
    }

    @Test
    fun `stress - backfill with 4 threads and concurrent 4-writer inserts across 20 users`() {
        resetUsersTo(20)
        val seedRows = (1..800).map { i ->
            Triple((i % 20) + 1, "CALL", 3.00)
        }
        bulkInsertJdbc(seedRows)

        val result = generateResult()
        transaction { exec(result.summaryTable) }

        val backfillDone = AtomicBoolean(false)
        val totalInserted = AtomicInteger(0)

        val writers = (1..4).map { writerId ->
            Thread {
                while (!backfillDone.get()) {
                    try {
                        connect().use { conn ->
                            conn.prepareStatement(
                                "INSERT INTO transactions (user_id, type, service, cost) VALUES (?, 'DEBIT', 'SMS', ?)"
                            ).use { stmt ->
                                for (j in 1..5) {
                                    val userId = ((writerId * 5 + j) % 20) + 1
                                    stmt.setInt(1, userId)
                                    stmt.setDouble(2, 0.50)
                                    stmt.addBatch()
                                }
                                stmt.executeBatch()
                            }
                        }
                        totalInserted.addAndGet(5)
                    } catch (_: Exception) {}
                }
            }
        }

        writers.forEach { it.start() }

        BackfillService(chunkSize = 20, threadCount = 4)
            .backfill(result.backfillContext, result.triggers.values.toList())

        backfillDone.set(true)
        writers.forEach { it.join(5000) }

        println("20-user stress: ${totalInserted.get()} rows inserted during backfill")
        assertSummaryMatchesOriginal("after 20-user stress test")
        assertEquals(20, summaryRowCount(), "Should have 20 groups")
    }

    @Test
    fun `stress - sustained inserts while backfill processes large sparse PK range`() {
        executeSQL("DELETE FROM transactions")
        // Create a sparse PK range to increase backfill duration
        executeSQL("ALTER TABLE transactions AUTO_INCREMENT = 1")
        for (i in 1..100) {
            executeSQL(
                "INSERT INTO transactions (user_id, type, service, cost) VALUES (${(i % 3) + 1}, 'DEBIT', 'CALL', 1.00)"
            )
        }
        // Jump to PK 5000
        executeSQL("ALTER TABLE transactions AUTO_INCREMENT = 5000")
        for (i in 1..100) {
            executeSQL(
                "INSERT INTO transactions (user_id, type, service, cost) VALUES (${(i % 3) + 1}, 'DEBIT', 'SMS', 2.00)"
            )
        }

        val result = generateResult()
        transaction { exec(result.summaryTable) }

        val backfillDone = AtomicBoolean(false)
        val totalInserted = AtomicInteger(0)

        val writer = Thread {
            while (!backfillDone.get()) {
                try {
                    connect().use { conn ->
                        conn.prepareStatement(
                            "INSERT INTO transactions (user_id, type, service, cost) VALUES (?, 'DEBIT', 'DATA', ?)"
                        ).use { stmt ->
                            stmt.setInt(1, (totalInserted.get() % 3) + 1)
                            stmt.setDouble(2, 0.50)
                            stmt.executeUpdate()
                        }
                    }
                    totalInserted.incrementAndGet()
                } catch (_: Exception) {}
            }
        }

        writer.start()

        // Tiny chunk size over large PK range = many chunks, long backfill
        BackfillService(chunkSize = 5, threadCount = 2)
            .backfill(result.backfillContext, result.triggers.values.toList())

        backfillDone.set(true)
        writer.join(5000)

        println("Sparse range stress: ${totalInserted.get()} rows inserted during backfill")
        assertSummaryMatchesOriginal("after sparse range stress test")
    }

    // ---------------------------------------------------------------------------
    // 9. Backfill thread-pool correctness
    // ---------------------------------------------------------------------------

    @Test
    fun `single backfill thread produces correct result`() {
        executeSQL("DELETE FROM transactions")
        val rows = (1..100).map { i ->
            Triple((i % 3) + 1, "CALL", 1.50)
        }
        bulkInsertJdbc(rows)

        setupSummaryTableAndBackfill(chunkSize = 10, threadCount = 1)
        assertSummaryMatchesOriginal("single backfill thread")
    }

    @Test
    fun `many backfill threads on small dataset`() {
        setupSummaryTableAndBackfill(chunkSize = 1, threadCount = 8)
        assertSummaryMatchesOriginal("8 threads on 10 rows")
    }

    @Test
    fun `backfill threads produce consistent sums across repeated runs`() {
        executeSQL("DELETE FROM transactions")
        val rows = (1..300).map { i ->
            Triple((i % 3) + 1, "CALL", 1.00)
        }
        bulkInsertJdbc(rows)

        setupSummaryTableAndBackfill(chunkSize = 7, threadCount = 4)
        val firstResult = querySummaryTable()

        for ((chunk, threads) in listOf(3 to 2, 11 to 3, 50 to 1, 1 to 4)) {
            reBackfill(chunkSize = chunk, threadCount = threads)
            assertEquals(firstResult, querySummaryTable(),
                "chunk=$chunk threads=$threads should match first run")
        }
    }

    @Test
    fun `concurrent chunk threads do not double-count shared group keys`() {
        // All rows for a single user — every chunk touches the same summary row.
        // Verifies ON DUPLICATE KEY UPDATE += is safe across concurrent chunk threads.
        executeSQL("DELETE FROM transactions")
        val rows = (1..200).map { Triple(1, "CALL", 1.00) }
        bulkInsertJdbc(rows)

        setupSummaryTableAndBackfill(chunkSize = 5, threadCount = 8)
        assertEquals(1, summaryRowCount())
        assertEquals(BigDecimal("200.00"), querySummaryTable()[1])
    }

    // ---------------------------------------------------------------------------
    // 10. Edge cases and error conditions
    // ---------------------------------------------------------------------------

    @Test
    fun `backfill handles moderately large cost values`() {
        executeSQL("DELETE FROM transactions")
        // DECIMAL(10,2) max is 99_999_999.99. Stay within range.
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'CALL', 50000000.00)")
        executeSQL("INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'SMS', 49999999.99)")
        setupSummaryTableAndBackfill()

        assertEquals(BigDecimal("99999999.99"), querySummaryTable()[1])
    }

    @Test
    fun `backfill with only one user having many transactions`() {
        executeSQL("DELETE FROM transactions")
        val rows = (1..200).map { Triple(1, "CALL", 0.01) }
        bulkInsertJdbc(rows)

        setupSummaryTableAndBackfill(chunkSize = 13)
        assertEquals(1, summaryRowCount())
        assertEquals(BigDecimal("2.00"), querySummaryTable()[1])
    }

    @Test
    fun `backfill validates updated_at column exists`() {
        executeSQL("CREATE TABLE IF NOT EXISTS no_ts_table (id INT AUTO_INCREMENT PRIMARY KEY, val INT)")
        try {
            val context = com.coderjoe.services.BackfillContext(
                baseTableName = "no_ts_table",
                summaryTableName = "no_ts_summary",
                groupByColumns = emptyList(),
                aggregates = listOf(com.coderjoe.services.AggregateInfo("COUNT", "*", "row_count")),
                whereClause = null
            )
            assertThrows(IllegalStateException::class.java) {
                BackfillService().backfill(context, emptyList())
            }
        } finally {
            executeSQL("DROP TABLE IF EXISTS no_ts_table")
        }
    }

    @Test
    fun `backfill with all identical costs`() {
        executeSQL("DELETE FROM transactions")
        for (i in 1..30) {
            executeSQL(
                "INSERT INTO transactions (user_id, type, service, cost) VALUES (${(i % 3) + 1}, 'DEBIT', 'CALL', 7.77)"
            )
        }
        setupSummaryTableAndBackfill(chunkSize = 4)
        assertSummaryMatchesOriginal("all identical costs")
    }

    @Test
    fun `backfill with alternating high and low costs`() {
        executeSQL("DELETE FROM transactions")
        for (i in 1..100) {
            val cost = if (i % 2 == 0) 9999.99 else 0.01
            executeSQL(
                "INSERT INTO transactions (user_id, type, service, cost) VALUES (${(i % 3) + 1}, 'DEBIT', 'CALL', $cost)"
            )
        }
        setupSummaryTableAndBackfill(chunkSize = 9)
        assertSummaryMatchesOriginal("alternating high/low costs")
    }

    // ---------------------------------------------------------------------------
    // 11. Data inserted BEFORE triggers exist is captured by backfill
    // ---------------------------------------------------------------------------

    @Test
    fun `pre-existing data without triggers is fully captured by backfill`() {
        val originalBefore = queryOriginalTable()
        assertTrue(originalBefore.isNotEmpty(), "Seed data should exist")

        setupSummaryTableAndBackfill()
        assertSummaryMatchesOriginal("pre-existing data captured")

        insertTransaction(1, 77.00)
        assertSummaryMatchesOriginal("pre-existing + trigger data")
    }

    @Test
    fun `large pre-existing dataset fully captured then triggers work`() {
        executeSQL("DELETE FROM transactions")
        val rows = (1..1000).map { i ->
            Triple((i % 3) + 1, "CALL", 1.00 + (i % 7) * 0.10)
        }
        bulkInsertJdbc(rows)

        setupSummaryTableAndBackfill(chunkSize = 25, threadCount = 4)
        assertSummaryMatchesOriginal("1000 rows captured")

        for (i in 1..100) {
            insertTransaction((i % 3) + 1, 0.50)
        }
        assertSummaryMatchesOriginal("after 100 trigger inserts")
    }

    @Test
    fun `backfill then high volume trigger inserts match original`() {
        setupSummaryTableAndBackfill()

        // Hammer triggers with rapid inserts after backfill
        val rows = (1..500).map { i ->
            Triple((i % 3) + 1, "CALL", 0.10)
        }
        bulkInsertJdbc(rows)

        assertSummaryMatchesOriginal("after 500 post-backfill bulk inserts")
    }

    // ---------------------------------------------------------------------------
    // 12. Lock correctness — writers blocked during lock window
    // ---------------------------------------------------------------------------

    @Test
    fun `writer is blocked during lock window and resumes after`() {
        executeSQL("DELETE FROM transactions")
        val seedRows = (1..100).map { i ->
            Triple((i % 3) + 1, "CALL", 1.00)
        }
        bulkInsertJdbc(seedRows)

        val result = generateResult()
        transaction { exec(result.summaryTable) }

        val writerStarted = CountDownLatch(1)
        val writerCompleted = AtomicBoolean(false)

        // This writer tries to insert immediately — it should be blocked by the lock
        val writer = Thread {
            writerStarted.countDown()
            try {
                connect().use { conn ->
                    conn.prepareStatement(
                        "INSERT INTO transactions (user_id, type, service, cost) VALUES (1, 'DEBIT', 'CALL', 99.00)"
                    ).use { stmt ->
                        // This will block until the lock is released
                        stmt.executeUpdate()
                    }
                }
                writerCompleted.set(true)
            } catch (_: Exception) {}
        }

        writer.start()
        writerStarted.await()
        // Small delay to ensure writer has attempted connection
        Thread.sleep(50)

        BackfillService(chunkSize = 25, threadCount = 2)
            .backfill(result.backfillContext, result.triggers.values.toList())

        writer.join(5000)
        assertTrue(writerCompleted.get(), "Writer should have completed after lock released")

        assertSummaryMatchesOriginal("after blocked writer completes")
    }
}
