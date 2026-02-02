package com.coderjoe.lightningtables.core.database.seeders

import com.coderjoe.lightningtables.core.database.TransactionService
import com.coderjoe.lightningtables.core.database.TransactionType
import com.coderjoe.lightningtables.core.database.TransactionsRepository
import kotlin.random.Random

class TransactionsSeeder {
    fun seed(recordCount: Int) {
        val repository = TransactionsRepository()

        val startTime = System.currentTimeMillis()

        repeat(recordCount) { index ->
            val cost = Random.nextDouble(0.01, 2.0)
            val service = TransactionService.entries.random()
            repository.insert(1, TransactionType.DEBIT, service, cost)

            if ((index + 1) % 10_000 == 0) {
                println("Inserted ${index + 1} records...")
            }
        }

        val endTime = System.currentTimeMillis()
        val duration = (endTime - startTime) / 1000.0

        println("Completed! Inserted $recordCount records in $duration seconds")
    }
}
