package com.coderjoe

import com.coderjoe.database.DatabaseConfig
import com.coderjoe.services.SummaryTriggerGeneratorSqlParser

val query =
    """
    SELECT user_id, SUM(cost) as total_cost
    FROM transactions
    GROUP BY user_id
    """.trimIndent()

val parser = SummaryTriggerGeneratorSqlParser()

fun main() {
    DatabaseConfig.initialize(
        url = "jdbc:mariadb://localhost:3306/summaryslayer",
        username = "root",
        password = "rootpassword",
    )

    val result = parser.generate(query)

    testSummaryGen()

//    Transactions().seed(1000)
}

fun testSummaryGen() {
    val parser = SummaryTriggerGeneratorSqlParser()

    // Example query with GROUP BY
    val query =
        """
        SELECT user_id, SUM(cost) as total_cost
        FROM transactions
        GROUP BY user_id
        """.trimIndent()

    try {
        val result = parser.generate(query)

        println("=== Generated Summary Table DDL ===")
        println(result.summaryTable)
        println()

        println("=== Generated Triggers ===")
        println("\n-- INSERT Trigger:")
        println(result.triggers["insert"])
        println("\n-- UPDATE Trigger:")
        println(result.triggers["update"])
        println("\n-- DELETE Trigger:")
        println(result.triggers["delete"])
        println()

        println("=== Complete Preview ===")
        println(result.preview)
    } catch (e: Exception) {
        println("Error generating triggers: ${e.message}")
        e.printStackTrace()
    }
}
