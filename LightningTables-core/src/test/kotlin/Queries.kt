package com.coderjoe.lightningtables.core

val queries =
    mapOf(
        "sumCostByUser" to
            """
            SELECT user_id, SUM(cost) as total_cost
            FROM transactions
            GROUP BY user_id
            """.trimIndent(),
        "totalRecords" to
            """
            SELECT COUNT(*) as record_count
            FROM transactions
            """.trimIndent(),
        "totalRecordsByUserId" to
            """
            SELECT COUNT(*) as record_count, user_id
            FROM transactions
            GROUP BY user_id
            """.trimIndent(),
    )
