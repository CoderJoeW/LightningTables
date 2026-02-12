package com.coderjoe.lightningtables.cli.menus

import com.coderjoe.lightningtables.cli.menus.utils.ConsoleInputHelper
import com.coderjoe.lightningtables.core.services.BackfillService
import com.coderjoe.lightningtables.core.services.LightningTableTriggerGeneratorSqlParser
import com.coderjoe.lightningtables.core.services.LightningTablesService

object MainMenu {
    val sqlParser = LightningTableTriggerGeneratorSqlParser()
    val lightningTableService = LightningTablesService()

    fun load() {
        var exit = false

        while (!exit) {
            val input =
                ConsoleInputHelper.getInputWithLabel(
                    """
                    Menu:
                    1. Create new lightning table
                    2. List lightning tables
                    3. Exit
                """,
                )

            when (input) {
                "1" -> {
                    val query = ConsoleInputHelper.getInputWithLabel("Enter query to create lightning table: ")
                    val tableName = ConsoleInputHelper.getInputWithLabel("Enter name for the lightning table: ")
                    val result = sqlParser.generate(query, tableName)

                    lightningTableService.createLightningTable(result)
                    lightningTableService.insert(result, query)

                    BackfillService().backfill(result.backfillContext, result.triggers.values.toList())
                }
                "2" -> {
                    println("Listing LT Tables")
                }
                "3" -> {
                    println("Exiting.")
                    exit = true
                }
                else -> {
                    println("Invalid option. Please try again.")
                }
            }
        }
    }
}
