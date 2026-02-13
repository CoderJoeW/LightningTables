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
            println("-".repeat(50))
            val input =
                ConsoleInputHelper.getInputWithLabel(
                    """
                    |Menu:
                    |  1. Create new lightning table
                    |  2. List lightning tables
                    |  3. Delete lightning table
                    |  4. Exit
                    |
                    |Select an option: 
                    """.trimMargin(),
                )

            when (input) {
                "1" -> {
                    println()
                    val query = ConsoleInputHelper.getInputWithLabel("Enter query to create lightning table: ")
                    val tableName = ConsoleInputHelper.getInputWithLabel("Enter name for the lightning table: ")
                    println()
                    println("Creating lightning table '$tableName'...")
                    val result = sqlParser.generate(query, tableName)

                    lightningTableService.createLightningTable(result)
                    lightningTableService.insert(result, query)

                    BackfillService().backfill(result.backfillContext, result.triggers.values.toList())
                    println("Lightning table '$tableName' created successfully.")
                }
                "2" -> {
                    val entries = lightningTableService.list()
                    println()

                    if (entries.isEmpty()) {
                        println("No lightning tables found.")
                        continue
                    }

                    println("Lightning Tables (${entries.size}):")
                    entries.forEach {
                        println("-".repeat(40))
                        println("  ID:              ${it.id}")
                        println("  Table Name:      ${it.tableName}")
                        println("  Base Table:      ${it.baseTableName}")
                        println("  Query:           ${it.query}")
                        println("  Insert Trigger:  ${it.insertTriggerName}")
                        println("  Update Trigger:  ${it.updateTriggerName}")
                        println("  Delete Trigger:  ${it.deleteTriggerName}")
                    }
                    println("-".repeat(40))
                }
                "3" -> {
                    val entries = lightningTableService.list()
                    println()

                    if (entries.isEmpty()) {
                        println("No lightning tables found.")
                        continue
                    }

                    val selected =
                        ConsoleInputHelper.getListInput(
                            "Select a lightning table to delete:",
                            entries,
                        ) { "${it.tableName} (base: ${it.baseTableName})" }

                    if (selected == null) continue
                    println()

                    val confirm =
                        ConsoleInputHelper.getInputWithLabel(
                            "Are you sure you want to delete '${selected.tableName}'? (y/n): ",
                        )
                    if (confirm.lowercase() != "y") {
                        println("Delete cancelled.")
                        continue
                    }

                    println()
                    val success = lightningTableService.delete(selected)
                    if (success) {
                        println("Lightning table '${selected.tableName}' deleted successfully.")
                    } else {
                        println("Failed to delete lightning table '${selected.tableName}'.")
                    }
                }
                "4" -> {
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
