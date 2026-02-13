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
                    3. Delete lightning table
                    4. Exit
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
                    val entries = lightningTableService.list()

                    if (entries.isEmpty()) {
                        println("No lightning table found")
                        return
                    }

                    println("Lightning tables:")
                    entries.forEach {
                        println(
                            """
                            ID: ${it.id}
                            Lightning Table Name: ${it.tableName}
                            Base Table Name: ${it.baseTableName}
                            Query: ${it.query}
                            Insert Trigger Name: ${it.insertTriggerName}
                            Update Trigger Name: ${it.updateTriggerName}
                            Delete Trigger Name: ${it.deleteTriggerName}
                            """.trimIndent(),
                        )
                    }
                }
                "3" -> {
                    val entries = lightningTableService.list()

                    if (entries.isEmpty()) {
                        println("No lightning tables found")
                        return
                    }

                    val selected =
                        ConsoleInputHelper.getListInput(
                            "Select a lightning table to delete:",
                            entries,
                        ) { "${it.tableName} (base: ${it.baseTableName})" }

                    if (selected != null) {
                        val confirm =
                            ConsoleInputHelper.getInputWithLabel(
                                "Are you sure you want to delete '${selected.tableName}'? (y/n): ",
                            )
                        if (confirm.lowercase() == "y") {
                            val success = lightningTableService.delete(selected)
                            if (success) {
                                println("Lightning table '${selected.tableName}' deleted successfully.")
                            } else {
                                println("Failed to delete lightning table '${selected.tableName}'.")
                            }
                        } else {
                            println("Delete cancelled.")
                        }
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
