package com.coderjoe.lightningtables.cli

import com.coderjoe.lightningtables.cli.menus.DatabaseConnectMenu
import com.coderjoe.lightningtables.cli.menus.MainMenu
import com.coderjoe.lightningtables.cli.menus.utils.ConsoleInputHelper
import com.coderjoe.lightningtables.core.services.LightningTablesService

val lightningTableService = LightningTablesService()

fun main() {
    val validatedCredentials = DatabaseConnectMenu.load()

    if (!validatedCredentials) {
        println("A valid database connection is required for this application to run. Exiting.")
        return
    }

    validateApplicationTablesExist()

    MainMenu.load()
}

fun validateApplicationTablesExist() {
    println("Checking if application tables exist")

    if (!lightningTableService.ltTablesExists()) {
        println(
            """
            You can use this application without the application tables
            but you will not be able to delete or manage any LT tables created.
            You will only be able to create new LT tables.
            If you choose not to create the application tables,
            you can create them later by running this application again.
        """,
        )

        when (ConsoleInputHelper.getInputWithLabel("No lightning tables exist. Would you like to create them? (y/n)")) {
            "y" -> {
                println("Creating tables...")
                lightningTableService.createLtTables()
                if (!lightningTableService.ltTablesExists()) {
                    println("Failed to create tables. Exiting.")
                    return
                }
                println("Tables created successfully.")
            }
            else -> {
                println(
                    """
                    You will not be able to manage or delete any LT tables
                    created without the application tables.
                    You can create the application tables later by running this application again.
                """,
                )
            }
        }
    }
}
