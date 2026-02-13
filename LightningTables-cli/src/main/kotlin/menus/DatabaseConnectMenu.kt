package com.coderjoe.lightningtables.cli.menus

import com.coderjoe.lightningtables.cli.menus.utils.ConsoleInputHelper
import com.coderjoe.lightningtables.cli.model.DatabaseConnection
import com.coderjoe.lightningtables.core.database.DatabaseConfig

object DatabaseConnectMenu {
    fun load(): Boolean {
        var tryCount = 0

        println("Database Connection")
        println("-".repeat(30))

        while (tryCount < 3) {
            try {
                if (tryCount > 0) {
                    println()
                    println("Attempt ${tryCount + 1} of 3")
                }

                val credentials =
                    DatabaseConnection(
                        host = ConsoleInputHelper.getInputWithLabel("  Host: "),
                        username = ConsoleInputHelper.getInputWithLabel("  Username: "),
                        password = ConsoleInputHelper.getPasswordWithLabel("  Password: "),
                        database = ConsoleInputHelper.getInputWithLabel("  Database: "),
                        port = ConsoleInputHelper.getInputWithLabel("  Port: ").toInt(),
                    )

                println()
                println("Connecting...")

                DatabaseConfig.initialize(
                    url = "jdbc:mariadb://${credentials.host}:${credentials.port}/${credentials.database}",
                    username = credentials.username,
                    password = credentials.password,
                )

                println("Connected successfully.")
                println()
                return true
            } catch (e: Exception) {
                println()
                println("Error: ${e.message}")
                tryCount++
            }
        }

        println()
        println("Too many failed attempts. Exiting.")
        return false
    }
}
