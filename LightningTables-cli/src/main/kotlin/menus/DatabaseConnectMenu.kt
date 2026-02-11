package com.coderjoe.lightningtables.cli.menus

import com.coderjoe.lightningtables.cli.menus.utils.ConsoleInputHelper
import com.coderjoe.lightningtables.cli.model.DatabaseConnection
import com.coderjoe.lightningtables.core.database.DatabaseConfig

object DatabaseConnectMenu {
    fun load(): Boolean {
        var tryCount = 0

        while(tryCount < 3) {
            try {
                val credentials =
                    DatabaseConnection(
                        host = ConsoleInputHelper.getInputWithLabel("Host: "),
                        username = ConsoleInputHelper.getInputWithLabel("Username: "),
                        password = ConsoleInputHelper.getInputWithLabel("Password: "),
                        database = ConsoleInputHelper.getInputWithLabel("Database: "),
                        port = ConsoleInputHelper.getInputWithLabel("Port: ").toInt(),
                    )

                DatabaseConfig.initialize(
                    url = "jdbc:mariadb://${credentials.host}:${credentials.port}/${credentials.database}",
                    username = credentials.username,
                    password = credentials.password,
                )

                return true
            } catch (e: Exception) {
                println("Error: ${e.message}")
                tryCount++
            }
        }

        println("Too many failed attempts. Exiting.")
        return false
    }
}