package com.coderjoe.lightningtables.cli

import com.coderjoe.lightningtables.cli.model.DatabaseConnection
import com.coderjoe.lightningtables.core.database.DatabaseConfig

fun main() {
    val validatedCredentials = credentialsLoop()

    if (!validatedCredentials) {
        println("A valid database connection is required for this application to run. Exiting.")
        return
    }
}

fun credentialsLoop(tryCount: Int = 0): Boolean {
    var tryCount = tryCount

    if (tryCount >= 3) {
        println("Too many failed attempts. Exiting.")
        return false
    }

    try {
        val credentials = DatabaseConnection(
            host = getInput("Host: "),
            username = getInput("Username: "),
            password = getInput("Password: "),
            database = getInput("Database: "),
            port = getInput("Port: ").toInt()
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
        return credentialsLoop(tryCount)
    }
}

fun getInput(title: String): String {
    println(title)
    return readln()
}