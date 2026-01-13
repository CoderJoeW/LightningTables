package com.coderjoe.database

import java.sql.Connection
import java.sql.DriverManager

object DatabaseConnection {
    private var connection: Connection? = null

    fun initialize(url: String, username: String, password: String) {
        connection = DriverManager.getConnection(url, username, password)
    }

    fun getConnection(): Connection? {
        return connection ?: throw IllegalStateException("Database connection is not initialized.")
    }
}

abstract class Table(val tableName: String) {
    fun insert(values: Map<String, Any?>) {
        val connection = DatabaseConnection.getConnection() ?: throw Exception("Database not initialized")

        val columns = values.keys.joinToString(", ")
        val placeholders = values.keys.joinToString(", ") { "?" }
        val sql = "INSERT INTO $tableName ($columns) VALUES ($placeholders)"

        connection.prepareStatement(sql).use { statement ->
            values.values.forEachIndexed { index, value ->
                statement.setObject(index + 1, value)
            }
            statement.executeUpdate()
        }
    }

    fun lock(lockType: LockType = LockType.WRITE) {
        val connection = DatabaseConnection.getConnection()
        connection!!.createStatement().execute("LOCK TABLES $tableName ${lockType.name}")
    }

    fun unlock() {
        val connection = DatabaseConnection.getConnection()
        connection!!.createStatement().execute("UNLOCK TABLES")
    }

    enum class LockType {
        READ, WRITE
    }
}