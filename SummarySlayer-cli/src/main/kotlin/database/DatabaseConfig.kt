package com.coderjoe.database

import org.jetbrains.exposed.v1.jdbc.Database
import java.sql.Connection
import java.sql.DriverManager

object DatabaseConfig {
    private var url: String? = null
    private var username: String? = null
    private var password: String? = null

    fun initialize(
        url: String,
        username: String,
        password: String,
    ) {
        this.url = url
        this.username = username
        this.password = password
        Database.Companion.connect(url, driver = "org.mariadb.jdbc.Driver", user = username, password = password)
    }

    fun getConnection(): Connection {
        val u = url ?: throw IllegalStateException("Database connection is not initialized.")
        return DriverManager.getConnection(u, username, password)
    }
}