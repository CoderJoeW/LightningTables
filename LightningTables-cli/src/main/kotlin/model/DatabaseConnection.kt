package com.coderjoe.lightningtables.cli.model

data class DatabaseConnection(
    val host: String,
    val username: String,
    val password: String,
    val database: String,
    val port: Int,
)
