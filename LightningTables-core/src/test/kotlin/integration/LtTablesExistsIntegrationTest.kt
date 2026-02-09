package com.coderjoe.lightningtables.core.integration

import com.coderjoe.lightningtables.core.services.LightningTablesService
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class LtTablesExistsIntegrationTest : DockerComposeTestBase() {
    private val service = LightningTablesService()

    @Test
    fun `returns false when lt_tables does not exist`() {
        executeSQL("DROP TABLE IF EXISTS lt_tables")

        assertFalse(service.ltTablesExists())
    }

    @Test
    fun `returns true when lt_tables exists`() {
        executeSQL("CREATE TABLE IF NOT EXISTS lt_tables (id INT PRIMARY KEY)")

        assertTrue(service.ltTablesExists())

        executeSQL("DROP TABLE IF EXISTS lt_tables")
    }

    @Test
    fun `returns false after lt_tables is created then dropped`() {
        executeSQL("CREATE TABLE IF NOT EXISTS lt_tables (id INT PRIMARY KEY)")
        assertTrue(service.ltTablesExists())

        executeSQL("DROP TABLE IF EXISTS lt_tables")
        assertFalse(service.ltTablesExists())
    }
}
