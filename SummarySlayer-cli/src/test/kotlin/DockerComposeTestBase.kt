package com.coderjoe

import com.coderjoe.database.DatabaseConnection
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import java.io.File
import java.sql.DriverManager

/**
 * Base class for integration tests that automatically manages a docker-compose MariaDB container.
 *
 * This class automatically:
 * - Starts the docker-compose.test.yml container before all tests
 * - Waits for the database to be healthy and ready
 * - Stops and removes the container after all tests complete
 */
abstract class DockerComposeTestBase {

    companion object {
        private const val JDBC_URL = "jdbc:mariadb://localhost:3307/summaryslayer"
        private const val USERNAME = "testuser"
        private const val PASSWORD = "testpassword"
        private const val DOCKER_COMPOSE_FILE = "docker-compose.test.yml"
        private const val MAX_WAIT_SECONDS = 60

        @JvmStatic
        @BeforeAll
        fun setupDatabase() {
            println("Starting Docker Compose test environment...")

            startDockerCompose()
            waitForDatabase()

            // Initialize DatabaseConnection with docker-compose settings
            DatabaseConnection.initialize(
                url = JDBC_URL,
                username = USERNAME,
                password = PASSWORD
            )

            println("Docker Compose test environment ready!")
        }

        @BeforeEach
        fun cleanupBeforeTest() {
            cleanDatabase()
            recreateSchema()
            reseedDatabase()
        }

        @JvmStatic
        @AfterAll
        fun teardownDatabase() {
            println("Stopping Docker Compose test environment...")
            stopDockerCompose()
            println("Docker Compose test environment stopped!")
        }

        private fun startDockerCompose() {
            val projectRoot = File(System.getProperty("user.dir"))
            val composeFile = File(projectRoot, DOCKER_COMPOSE_FILE)

            if (!composeFile.exists()) {
                throw IllegalStateException("docker-compose.test.yml not found at: ${composeFile.absolutePath}")
            }

            val process = ProcessBuilder(
                "docker-compose",
                "-f", DOCKER_COMPOSE_FILE,
                "up",
                "-d"
            )
                .directory(projectRoot)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start()

            val exitCode = process.waitFor()
            if (exitCode != 0) {
                throw RuntimeException("Failed to start docker-compose. Exit code: $exitCode")
            }
        }

        private fun stopDockerCompose() {
            val projectRoot = File(System.getProperty("user.dir"))

            val process = ProcessBuilder(
                "docker-compose",
                "-f", DOCKER_COMPOSE_FILE,
                "down",
                "-v"  // Remove volumes to ensure clean state
            )
                .directory(projectRoot)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start()

            process.waitFor()
        }

        private fun waitForDatabase() {
            println("Waiting for database to be ready...")
            val startTime = System.currentTimeMillis()
            var lastException: Exception? = null

            while (System.currentTimeMillis() - startTime < MAX_WAIT_SECONDS * 1000) {
                try {
                    DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD).use { connection ->
                        connection.createStatement().use { statement ->
                            statement.executeQuery("SELECT 1")
                            println("Database is ready!")
                            return
                        }
                    }
                } catch (e: Exception) {
                    lastException = e
                    Thread.sleep(1000)
                }
            }

            throw RuntimeException(
                "Database did not become ready within $MAX_WAIT_SECONDS seconds",
                lastException
            )
        }

        fun getJdbcUrl(): String = JDBC_URL
        fun getUsername(): String = USERNAME
        fun getPassword(): String = PASSWORD

        fun executeSQL(sql: String) {
            DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(sql)
                }
            }
        }

        fun cleanDatabase() {
            DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD).use { connection ->
                connection.createStatement().use { statement ->
                    // Disable foreign key checks temporarily
                    statement.execute("SET FOREIGN_KEY_CHECKS = 0")
                    statement.execute("DROP TABLE transactions")
                    statement.execute("DROP TABLE users")
                    // Re-enable foreign key checks
                    statement.execute("SET FOREIGN_KEY_CHECKS = 1")
                }
            }
        }

        fun reseedDatabase() {
            val seedSQL = Thread.currentThread().contextClassLoader.getResource("seed.sql")?.readText()
                ?: throw IllegalStateException("seed.sql not found in test resources")

            DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("SET FOREIGN_KEY_CHECKS = 0")
                    // Split and execute each statement
                    seedSQL.split(";")
                        .map { it.trim() }
                        .filter { it.isNotEmpty() && !it.startsWith("--") }
                        .forEach { sql ->
                            if (sql.isNotBlank()) {
                                statement.execute(sql)
                            }
                        }
                    statement.execute("SET FOREIGN_KEY_CHECKS = 1")
                }
            }
        }

        fun recreateSchema() {
            val schemaSQL = Thread.currentThread().contextClassLoader.getResource("schema.sql")?.readText()
                ?: throw IllegalStateException("schema.sql not found in test resources")

            DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD).use { connection ->
                connection.createStatement().use { statement ->
                    statement.execute("SET FOREIGN_KEY_CHECKS = 0")
                    // Split and execute each statement
                    schemaSQL.split(";")
                        .map { it.trim() }
                        .filter { it.isNotEmpty() && !it.startsWith("--") }
                        .forEach { sql ->
                            if (sql.isNotBlank()) {
                                statement.execute(sql)
                            }
                        }
                    statement.execute("SET FOREIGN_KEY_CHECKS = 1")
                }
            }
        }
    }
}

