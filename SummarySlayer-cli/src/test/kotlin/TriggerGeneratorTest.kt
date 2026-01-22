package com.coderjoe

import com.coderjoe.services.TriggerGenerator
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach

class TriggerGeneratorTest {
    private lateinit var triggerGenerator: TriggerGenerator

    @BeforeEach
    fun setup() {
        triggerGenerator = TriggerGenerator()
    }

    @Test
    fun `buildUpsertStatement with single key and single insert column`() {
        val result = triggerGenerator.buildUpsertStatement(
            tableName = "summary_table",
            keyColumns = listOf("user_id"),
            keyExpressions = listOf("NEW.user_id"),
            insertColumns = listOf("total_amount"),
            insertValues = listOf("NEW.amount"),
            updateExpressions = listOf("total_amount = total_amount + NEW.amount")
        )

        val expected = """
            INSERT INTO summary_table (user_id, total_amount)
            VALUES (NEW.user_id, NEW.amount)
            ON DUPLICATE KEY UPDATE total_amount = total_amount + NEW.amount;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpsertStatement with multiple key columns`() {
        val result = triggerGenerator.buildUpsertStatement(
            tableName = "summary_table",
            keyColumns = listOf("user_id", "category_id"),
            keyExpressions = listOf("NEW.user_id", "NEW.category_id"),
            insertColumns = listOf("total_amount"),
            insertValues = listOf("NEW.amount"),
            updateExpressions = listOf("total_amount = total_amount + NEW.amount")
        )

        val expected = """
            INSERT INTO summary_table (user_id, category_id, total_amount)
            VALUES (NEW.user_id, NEW.category_id, NEW.amount)
            ON DUPLICATE KEY UPDATE total_amount = total_amount + NEW.amount;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpsertStatement with multiple insert columns`() {
        val result = triggerGenerator.buildUpsertStatement(
            tableName = "summary_table",
            keyColumns = listOf("user_id"),
            keyExpressions = listOf("NEW.user_id"),
            insertColumns = listOf("total_amount", "count", "last_updated"),
            insertValues = listOf("NEW.amount", "1", "NOW()"),
            updateExpressions = listOf(
                "total_amount = total_amount + NEW.amount",
                "count = count + 1",
                "last_updated = NOW()"
            )
        )

        val expected = """
            INSERT INTO summary_table (user_id, total_amount, count, last_updated)
            VALUES (NEW.user_id, NEW.amount, 1, NOW())
            ON DUPLICATE KEY UPDATE total_amount = total_amount + NEW.amount, count = count + 1, last_updated = NOW();
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpsertStatement with complex expressions`() {
        val result = triggerGenerator.buildUpsertStatement(
            tableName = "user_stats",
            keyColumns = listOf("user_id", "month", "year"),
            keyExpressions = listOf("NEW.user_id", "MONTH(NEW.created_at)", "YEAR(NEW.created_at)"),
            insertColumns = listOf("total_spent", "transaction_count", "avg_amount"),
            insertValues = listOf("NEW.amount", "1", "NEW.amount"),
            updateExpressions = listOf(
                "total_spent = total_spent + NEW.amount",
                "transaction_count = transaction_count + 1",
                "avg_amount = total_spent / transaction_count"
            )
        )

        val expected = """
            INSERT INTO user_stats (user_id, month, year, total_spent, transaction_count, avg_amount)
            VALUES (NEW.user_id, MONTH(NEW.created_at), YEAR(NEW.created_at), NEW.amount, 1, NEW.amount)
            ON DUPLICATE KEY UPDATE total_spent = total_spent + NEW.amount, transaction_count = transaction_count + 1, avg_amount = total_spent / transaction_count;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpsertStatement with OLD reference for delete operations`() {
        val result = triggerGenerator.buildUpsertStatement(
            tableName = "summary_table",
            keyColumns = listOf("user_id"),
            keyExpressions = listOf("OLD.user_id"),
            insertColumns = listOf("total_amount"),
            insertValues = listOf("-OLD.amount"),
            updateExpressions = listOf("total_amount = total_amount - OLD.amount")
        )

        val expected = """
            INSERT INTO summary_table (user_id, total_amount)
            VALUES (OLD.user_id, -OLD.amount)
            ON DUPLICATE KEY UPDATE total_amount = total_amount - OLD.amount;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpsertStatement with aggregate functions and calculations`() {
        val result = triggerGenerator.buildUpsertStatement(
            tableName = "product_summary",
            keyColumns = listOf("product_id"),
            keyExpressions = listOf("NEW.product_id"),
            insertColumns = listOf("total_revenue", "units_sold", "min_price", "max_price"),
            insertValues = listOf(
                "NEW.price * NEW.quantity",
                "NEW.quantity",
                "NEW.price",
                "NEW.price"
            ),
            updateExpressions = listOf(
                "total_revenue = total_revenue + (NEW.price * NEW.quantity)",
                "units_sold = units_sold + NEW.quantity",
                "min_price = LEAST(min_price, NEW.price)",
                "max_price = GREATEST(max_price, NEW.price)"
            )
        )

        val expected = """
            INSERT INTO product_summary (product_id, total_revenue, units_sold, min_price, max_price)
            VALUES (NEW.product_id, NEW.price * NEW.quantity, NEW.quantity, NEW.price, NEW.price)
            ON DUPLICATE KEY UPDATE total_revenue = total_revenue + (NEW.price * NEW.quantity), units_sold = units_sold + NEW.quantity, min_price = LEAST(min_price, NEW.price), max_price = GREATEST(max_price, NEW.price);
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpsertStatement with conditional expressions`() {
        val result = triggerGenerator.buildUpsertStatement(
            tableName = "conditional_summary",
            keyColumns = listOf("user_id"),
            keyExpressions = listOf("NEW.user_id"),
            insertColumns = listOf("high_value_count", "low_value_count"),
            insertValues = listOf(
                "IF(NEW.amount > 1000, 1, 0)",
                "IF(NEW.amount <= 1000, 1, 0)"
            ),
            updateExpressions = listOf(
                "high_value_count = high_value_count + IF(NEW.amount > 1000, 1, 0)",
                "low_value_count = low_value_count + IF(NEW.amount <= 1000, 1, 0)"
            )
        )

        val expected = """
            INSERT INTO conditional_summary (user_id, high_value_count, low_value_count)
            VALUES (NEW.user_id, IF(NEW.amount > 1000, 1, 0), IF(NEW.amount <= 1000, 1, 0))
            ON DUPLICATE KEY UPDATE high_value_count = high_value_count + IF(NEW.amount > 1000, 1, 0), low_value_count = low_value_count + IF(NEW.amount <= 1000, 1, 0);
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpsertStatement with empty lists should produce minimal statement`() {
        val result = triggerGenerator.buildUpsertStatement(
            tableName = "minimal_table",
            keyColumns = listOf("id"),
            keyExpressions = listOf("NEW.id"),
            insertColumns = emptyList(),
            insertValues = emptyList(),
            updateExpressions = emptyList()
        )

        val expected = """
            INSERT INTO minimal_table (id, )
            VALUES (NEW.id, )
            ON DUPLICATE KEY UPDATE ;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpsertStatement with string literals and special characters`() {
        val result = triggerGenerator.buildUpsertStatement(
            tableName = "special_table",
            keyColumns = listOf("user_id"),
            keyExpressions = listOf("NEW.user_id"),
            insertColumns = listOf("status", "description"),
            insertValues = listOf("'active'", "CONCAT('User ', NEW.name, ' registered')"),
            updateExpressions = listOf(
                "status = 'updated'",
                "description = CONCAT('User ', NEW.name, ' updated')"
            )
        )

        val expected = """
            INSERT INTO special_table (user_id, status, description)
            VALUES (NEW.user_id, 'active', CONCAT('User ', NEW.name, ' registered'))
            ON DUPLICATE KEY UPDATE status = 'updated', description = CONCAT('User ', NEW.name, ' updated');
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpsertStatement with COALESCE and NULL handling`() {
        val result = triggerGenerator.buildUpsertStatement(
            tableName = "null_safe_table",
            keyColumns = listOf("user_id"),
            keyExpressions = listOf("NEW.user_id"),
            insertColumns = listOf("total", "nullable_field"),
            insertValues = listOf("COALESCE(NEW.amount, 0)", "NEW.optional_field"),
            updateExpressions = listOf(
                "total = total + COALESCE(NEW.amount, 0)",
                "nullable_field = COALESCE(NEW.optional_field, nullable_field)"
            )
        )

        val expected = """
            INSERT INTO null_safe_table (user_id, total, nullable_field)
            VALUES (NEW.user_id, COALESCE(NEW.amount, 0), NEW.optional_field)
            ON DUPLICATE KEY UPDATE total = total + COALESCE(NEW.amount, 0), nullable_field = COALESCE(NEW.optional_field, nullable_field);
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpsertStatement with quoted table and column names`() {
        val result = triggerGenerator.buildUpsertStatement(
            tableName = "`user-summary`",
            keyColumns = listOf("`user-id`", "`date-key`"),
            keyExpressions = listOf("NEW.`user-id`", "NEW.`date-key`"),
            insertColumns = listOf("`total-amount`"),
            insertValues = listOf("NEW.`amount`"),
            updateExpressions = listOf("`total-amount` = `total-amount` + NEW.`amount`")
        )

        val expected = """
            INSERT INTO `user-summary` (`user-id`, `date-key`, `total-amount`)
            VALUES (NEW.`user-id`, NEW.`date-key`, NEW.`amount`)
            ON DUPLICATE KEY UPDATE `total-amount` = `total-amount` + NEW.`amount`;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildInsertTrigger with simple predicate and upsert`() {
        val upsertStatement = """
INSERT INTO summary_table (user_id, total_amount)
VALUES (NEW.user_id, NEW.amount)
ON DUPLICATE KEY UPDATE total_amount = total_amount + NEW.amount;
        """.trimIndent()

        val result = triggerGenerator.buildInsertTrigger(
            tableName = "summary_table",
            baseTableName = "transactions",
            predicate = "NEW.amount > 0",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `summary_table_after_insert_summary`
            AFTER INSERT ON `transactions`
            FOR EACH ROW
            BEGIN
                IF NEW.amount > 0 THEN
                    INSERT INTO summary_table (user_id, total_amount)
VALUES (NEW.user_id, NEW.amount)
ON DUPLICATE KEY UPDATE total_amount = total_amount + NEW.amount;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildInsertTrigger with complex predicate`() {
        val upsertStatement = """
INSERT INTO user_stats (user_id, high_value_count)
VALUES (NEW.user_id, 1)
ON DUPLICATE KEY UPDATE high_value_count = high_value_count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildInsertTrigger(
            tableName = "user_stats",
            baseTableName = "orders",
            predicate = "NEW.amount > 1000 AND NEW.status = 'completed'",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `user_stats_after_insert_summary`
            AFTER INSERT ON `orders`
            FOR EACH ROW
            BEGIN
                IF NEW.amount > 1000 AND NEW.status = 'completed' THEN
                    INSERT INTO user_stats (user_id, high_value_count)
VALUES (NEW.user_id, 1)
ON DUPLICATE KEY UPDATE high_value_count = high_value_count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildInsertTrigger with predicate using IN clause`() {
        val upsertStatement = """
INSERT INTO category_summary (category_id, item_count)
VALUES (NEW.category_id, 1)
ON DUPLICATE KEY UPDATE item_count = item_count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildInsertTrigger(
            tableName = "category_summary",
            baseTableName = "products",
            predicate = "NEW.category_id IN (1, 2, 3, 5)",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `category_summary_after_insert_summary`
            AFTER INSERT ON `products`
            FOR EACH ROW
            BEGIN
                IF NEW.category_id IN (1, 2, 3, 5) THEN
                    INSERT INTO category_summary (category_id, item_count)
VALUES (NEW.category_id, 1)
ON DUPLICATE KEY UPDATE item_count = item_count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildInsertTrigger with predicate using NOT NULL check`() {
        val upsertStatement = """
INSERT INTO valid_entries (entry_id, count)
VALUES (NEW.id, 1)
ON DUPLICATE KEY UPDATE count = count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildInsertTrigger(
            tableName = "valid_entries",
            baseTableName = "raw_data",
            predicate = "NEW.important_field IS NOT NULL",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `valid_entries_after_insert_summary`
            AFTER INSERT ON `raw_data`
            FOR EACH ROW
            BEGIN
                IF NEW.important_field IS NOT NULL THEN
                    INSERT INTO valid_entries (entry_id, count)
VALUES (NEW.id, 1)
ON DUPLICATE KEY UPDATE count = count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildInsertTrigger with always true predicate`() {
        val upsertStatement = """
INSERT INTO all_records (record_id, timestamp)
VALUES (NEW.id, NOW())
ON DUPLICATE KEY UPDATE timestamp = NOW();
        """.trimIndent()

        val result = triggerGenerator.buildInsertTrigger(
            tableName = "all_records",
            baseTableName = "events",
            predicate = "TRUE",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `all_records_after_insert_summary`
            AFTER INSERT ON `events`
            FOR EACH ROW
            BEGIN
                IF TRUE THEN
                    INSERT INTO all_records (record_id, timestamp)
VALUES (NEW.id, NOW())
ON DUPLICATE KEY UPDATE timestamp = NOW();
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildInsertTrigger with multi-line upsert statement`() {
        val upsertStatement = """
INSERT INTO complex_summary (
    user_id,
    month,
    year,
    total_spent,
    transaction_count
)
VALUES (
    NEW.user_id,
    MONTH(NEW.created_at),
    YEAR(NEW.created_at),
    NEW.amount,
    1
)
ON DUPLICATE KEY UPDATE
    total_spent = total_spent + NEW.amount,
    transaction_count = transaction_count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildInsertTrigger(
            tableName = "complex_summary",
            baseTableName = "transactions",
            predicate = "NEW.user_id IS NOT NULL AND NEW.amount > 0",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `complex_summary_after_insert_summary`
            AFTER INSERT ON `transactions`
            FOR EACH ROW
            BEGIN
                IF NEW.user_id IS NOT NULL AND NEW.amount > 0 THEN
                    INSERT INTO complex_summary (
    user_id,
    month,
    year,
    total_spent,
    transaction_count
)
VALUES (
    NEW.user_id,
    MONTH(NEW.created_at),
    YEAR(NEW.created_at),
    NEW.amount,
    1
)
ON DUPLICATE KEY UPDATE
    total_spent = total_spent + NEW.amount,
    transaction_count = transaction_count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildInsertTrigger with special characters in table names`() {
        val upsertStatement = """
INSERT INTO `user-summary` (`user-id`, `total`)
VALUES (NEW.`user-id`, NEW.amount)
ON DUPLICATE KEY UPDATE `total` = `total` + NEW.amount;
        """.trimIndent()

        val result = triggerGenerator.buildInsertTrigger(
            tableName = "user-summary",
            baseTableName = "user-transactions",
            predicate = "NEW.`user-id` > 0",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `user-summary_after_insert_summary`
            AFTER INSERT ON `user-transactions`
            FOR EACH ROW
            BEGIN
                IF NEW.`user-id` > 0 THEN
                    INSERT INTO `user-summary` (`user-id`, `total`)
VALUES (NEW.`user-id`, NEW.amount)
ON DUPLICATE KEY UPDATE `total` = `total` + NEW.amount;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildInsertTrigger with predicate using function calls`() {
        val upsertStatement = """
INSERT INTO date_summary (date_key, record_count)
VALUES (DATE(NEW.created_at), 1)
ON DUPLICATE KEY UPDATE record_count = record_count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildInsertTrigger(
            tableName = "date_summary",
            baseTableName = "events",
            predicate = "YEAR(NEW.created_at) >= 2024",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `date_summary_after_insert_summary`
            AFTER INSERT ON `events`
            FOR EACH ROW
            BEGIN
                IF YEAR(NEW.created_at) >= 2024 THEN
                    INSERT INTO date_summary (date_key, record_count)
VALUES (DATE(NEW.created_at), 1)
ON DUPLICATE KEY UPDATE record_count = record_count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpdateTrigger with simple predicates and upserts`() {
        val oldUpsertStatement = """
INSERT INTO summary_table (user_id, total_amount)
VALUES (OLD.user_id, -OLD.amount)
ON DUPLICATE KEY UPDATE total_amount = total_amount - OLD.amount;
        """.trimIndent()

        val newUpsertStatement = """
INSERT INTO summary_table (user_id, total_amount)
VALUES (NEW.user_id, NEW.amount)
ON DUPLICATE KEY UPDATE total_amount = total_amount + NEW.amount;
        """.trimIndent()

        val result = triggerGenerator.buildUpdateTrigger(
            tableName = "summary_table",
            baseTableName = "transactions",
            oldPredicate = "OLD.amount > 0",
            oldUpsertStatement = oldUpsertStatement,
            newPredicate = "NEW.amount > 0",
            newUpsertStatement = newUpsertStatement
        )

        val expected = """
            CREATE TRIGGER `summary_table_after_update_summary`
            AFTER UPDATE ON `transactions`
            FOR EACH ROW
            BEGIN
                IF OLD.amount > 0 THEN
                    INSERT INTO summary_table (user_id, total_amount)
VALUES (OLD.user_id, -OLD.amount)
ON DUPLICATE KEY UPDATE total_amount = total_amount - OLD.amount;
                END IF;

                IF NEW.amount > 0 THEN
                    INSERT INTO summary_table (user_id, total_amount)
VALUES (NEW.user_id, NEW.amount)
ON DUPLICATE KEY UPDATE total_amount = total_amount + NEW.amount;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpdateTrigger with complex predicates`() {
        val oldUpsertStatement = """
INSERT INTO user_stats (user_id, high_value_count)
VALUES (OLD.user_id, -1)
ON DUPLICATE KEY UPDATE high_value_count = high_value_count - 1;
        """.trimIndent()

        val newUpsertStatement = """
INSERT INTO user_stats (user_id, high_value_count)
VALUES (NEW.user_id, 1)
ON DUPLICATE KEY UPDATE high_value_count = high_value_count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildUpdateTrigger(
            tableName = "user_stats",
            baseTableName = "orders",
            oldPredicate = "OLD.amount > 1000 AND OLD.status = 'completed'",
            oldUpsertStatement = oldUpsertStatement,
            newPredicate = "NEW.amount > 1000 AND NEW.status = 'completed'",
            newUpsertStatement = newUpsertStatement
        )

        val expected = """
            CREATE TRIGGER `user_stats_after_update_summary`
            AFTER UPDATE ON `orders`
            FOR EACH ROW
            BEGIN
                IF OLD.amount > 1000 AND OLD.status = 'completed' THEN
                    INSERT INTO user_stats (user_id, high_value_count)
VALUES (OLD.user_id, -1)
ON DUPLICATE KEY UPDATE high_value_count = high_value_count - 1;
                END IF;

                IF NEW.amount > 1000 AND NEW.status = 'completed' THEN
                    INSERT INTO user_stats (user_id, high_value_count)
VALUES (NEW.user_id, 1)
ON DUPLICATE KEY UPDATE high_value_count = high_value_count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpdateTrigger with different predicates for old and new`() {
        val oldUpsertStatement = """
INSERT INTO category_summary (category_id, item_count)
VALUES (OLD.category_id, -1)
ON DUPLICATE KEY UPDATE item_count = item_count - 1;
        """.trimIndent()

        val newUpsertStatement = """
INSERT INTO category_summary (category_id, item_count)
VALUES (NEW.category_id, 1)
ON DUPLICATE KEY UPDATE item_count = item_count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildUpdateTrigger(
            tableName = "category_summary",
            baseTableName = "products",
            oldPredicate = "OLD.category_id IN (1, 2, 3)",
            oldUpsertStatement = oldUpsertStatement,
            newPredicate = "NEW.category_id IN (4, 5, 6)",
            newUpsertStatement = newUpsertStatement
        )

        val expected = """
            CREATE TRIGGER `category_summary_after_update_summary`
            AFTER UPDATE ON `products`
            FOR EACH ROW
            BEGIN
                IF OLD.category_id IN (1, 2, 3) THEN
                    INSERT INTO category_summary (category_id, item_count)
VALUES (OLD.category_id, -1)
ON DUPLICATE KEY UPDATE item_count = item_count - 1;
                END IF;

                IF NEW.category_id IN (4, 5, 6) THEN
                    INSERT INTO category_summary (category_id, item_count)
VALUES (NEW.category_id, 1)
ON DUPLICATE KEY UPDATE item_count = item_count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpdateTrigger with NULL checks`() {
        val oldUpsertStatement = """
INSERT INTO valid_entries (entry_id, count)
VALUES (OLD.id, -1)
ON DUPLICATE KEY UPDATE count = count - 1;
        """.trimIndent()

        val newUpsertStatement = """
INSERT INTO valid_entries (entry_id, count)
VALUES (NEW.id, 1)
ON DUPLICATE KEY UPDATE count = count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildUpdateTrigger(
            tableName = "valid_entries",
            baseTableName = "raw_data",
            oldPredicate = "OLD.important_field IS NOT NULL",
            oldUpsertStatement = oldUpsertStatement,
            newPredicate = "NEW.important_field IS NOT NULL",
            newUpsertStatement = newUpsertStatement
        )

        val expected = """
            CREATE TRIGGER `valid_entries_after_update_summary`
            AFTER UPDATE ON `raw_data`
            FOR EACH ROW
            BEGIN
                IF OLD.important_field IS NOT NULL THEN
                    INSERT INTO valid_entries (entry_id, count)
VALUES (OLD.id, -1)
ON DUPLICATE KEY UPDATE count = count - 1;
                END IF;

                IF NEW.important_field IS NOT NULL THEN
                    INSERT INTO valid_entries (entry_id, count)
VALUES (NEW.id, 1)
ON DUPLICATE KEY UPDATE count = count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpdateTrigger with always true predicates`() {
        val oldUpsertStatement = """
INSERT INTO audit_log (record_id, action, timestamp)
VALUES (OLD.id, 'old_value', NOW())
ON DUPLICATE KEY UPDATE timestamp = NOW();
        """.trimIndent()

        val newUpsertStatement = """
INSERT INTO audit_log (record_id, action, timestamp)
VALUES (NEW.id, 'new_value', NOW())
ON DUPLICATE KEY UPDATE timestamp = NOW();
        """.trimIndent()

        val result = triggerGenerator.buildUpdateTrigger(
            tableName = "audit_log",
            baseTableName = "events",
            oldPredicate = "TRUE",
            oldUpsertStatement = oldUpsertStatement,
            newPredicate = "TRUE",
            newUpsertStatement = newUpsertStatement
        )

        val expected = """
            CREATE TRIGGER `audit_log_after_update_summary`
            AFTER UPDATE ON `events`
            FOR EACH ROW
            BEGIN
                IF TRUE THEN
                    INSERT INTO audit_log (record_id, action, timestamp)
VALUES (OLD.id, 'old_value', NOW())
ON DUPLICATE KEY UPDATE timestamp = NOW();
                END IF;

                IF TRUE THEN
                    INSERT INTO audit_log (record_id, action, timestamp)
VALUES (NEW.id, 'new_value', NOW())
ON DUPLICATE KEY UPDATE timestamp = NOW();
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpdateTrigger with multi-line upsert statements`() {
        val oldUpsertStatement = """
INSERT INTO complex_summary (
    user_id,
    month,
    year,
    total_spent,
    transaction_count
)
VALUES (
    OLD.user_id,
    MONTH(OLD.created_at),
    YEAR(OLD.created_at),
    -OLD.amount,
    -1
)
ON DUPLICATE KEY UPDATE
    total_spent = total_spent - OLD.amount,
    transaction_count = transaction_count - 1;
        """.trimIndent()

        val newUpsertStatement = """
INSERT INTO complex_summary (
    user_id,
    month,
    year,
    total_spent,
    transaction_count
)
VALUES (
    NEW.user_id,
    MONTH(NEW.created_at),
    YEAR(NEW.created_at),
    NEW.amount,
    1
)
ON DUPLICATE KEY UPDATE
    total_spent = total_spent + NEW.amount,
    transaction_count = transaction_count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildUpdateTrigger(
            tableName = "complex_summary",
            baseTableName = "transactions",
            oldPredicate = "OLD.user_id IS NOT NULL AND OLD.amount > 0",
            oldUpsertStatement = oldUpsertStatement,
            newPredicate = "NEW.user_id IS NOT NULL AND NEW.amount > 0",
            newUpsertStatement = newUpsertStatement
        )

        val expected = """
            CREATE TRIGGER `complex_summary_after_update_summary`
            AFTER UPDATE ON `transactions`
            FOR EACH ROW
            BEGIN
                IF OLD.user_id IS NOT NULL AND OLD.amount > 0 THEN
                    INSERT INTO complex_summary (
    user_id,
    month,
    year,
    total_spent,
    transaction_count
)
VALUES (
    OLD.user_id,
    MONTH(OLD.created_at),
    YEAR(OLD.created_at),
    -OLD.amount,
    -1
)
ON DUPLICATE KEY UPDATE
    total_spent = total_spent - OLD.amount,
    transaction_count = transaction_count - 1;
                END IF;

                IF NEW.user_id IS NOT NULL AND NEW.amount > 0 THEN
                    INSERT INTO complex_summary (
    user_id,
    month,
    year,
    total_spent,
    transaction_count
)
VALUES (
    NEW.user_id,
    MONTH(NEW.created_at),
    YEAR(NEW.created_at),
    NEW.amount,
    1
)
ON DUPLICATE KEY UPDATE
    total_spent = total_spent + NEW.amount,
    transaction_count = transaction_count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpdateTrigger with special characters in table names`() {
        val oldUpsertStatement = """
INSERT INTO `user-summary` (`user-id`, `total`)
VALUES (OLD.`user-id`, -OLD.amount)
ON DUPLICATE KEY UPDATE `total` = `total` - OLD.amount;
        """.trimIndent()

        val newUpsertStatement = """
INSERT INTO `user-summary` (`user-id`, `total`)
VALUES (NEW.`user-id`, NEW.amount)
ON DUPLICATE KEY UPDATE `total` = `total` + NEW.amount;
        """.trimIndent()

        val result = triggerGenerator.buildUpdateTrigger(
            tableName = "user-summary",
            baseTableName = "user-transactions",
            oldPredicate = "OLD.`user-id` > 0",
            oldUpsertStatement = oldUpsertStatement,
            newPredicate = "NEW.`user-id` > 0",
            newUpsertStatement = newUpsertStatement
        )

        val expected = """
            CREATE TRIGGER `user-summary_after_update_summary`
            AFTER UPDATE ON `user-transactions`
            FOR EACH ROW
            BEGIN
                IF OLD.`user-id` > 0 THEN
                    INSERT INTO `user-summary` (`user-id`, `total`)
VALUES (OLD.`user-id`, -OLD.amount)
ON DUPLICATE KEY UPDATE `total` = `total` - OLD.amount;
                END IF;

                IF NEW.`user-id` > 0 THEN
                    INSERT INTO `user-summary` (`user-id`, `total`)
VALUES (NEW.`user-id`, NEW.amount)
ON DUPLICATE KEY UPDATE `total` = `total` + NEW.amount;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpdateTrigger with predicates using function calls`() {
        val oldUpsertStatement = """
INSERT INTO date_summary (date_key, record_count)
VALUES (DATE(OLD.created_at), -1)
ON DUPLICATE KEY UPDATE record_count = record_count - 1;
        """.trimIndent()

        val newUpsertStatement = """
INSERT INTO date_summary (date_key, record_count)
VALUES (DATE(NEW.created_at), 1)
ON DUPLICATE KEY UPDATE record_count = record_count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildUpdateTrigger(
            tableName = "date_summary",
            baseTableName = "events",
            oldPredicate = "YEAR(OLD.created_at) >= 2024",
            oldUpsertStatement = oldUpsertStatement,
            newPredicate = "YEAR(NEW.created_at) >= 2024",
            newUpsertStatement = newUpsertStatement
        )

        val expected = """
            CREATE TRIGGER `date_summary_after_update_summary`
            AFTER UPDATE ON `events`
            FOR EACH ROW
            BEGIN
                IF YEAR(OLD.created_at) >= 2024 THEN
                    INSERT INTO date_summary (date_key, record_count)
VALUES (DATE(OLD.created_at), -1)
ON DUPLICATE KEY UPDATE record_count = record_count - 1;
                END IF;

                IF YEAR(NEW.created_at) >= 2024 THEN
                    INSERT INTO date_summary (date_key, record_count)
VALUES (DATE(NEW.created_at), 1)
ON DUPLICATE KEY UPDATE record_count = record_count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpdateTrigger handling category changes`() {
        val oldUpsertStatement = """
INSERT INTO category_stats (category_id, product_count)
VALUES (OLD.category_id, -1)
ON DUPLICATE KEY UPDATE product_count = product_count - 1;
        """.trimIndent()

        val newUpsertStatement = """
INSERT INTO category_stats (category_id, product_count)
VALUES (NEW.category_id, 1)
ON DUPLICATE KEY UPDATE product_count = product_count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildUpdateTrigger(
            tableName = "category_stats",
            baseTableName = "products",
            oldPredicate = "OLD.category_id IS NOT NULL",
            oldUpsertStatement = oldUpsertStatement,
            newPredicate = "NEW.category_id IS NOT NULL",
            newUpsertStatement = newUpsertStatement
        )

        val expected = """
            CREATE TRIGGER `category_stats_after_update_summary`
            AFTER UPDATE ON `products`
            FOR EACH ROW
            BEGIN
                IF OLD.category_id IS NOT NULL THEN
                    INSERT INTO category_stats (category_id, product_count)
VALUES (OLD.category_id, -1)
ON DUPLICATE KEY UPDATE product_count = product_count - 1;
                END IF;

                IF NEW.category_id IS NOT NULL THEN
                    INSERT INTO category_stats (category_id, product_count)
VALUES (NEW.category_id, 1)
ON DUPLICATE KEY UPDATE product_count = product_count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpdateTrigger with status transitions`() {
        val oldUpsertStatement = """
INSERT INTO status_summary (status, count)
VALUES (OLD.status, -1)
ON DUPLICATE KEY UPDATE count = count - 1;
        """.trimIndent()

        val newUpsertStatement = """
INSERT INTO status_summary (status, count)
VALUES (NEW.status, 1)
ON DUPLICATE KEY UPDATE count = count + 1;
        """.trimIndent()

        val result = triggerGenerator.buildUpdateTrigger(
            tableName = "status_summary",
            baseTableName = "orders",
            oldPredicate = "OLD.status IN ('pending', 'processing', 'completed')",
            oldUpsertStatement = oldUpsertStatement,
            newPredicate = "NEW.status IN ('pending', 'processing', 'completed')",
            newUpsertStatement = newUpsertStatement
        )

        val expected = """
            CREATE TRIGGER `status_summary_after_update_summary`
            AFTER UPDATE ON `orders`
            FOR EACH ROW
            BEGIN
                IF OLD.status IN ('pending', 'processing', 'completed') THEN
                    INSERT INTO status_summary (status, count)
VALUES (OLD.status, -1)
ON DUPLICATE KEY UPDATE count = count - 1;
                END IF;

                IF NEW.status IN ('pending', 'processing', 'completed') THEN
                    INSERT INTO status_summary (status, count)
VALUES (NEW.status, 1)
ON DUPLICATE KEY UPDATE count = count + 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildUpdateTrigger with aggregate recalculations`() {
        val oldUpsertStatement = """
INSERT INTO product_stats (product_id, total_revenue, units_sold)
VALUES (OLD.product_id, -(OLD.price * OLD.quantity), -OLD.quantity)
ON DUPLICATE KEY UPDATE
    total_revenue = total_revenue - (OLD.price * OLD.quantity),
    units_sold = units_sold - OLD.quantity;
        """.trimIndent()

        val newUpsertStatement = """
INSERT INTO product_stats (product_id, total_revenue, units_sold)
VALUES (NEW.product_id, NEW.price * NEW.quantity, NEW.quantity)
ON DUPLICATE KEY UPDATE
    total_revenue = total_revenue + (NEW.price * NEW.quantity),
    units_sold = units_sold + NEW.quantity;
        """.trimIndent()

        val result = triggerGenerator.buildUpdateTrigger(
            tableName = "product_stats",
            baseTableName = "sales",
            oldPredicate = "OLD.product_id IS NOT NULL AND OLD.quantity > 0",
            oldUpsertStatement = oldUpsertStatement,
            newPredicate = "NEW.product_id IS NOT NULL AND NEW.quantity > 0",
            newUpsertStatement = newUpsertStatement
        )

        val expected = """
            CREATE TRIGGER `product_stats_after_update_summary`
            AFTER UPDATE ON `sales`
            FOR EACH ROW
            BEGIN
                IF OLD.product_id IS NOT NULL AND OLD.quantity > 0 THEN
                    INSERT INTO product_stats (product_id, total_revenue, units_sold)
VALUES (OLD.product_id, -(OLD.price * OLD.quantity), -OLD.quantity)
ON DUPLICATE KEY UPDATE
    total_revenue = total_revenue - (OLD.price * OLD.quantity),
    units_sold = units_sold - OLD.quantity;
                END IF;

                IF NEW.product_id IS NOT NULL AND NEW.quantity > 0 THEN
                    INSERT INTO product_stats (product_id, total_revenue, units_sold)
VALUES (NEW.product_id, NEW.price * NEW.quantity, NEW.quantity)
ON DUPLICATE KEY UPDATE
    total_revenue = total_revenue + (NEW.price * NEW.quantity),
    units_sold = units_sold + NEW.quantity;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildDeleteTrigger with simple predicate and upsert`() {
        val upsertStatement = """
INSERT INTO summary_table (user_id, total_amount)
VALUES (OLD.user_id, -OLD.amount)
ON DUPLICATE KEY UPDATE total_amount = total_amount - OLD.amount;
        """.trimIndent()

        val result = triggerGenerator.buildDeleteTrigger(
            tableName = "summary_table",
            baseTableName = "transactions",
            predicate = "OLD.amount > 0",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `summary_table_after_delete_summary`
            AFTER DELETE ON `transactions`
            FOR EACH ROW
            BEGIN
                IF OLD.amount > 0 THEN
                    INSERT INTO summary_table (user_id, total_amount)
VALUES (OLD.user_id, -OLD.amount)
ON DUPLICATE KEY UPDATE total_amount = total_amount - OLD.amount;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildDeleteTrigger with complex predicate`() {
        val upsertStatement = """
INSERT INTO user_stats (user_id, high_value_count)
VALUES (OLD.user_id, -1)
ON DUPLICATE KEY UPDATE high_value_count = high_value_count - 1;
        """.trimIndent()

        val result = triggerGenerator.buildDeleteTrigger(
            tableName = "user_stats",
            baseTableName = "orders",
            predicate = "OLD.amount > 1000 AND OLD.status = 'completed'",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `user_stats_after_delete_summary`
            AFTER DELETE ON `orders`
            FOR EACH ROW
            BEGIN
                IF OLD.amount > 1000 AND OLD.status = 'completed' THEN
                    INSERT INTO user_stats (user_id, high_value_count)
VALUES (OLD.user_id, -1)
ON DUPLICATE KEY UPDATE high_value_count = high_value_count - 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildDeleteTrigger with predicate using IN clause`() {
        val upsertStatement = """
INSERT INTO category_summary (category_id, item_count)
VALUES (OLD.category_id, -1)
ON DUPLICATE KEY UPDATE item_count = item_count - 1;
        """.trimIndent()

        val result = triggerGenerator.buildDeleteTrigger(
            tableName = "category_summary",
            baseTableName = "products",
            predicate = "OLD.category_id IN (1, 2, 3, 5)",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `category_summary_after_delete_summary`
            AFTER DELETE ON `products`
            FOR EACH ROW
            BEGIN
                IF OLD.category_id IN (1, 2, 3, 5) THEN
                    INSERT INTO category_summary (category_id, item_count)
VALUES (OLD.category_id, -1)
ON DUPLICATE KEY UPDATE item_count = item_count - 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildDeleteTrigger with predicate using NOT NULL check`() {
        val upsertStatement = """
INSERT INTO valid_entries (entry_id, count)
VALUES (OLD.id, -1)
ON DUPLICATE KEY UPDATE count = count - 1;
        """.trimIndent()

        val result = triggerGenerator.buildDeleteTrigger(
            tableName = "valid_entries",
            baseTableName = "raw_data",
            predicate = "OLD.important_field IS NOT NULL",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `valid_entries_after_delete_summary`
            AFTER DELETE ON `raw_data`
            FOR EACH ROW
            BEGIN
                IF OLD.important_field IS NOT NULL THEN
                    INSERT INTO valid_entries (entry_id, count)
VALUES (OLD.id, -1)
ON DUPLICATE KEY UPDATE count = count - 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildDeleteTrigger with always true predicate`() {
        val upsertStatement = """
INSERT INTO deleted_records (record_id, deleted_at)
VALUES (OLD.id, NOW())
ON DUPLICATE KEY UPDATE deleted_at = NOW();
        """.trimIndent()

        val result = triggerGenerator.buildDeleteTrigger(
            tableName = "deleted_records",
            baseTableName = "events",
            predicate = "TRUE",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `deleted_records_after_delete_summary`
            AFTER DELETE ON `events`
            FOR EACH ROW
            BEGIN
                IF TRUE THEN
                    INSERT INTO deleted_records (record_id, deleted_at)
VALUES (OLD.id, NOW())
ON DUPLICATE KEY UPDATE deleted_at = NOW();
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildDeleteTrigger with multi-line upsert statement`() {
        val upsertStatement = """
INSERT INTO complex_summary (
    user_id,
    month,
    year,
    total_spent,
    transaction_count
)
VALUES (
    OLD.user_id,
    MONTH(OLD.created_at),
    YEAR(OLD.created_at),
    -OLD.amount,
    -1
)
ON DUPLICATE KEY UPDATE
    total_spent = total_spent - OLD.amount,
    transaction_count = transaction_count - 1;
        """.trimIndent()

        val result = triggerGenerator.buildDeleteTrigger(
            tableName = "complex_summary",
            baseTableName = "transactions",
            predicate = "OLD.user_id IS NOT NULL AND OLD.amount > 0",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `complex_summary_after_delete_summary`
            AFTER DELETE ON `transactions`
            FOR EACH ROW
            BEGIN
                IF OLD.user_id IS NOT NULL AND OLD.amount > 0 THEN
                    INSERT INTO complex_summary (
    user_id,
    month,
    year,
    total_spent,
    transaction_count
)
VALUES (
    OLD.user_id,
    MONTH(OLD.created_at),
    YEAR(OLD.created_at),
    -OLD.amount,
    -1
)
ON DUPLICATE KEY UPDATE
    total_spent = total_spent - OLD.amount,
    transaction_count = transaction_count - 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildDeleteTrigger with special characters in table names`() {
        val upsertStatement = """
INSERT INTO `user-summary` (`user-id`, `total`)
VALUES (OLD.`user-id`, -OLD.amount)
ON DUPLICATE KEY UPDATE `total` = `total` - OLD.amount;
        """.trimIndent()

        val result = triggerGenerator.buildDeleteTrigger(
            tableName = "user-summary",
            baseTableName = "user-transactions",
            predicate = "OLD.`user-id` > 0",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `user-summary_after_delete_summary`
            AFTER DELETE ON `user-transactions`
            FOR EACH ROW
            BEGIN
                IF OLD.`user-id` > 0 THEN
                    INSERT INTO `user-summary` (`user-id`, `total`)
VALUES (OLD.`user-id`, -OLD.amount)
ON DUPLICATE KEY UPDATE `total` = `total` - OLD.amount;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildDeleteTrigger with predicate using function calls`() {
        val upsertStatement = """
INSERT INTO date_summary (date_key, record_count)
VALUES (DATE(OLD.created_at), -1)
ON DUPLICATE KEY UPDATE record_count = record_count - 1;
        """.trimIndent()

        val result = triggerGenerator.buildDeleteTrigger(
            tableName = "date_summary",
            baseTableName = "events",
            predicate = "YEAR(OLD.created_at) >= 2024",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `date_summary_after_delete_summary`
            AFTER DELETE ON `events`
            FOR EACH ROW
            BEGIN
                IF YEAR(OLD.created_at) >= 2024 THEN
                    INSERT INTO date_summary (date_key, record_count)
VALUES (DATE(OLD.created_at), -1)
ON DUPLICATE KEY UPDATE record_count = record_count - 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildDeleteTrigger with aggregate decrements`() {
        val upsertStatement = """
INSERT INTO product_stats (product_id, total_revenue, units_sold)
VALUES (OLD.product_id, -(OLD.price * OLD.quantity), -OLD.quantity)
ON DUPLICATE KEY UPDATE
    total_revenue = total_revenue - (OLD.price * OLD.quantity),
    units_sold = units_sold - OLD.quantity;
        """.trimIndent()

        val result = triggerGenerator.buildDeleteTrigger(
            tableName = "product_stats",
            baseTableName = "sales",
            predicate = "OLD.product_id IS NOT NULL AND OLD.quantity > 0",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `product_stats_after_delete_summary`
            AFTER DELETE ON `sales`
            FOR EACH ROW
            BEGIN
                IF OLD.product_id IS NOT NULL AND OLD.quantity > 0 THEN
                    INSERT INTO product_stats (product_id, total_revenue, units_sold)
VALUES (OLD.product_id, -(OLD.price * OLD.quantity), -OLD.quantity)
ON DUPLICATE KEY UPDATE
    total_revenue = total_revenue - (OLD.price * OLD.quantity),
    units_sold = units_sold - OLD.quantity;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildDeleteTrigger with status-based predicate`() {
        val upsertStatement = """
INSERT INTO status_summary (status, count)
VALUES (OLD.status, -1)
ON DUPLICATE KEY UPDATE count = count - 1;
        """.trimIndent()

        val result = triggerGenerator.buildDeleteTrigger(
            tableName = "status_summary",
            baseTableName = "orders",
            predicate = "OLD.status IN ('pending', 'processing', 'completed')",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `status_summary_after_delete_summary`
            AFTER DELETE ON `orders`
            FOR EACH ROW
            BEGIN
                IF OLD.status IN ('pending', 'processing', 'completed') THEN
                    INSERT INTO status_summary (status, count)
VALUES (OLD.status, -1)
ON DUPLICATE KEY UPDATE count = count - 1;
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }

    @Test
    fun `buildDeleteTrigger with audit trail logging`() {
        val upsertStatement = """
INSERT INTO audit_log (table_name, record_id, action, user_id, deleted_at)
VALUES ('transactions', OLD.id, 'DELETE', OLD.user_id, NOW());
        """.trimIndent()

        val result = triggerGenerator.buildDeleteTrigger(
            tableName = "audit_log",
            baseTableName = "transactions",
            predicate = "OLD.id IS NOT NULL",
            upsertStatement = upsertStatement
        )

        val expected = """
            CREATE TRIGGER `audit_log_after_delete_summary`
            AFTER DELETE ON `transactions`
            FOR EACH ROW
            BEGIN
                IF OLD.id IS NOT NULL THEN
                    INSERT INTO audit_log (table_name, record_id, action, user_id, deleted_at)
VALUES ('transactions', OLD.id, 'DELETE', OLD.user_id, NOW());
                END IF;
            END;
        """.trimIndent()

        assertEquals(expected, result)
    }
}
