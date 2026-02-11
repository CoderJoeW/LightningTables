package com.coderjoe.lightningtables.cli.menus

import com.coderjoe.lightningtables.cli.menus.utils.ConsoleInputHelper

object MainMenu {
    fun load() {
        var exit = false

        while (!exit) {
            val input = ConsoleInputHelper.getInputWithLabel("""
                    Menu:
                    1. Create new lightning table
                    2. List lightning tables
                    3. Exit
                """)

            when (input) {
                "1" -> {
                    println("Creating LT Table")
                }
                "2" -> {
                    println("Listing LT Tables")
                }
                "3" -> {
                    println("Exiting.")
                    exit = true
                }
                else -> {
                    println("Invalid option. Please try again.")
                }
            }
        }
    }
}