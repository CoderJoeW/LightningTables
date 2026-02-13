package com.coderjoe.lightningtables.cli.menus.utils

object ConsoleInputHelper {
    fun getInputWithLabel(title: String): String {
        print(title)
        return readln()
    }

    fun getPasswordWithLabel(title: String): String {
        print(title)
        return String(System.console()?.readPassword() ?: readln().toCharArray())
    }

    fun <T> getListInput(
        title: String,
        items: List<T>,
        displayMapper: (T) -> String,
    ): T? {
        println(title)
        items.forEachIndexed { index, item ->
            println("  ${index + 1}. ${displayMapper(item)}")
        }
        println("  0. Cancel")
        println()
        val input = getInputWithLabel("Select: ")
        val index = input.toIntOrNull()?.minus(1) ?: return null
        return items.getOrNull(index)
    }
}
