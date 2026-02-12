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
}
