package com.coderjoe.lightningtables.cli.menus.utils

object ConsoleInputHelper {
    fun getInputWithLabel(title: String): String {
        print(title)
        return readln()
    }
}