package co.llective.hyena.repl

import org.python.core.PyDictionary
import org.python.util.JLineConsole


fun main(args : Array<String>) {
    val env = PyDictionary()
    env.put("Connection", Connection::class.java)

    val console = JLineConsole(env)
    console.interact()
}