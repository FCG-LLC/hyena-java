package co.llective.hyena.repl

import co.llective.hyena.api.BlockType
import org.python.core.PyDictionary
import org.python.util.JLineConsole


fun main(args : Array<String>) {
    val env = PyDictionary()
    env.put("Connection", Connection::class.java)
    env.put("BlockType", BlockType::class.java)

    val console = JLineConsole(env)
    console.interact()
}