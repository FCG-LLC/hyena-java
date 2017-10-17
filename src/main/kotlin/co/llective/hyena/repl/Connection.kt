package co.llective.hyena.repl

import co.llective.hyena.api.BlockType
import co.llective.hyena.api.Column
import co.llective.hyena.api.HyenaApi
import io.airlift.log.Logger

@Suppress("unused")
class Connection(address: String) {
    val hyena: HyenaApi = HyenaApi()
    private val log = Logger.get(HyenaApi::class.java)

    fun listColumns() {
        val columns = hyena.listColumns()
        log.info(columns.joinToString(", "))
    }

    fun addColumn(name: String, type: BlockType)  {
        val id = hyena.addColumn(Column(type, -1, name))
        if (id.isPresent) {
            log.info("Column added with id ${id.get()}")
        }
    }

    init {
        hyena.connect(address)
    }
}