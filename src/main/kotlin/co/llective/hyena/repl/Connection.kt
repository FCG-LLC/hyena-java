package co.llective.hyena.repl

import co.llective.hyena.api.*
import io.airlift.log.Logger

@Suppress("unused")
class Connection(address: String) {
    val hyena: HyenaApi = HyenaApi(address)
    private val log = Logger.get(HyenaApi::class.java)

    fun listColumns() {
        val columns = hyena.listColumns()
        println(columns.joinToString(", "))
    }

    fun addColumn(name: String, type: BlockType) {
        val id = hyena.addColumn(Column(type, -1, name))
        if (id.isPresent) {
            println("Column added with id ${id.get()}")
        }
    }

    fun insert(source: Int, timestamps: List<Long>, vararg columnData: ColumnData) {
        val inserted = hyena.insert(source, timestamps, *columnData)
        if (inserted.isPresent) {
            println("Inserted ${inserted.get()}")
        }
    }

    fun scan(request: ScanRequest) {
        val result = hyena.scan(request)
        val maxrow = result.data
                .filter({ d -> d.data.isPresent() })
                .map({ d -> d.data.get().block.count() })
                .max()
        println("Hyena returned $maxrow rows")
        for (res in result.data) {
            println("${res}")
        }
    }

    fun getCatalog() {
        val catalog = hyena.refreshCatalog()
        println(catalog)
    }
}