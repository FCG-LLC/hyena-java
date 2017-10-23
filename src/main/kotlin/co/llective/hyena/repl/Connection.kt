package co.llective.hyena.repl

import co.llective.hyena.api.*
import io.airlift.log.Logger

@Suppress("unused")
class Connection(address: String) {
    val hyena: HyenaApi = HyenaApi()
    private val log = Logger.get(HyenaApi::class.java)

    fun listColumns() {
        val columns = hyena.listColumns()
        println(columns.joinToString(", "))
    }

    fun addColumn(name: String, type: BlockType)  {
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
        val result = hyena.scan(request, null)
        println("Hyena returned ${result.rowCount} rows")
    }

    fun getCatalog() {
        val catalog = hyena.refreshCatalog()
        println(catalog)
    }

    init {
        hyena.connect(address)
    }
}