package co.llective.hyena.api

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object IntegrationTest : Spek({
    describe("Scenario with peer sockets connection") {
        it("Correctly creates and communicates with peer connection") {
            val manager = SplitConnectionManager("ipc:///Users/iceq-cs/Projects/tmp/hyena.ipc")
//            manager.connect("")
            val splitConnection = manager.getSplitConnection()
            println("kurwa mac")
//            splitConnection.connect("")
//            val resp = splitConnection.listColumns()
            val catalog = splitConnection.refreshCatalog()
//            val catalog = Catalog(arrayListOf(Column(BlockType.U64Dense, 0L, "timestamp")))
            println("Catalog:\t$catalog")
            val resp = splitConnection.scan(ScanRequest(
                    0,
                    Long.MAX_VALUE,
                    emptySet(),
                    listOf(ScanFilterBuilder(catalog)
                            .withColumn(0)
                            .withOp(ScanComparison.Gt)
                            .withValue(0L)
                            .build()),
                    listOf(0L)
            ))
            println("ScanResponse:\t$resp")
        }
    }

    describe("Connect/Close/Connect scenario") {
        val manager = SplitConnectionManager("ipc:///Users/iceq-cs/Projects/tmp/hyena.ipc")
        println("Before first connect")
        manager.ensureConnected()
        println("Connected. Closing:")
        manager.close()
        println("Closed. Connecting")
        manager.ensureConnected()
        println("Closed again.")
    }
})