package co.llective.hyena.api

import co.llective.hyena.PeerConnectionManager
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.lang.Thread.sleep
import java.util.concurrent.CompletableFuture

object IntegrationTest : Spek({
    val socketAddress = "ipc:///Users/iceq-cs/Projects/tmp/hyena.ipc"

    describe("Scenario with peer sockets connection") {
        it("Correctly creates and communicates with peer connection") {
            val api = HyenaApi(socketAddress)
            val catalog = api.refreshCatalog()
//            val catalog = Catalog(arrayListOf(Column(BlockType.U64Dense, 0L, "timestamp")))
            println("Catalog:\t$catalog")
            val resp = api.scan(ScanRequest(
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
            println("ScanResponse:\t")
        }
    }

    describe("Connect/Close/Connect scenario") {
        it("reconnects correctly") {
            val manager = PeerConnectionManager(socketAddress)
            println("Before first connect")
            manager.ensureConnection()
            println("Connected. Closing:")
            manager.close()
            println("Closed. Connecting")
            manager.ensureConnection()
            println("Closed again.")
        }
    }

    describe("Futures test") {
        it("completes future") {
            val future = CompletableFuture<String>()
            val future2 = future.thenApply { s ->
                print("Thread name: " + Thread.currentThread().name)
                s + "dupa"
            }

            println("Curr thread: " + Thread.currentThread().name)

            Thread ({
                println("Thread 1 starting")
                println("Thread 1 - sleeping 3s")
                sleep(3000)
                println("Thread 1 setting future result")
                future.complete("asd")
            }, "thread1").start()

            Thread ({
                println("Thread 2 starting")
                println("Thread 2 checks if future is done: " + future2.isDone)
                println("Thread 2 awaits for result: " + future2.get())
            }, "thread2").start()

        }
    }

    describe("HyenaApi") {
        it("Does correct catalog request") {
            val api = HyenaApi(socketAddress)
            for (i in 0..100) {
                Thread ({
                    val catalog = api.refreshCatalog()
                    println(Thread.currentThread().name + ":\t" + catalog)
                }, "thread_$i").start()
            }
        }
    }
})