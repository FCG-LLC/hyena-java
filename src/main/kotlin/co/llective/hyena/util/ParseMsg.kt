package co.llective.hyena.util

import co.llective.hyena.api.*
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder

fun printUsage() {
    println("Usage: java -jar hyena-api-parsemsg.jar <filename>")
}

fun reduceColumns(columns: List<Column>) =
        if (columns.isEmpty()) {
            ""
        } else {
            columns.map { column -> column.toString() }.reduce { x, y -> x + ", " + y }
        }

fun <T> eitherToString(either: Either<T, ApiError>, success: (T) -> String): String =
        when (either) {
            is Left -> success(either.value)
            is Right -> "error=\"" + either.value + "\""
        }

fun printScanResult(result: ScanResult): String =
        "data=[" + result.data.map { triple ->
            "{id=" + triple.columnId +
                    ", type=" + triple.columnType +
                    ", block=" + if (triple.data.isPresent) {
                "#" + triple.data.get().block.count()
            } else {
                "no-data"
            } +
                    "}"
        }.reduce { x, y -> x + ", " + y } + "]"

fun printReply(reply: Reply) {
    when (reply) {
        is ListColumnsReply -> println(reply)
        is CatalogReply -> {
            println("CatalogReply(columns=[" +
                    reduceColumns(reply.result.columns)
                    + "], partitions=#" + reply.result.availablePartitions.size + ")")
        }
        is AddColumnReply -> {
            println("AddColumnReply("
                    + eitherToString(reply.result) { value -> "id=" + value }
                    + ")")
        }
        is InsertReply -> {
            println("InsertReply("
                    + eitherToString(reply.result) { result: Int -> "num=" + result }
                    + ")")
        }
        is ScanReply -> {
            println("ScanReply("
                    + eitherToString(reply.result) { result -> printScanResult(result) }
                    + ")")
        }
    }
}

fun main(args: Array<String>) {
    if (args.size != 1) {
        printUsage();
        System.exit(1);
    }

    val bytes = File(args[0]).readBytes()
    val buffer = ByteBuffer.wrap(bytes)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val reply = MessageDecoder.decode(buffer)
    printReply(reply)
}