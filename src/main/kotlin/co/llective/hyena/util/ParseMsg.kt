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

fun printReply(reply: Reply) {
    when (reply) {
        is ListColumnsReply -> println(reply)
        is CatalogReply -> {
            println("CatalogReply(columns=[" +
                    reduceColumns(reply.result.columns)
                    + "], partitions=#" + reply.result.availablePartitions.size + ")")
        }
        is AddColumnReply -> {
            println("AddColumnReply(" +
                    when(reply.result) {
                        is Left -> "id=" + reply.result.value
                        is Right -> "error=\"" + reply.result.value + "\""
                    } +
                    ")")
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