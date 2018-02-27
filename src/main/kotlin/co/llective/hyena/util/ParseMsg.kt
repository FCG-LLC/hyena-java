package co.llective.hyena.util

import co.llective.hyena.api.ListColumnsReply
import co.llective.hyena.api.MessageDecoder
import co.llective.hyena.api.Reply
import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder

fun printUsage() {
    println("Usage: java -jar hyena-api-parsemsg.jar <filename>")
}

fun printReply(reply: Reply) {
    when (reply) {
        is ListColumnsReply -> println(reply)
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