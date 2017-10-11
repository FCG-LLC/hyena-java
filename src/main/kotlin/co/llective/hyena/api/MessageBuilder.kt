package co.llective.hyena.api

import com.google.common.io.LittleEndianDataOutputStream
import java.io.ByteArrayOutputStream
import java.io.IOException

object MessageBuilder {

    @Throws(IOException::class)
    internal fun buildListColumnsMessage(): ByteArray {
        val baos = ByteArrayOutputStream()
        val dos = LittleEndianDataOutputStream(baos)
        dos.writeInt(ApiRequest.ListColumns.ordinal)
        dos.writeLong(0L) // 0 bytes for payload
        baos.close()

        return baos.toByteArray()
    }
}