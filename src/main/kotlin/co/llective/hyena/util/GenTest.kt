package co.llective.hyena.util

import co.llective.hyena.api.BlockType
import co.llective.hyena.api.FilterType
import co.llective.hyena.api.MessageBuilder
import co.llective.hyena.api.ScanComparison
import org.apache.commons.cli.*
import org.apache.commons.cli.HelpFormatter
import java.io.File
import java.util.*
import kotlin.collections.ArrayList

class GenOptions {
    var command: String = ""
    var output: String = ""
    val columnNames: MutableList<String> = ArrayList()
    val blockTypes: MutableList<BlockType> = ArrayList()
    val columnIds: MutableList<Int> = ArrayList()
    var rows: Int = 0
    var sourceId: Int = 0
    var minTs: Long = 0
    var maxTs: Long = 0
    val partitions: MutableSet<UUID> = HashSet()
    val filterColumns: MutableList<Int> = ArrayList()
    val filterTypes: MutableList<FilterType> = ArrayList()
    val filterOperator: MutableList<ScanComparison> = ArrayList()
    val filterValues: MutableList<Any> = ArrayList()
}

fun CommandLine.countOptionValues(shortOpt: String): Int {
    return this.options.filter {option ->
        option.opt == shortOpt
    }.size
}

fun getOptions(): Options {
    val options = Options()

    options.addOption("h", "help", false, "Prints this help")
    options.addRequiredOption("c", "command", true, "Valid command (catalog, columns, addcolumn, insert, scan)")
    options.addRequiredOption("o", "output",  true, "Name of a file to put output to")
    options.addOption("n", "column-name",     true, "Name of the column")
    options.addOption("t", "column-type",     true, "Type of the column")
    options.addOption("r", "rows",            true, "Number of rows to insert")
    options.addOption("s", "source-id",       true, "Source id")
    options.addOption("i", "id",              true, "Id of the column")
    options.addOption("m", "min-ts",          true, "Lower bound for timestamps")
    options.addOption("x", "max-ts",          true, "Upper bound for timestamps")
    options.addOption("u", "uuid",            false, "Add a random partition UUID")
    options.addOption("l", "filter-column",   true, "Column id for the filter")
    options.addOption("f", "filter-type",     true, "Filter type, one of: I8, I16, I32, I64, U8, U16, U32, U64")
    options.addOption("p", "filter-operator", true, "Filter operator, one of: LT, LTEQ, EQ, GTEQ, GT, NOTEQ")
    options.addOption("v", "filter-value",    true, "Filter value")

    return options
}

fun main(args: Array<String>) {
    val parser = DefaultParser()
    val commandLine: CommandLine = try {
        parser.parse(getOptions(), args)
    } catch (exp: ParseException) {
        System.err.println("Parsing failed.  Reason: " + exp.message)
        printHelp(getOptions())
        System.exit(1)
        return
    }

    if (commandLine.hasOption("h")) {
        printHelp(getOptions())
        System.exit(0)
    }

    val options = parseOptions(commandLine)

    when (options.command) {
        "COLUMNS" -> gen_columns(options)
    }

    println(options)
}

fun gen_columns(options: GenOptions) {
    val request = MessageBuilder.buildListColumnsMessage()
    write(options, request)
}

fun write(options: GenOptions, request: ByteArray) {
    val output = File(options.output)
    output.writeBytes(request)
}

fun parseOptions(commandLine: CommandLine): GenOptions {
    val options = GenOptions()

    for (option in commandLine.options) {
        when (option.opt) {
            "c" -> options.command = option.value.toUpperCase()
            "o" -> options.output = option.value
            "n" -> options.columnNames.add(option.value)
            "t" -> options.blockTypes.add(BlockType.valueOf(option.value))
            "r" -> options.rows = Integer.parseInt(option.value)
            "s" -> options.sourceId = Integer.parseInt(option.value)
            "i" -> options.columnIds.add(Integer.parseInt(option.value))
            "m" -> options.minTs = java.lang.Long.parseLong(option.value)
            "x" -> options.maxTs = java.lang.Long.parseLong(option.value)
            "u" -> options.partitions.add(UUID.randomUUID())
            "l" -> options.filterColumns.add(Integer.parseInt(option.value))
            "f" -> options.filterTypes.add(FilterType.valueOf(option.value))
            "p" -> options.filterOperator.add(ScanComparison.valueOf(option.value))
            "v" -> options.filterValues.add(option.value)
        }
    }

    if (options.filterTypes.size != options.filterValues.size) {
        println("Number of -f and -v options must be the same")
        System.exit(1);
    }

    return options
}

private fun printHelp(options: Options) {
    val formatter = HelpFormatter()
    formatter.width = 120
    formatter.printHelp("java -jar hyena-api-gentest.jar", options)
}