package co.llective.hyena.api

import java.util.*

sealed class Either<out L, out R>
data class Left<out L>(val value: L) : Either<L, Nothing>()
data class Right<out R>(val value: R) : Either<Nothing, R>()

enum class ApiErrorType(val type: ExtraType) {
    ColumnNameAlreadyExists(ExtraType.String),
    ColumnIdAlreadyExists(ExtraType.Long),
    ColumnNameCannotBeEmpty(ExtraType.None),
    NoData(ExtraType.String),
    InconsistentData(ExtraType.String),
    InvalidScanRequest(ExtraType.String),
    CatalogError(ExtraType.String),
    ScanError(ExtraType.String),
    Unknown(ExtraType.String);

    enum class ExtraType {
        Long,
        String,
        None
    }
}

data class ApiError(val type: ApiErrorType, val extra: Optional<Any>) {
    override fun toString(): String = "${this.type} (${this.extra.orElseGet({ "" })})"
}

sealed class Reply
data class ListColumnsReply(val columns: List<Column>) : Reply()
data class AddColumnReply(val result: Either<Int, ApiError>) : Reply()
data class InsertReply(val result: Either<Int, ApiError>) : Reply()
data class ScanReply(val result: Either<ScanResult, ApiError>) : Reply()
data class ScanReplySlice(val result: Either<ScanResultSlice, ApiError>) : Reply()
data class CatalogReply(val result: Catalog) : Reply()
data class SerializeError(val message: String) : Reply()
