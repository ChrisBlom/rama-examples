package rama.examples.stocklevel


sealed interface ReservationResult {
    data object Accepted : ReservationResult
    data class Rejected(@JvmField val insufficientLines: Map<String, Int>) : ReservationResult
}

