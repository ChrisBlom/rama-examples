package rama.examples.stocklevel.data

import com.rpl.rama.RamaSerializable

data class CreateStockReservation(
    @JvmField
    var reservationId: String,
    @JvmField
    var lines: List<StockReservationLine>
) : RamaSerializable
