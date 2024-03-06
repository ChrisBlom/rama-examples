package rama.examples.stocklevel.data

import com.rpl.rama.RamaSerializable


data class CreateStockReservation(
    @JvmField
    var reservationId: String,
    @JvmField
    var lines: List<StockReservationLine>,
    @JvmField
    var requestId: Int
) : RamaSerializable

data class StockReservationLine(
    @JvmField
    var productId: String,
    @JvmField
    var quantity: Int
) : RamaSerializable
