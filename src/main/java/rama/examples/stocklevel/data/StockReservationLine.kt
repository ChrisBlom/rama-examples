package rama.examples.stocklevel.data

import com.rpl.rama.RamaSerializable

data class StockReservationLine(
    @JvmField
    var productId: String,
    @JvmField
    var quantity: Int
) : RamaSerializable
