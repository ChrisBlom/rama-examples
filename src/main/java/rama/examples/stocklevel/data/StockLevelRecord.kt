package rama.examples.stocklevel.data

import com.rpl.rama.RamaSerializable

data class StockLevelRecord(
    @JvmField
    var productId: String,
    @JvmField
    var stockLevel: Int
) : RamaSerializable
