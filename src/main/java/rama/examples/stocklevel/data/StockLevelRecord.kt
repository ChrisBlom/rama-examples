package rama.examples.stocklevel.data

import com.rpl.rama.RamaSerializable

class StockLevelRecord(
    @JvmField
    var productId: String,
    @JvmField
    var stockLevel: Int
) : RamaSerializable
