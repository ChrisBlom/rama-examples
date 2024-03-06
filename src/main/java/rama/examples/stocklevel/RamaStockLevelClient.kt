package rama.examples.stocklevel

import com.rpl.rama.AckLevel
import com.rpl.rama.Depot
import com.rpl.rama.PState
import com.rpl.rama.Path
import com.rpl.rama.cluster.ClusterManagerBase
import rama.examples.stocklevel.data.CreateStockReservation
import rama.examples.stocklevel.data.StockLevelRecord
import rama.examples.stocklevel.data.StockReservationLine

class RamaStockLevelClient(cluster: ClusterManagerBase) {
    private val _reservationDepot: Depot
    private val _stockLevelDepot: Depot
    private val _stockLevelState: PState

    init {
        val moduleName = RamaStockLevelModule::class.java.name
        _stockLevelDepot = cluster.clusterDepot(moduleName, "*stockLevelDepot")
        _reservationDepot = cluster.clusterDepot(moduleName, "*reservationDepot")
        _stockLevelState = cluster.clusterPState(moduleName, "\$\$stockLevels")
        //w
//        _stockLevelState.proxy(Path.all().key("stockLevel"), new ProxyState.Callback<Object>() {
//            @Override
//            public void change(Object newVal, Diff diff, Object oldVal) {
//                System.out.println("apple changed: " + oldVal + " -> " + newVal + " diff: " + diff);
//            }
//
//        });
    }

    fun appendStockLevelRecord(productId: String, quantity: Int) {
        var x = _stockLevelDepot.append(StockLevelRecord(productId, quantity), AckLevel.ACK)
        println("Acked: " + x)

    }

    fun tryReserve(orderId: String, lines: Map<String, Int>, requestId: Int): Any? {
        var x = _reservationDepot.append(
            CreateStockReservation(
                reservationId = orderId,
                lines = lines.entries.map { (key, value) ->
                    StockReservationLine(
                        productId = key,
                        quantity = value
                    )
                },
                requestId = requestId
            ), AckLevel.ACK
        )
        println("Acked" + x)
        return x.get("stocklevel")

    }

    private fun stockLevel(productId: String): Int = _stockLevelState.selectOne<Any>(
        Path.key(productId, "stockLevel")
    ) as Int

    fun getStockLevelRecord(id: String?): StockLevelRecord {
        val record = _stockLevelState.selectOne<Map<*, *>>(Path.key(id))
        return StockLevelRecord(record["productId"] as String, (record["stockLevel"] as Int))
    }
}
