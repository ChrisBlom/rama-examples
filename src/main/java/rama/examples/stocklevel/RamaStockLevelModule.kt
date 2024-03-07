package rama.examples.stocklevel

import com.rpl.rama.Depot
import com.rpl.rama.Expr
import com.rpl.rama.PState
import com.rpl.rama.Path
import com.rpl.rama.RamaModule
import com.rpl.rama.RamaModule.Setup
import com.rpl.rama.RamaModule.Topologies
import com.rpl.rama.helpers.TopologyUtils.ExtractJavaField
import com.rpl.rama.helpers.TopologyUtils.extractJavaFields
import com.rpl.rama.ops.Ops

class RamaStockLevelModule : RamaModule {
    class ProductIdExtract : ExtractJavaField("productId")
    class OrderIdExtract : ExtractJavaField("reservationId")

    override fun define(setup: Setup, topologies: Topologies) {
        setup.declareDepot("*stockLevelDepot", Depot.hashBy(ProductIdExtract::class.java))
        setup.declareDepot("*reservationDepot", Depot.hashBy(OrderIdExtract::class.java))
        declareStockLevelTopology(topologies)
    }

    private fun declareStockLevelTopology(topologies: Topologies) {
        val topology = topologies.microbatch("stocklevel")

        val stockLevelsPState = "\$\$stockLevels"
        val reservationPState = "\$\$reservation"

        topology.pstate(
            stockLevelsPState, PState.mapSchema(
                String::class.java,  // indexed by productId
                PState.fixedKeysSchema(
                    "productId", String::class.java,
                    "stockLevel", Integer::class.java
                )
            )
        )

        // NOTE: rpl.rama.api.durable.exceptions.ValueSchemaMismatchException: Invalid value for schema
        // not very clear

        // pstate for reservations
        topology.pstate(
            reservationPState, PState.mapSchema(
                String::class.java,  // indexed reservationId
                PState.fixedKeysSchema(
                    "reservationId", String::class.java,
                    "lines", PState.mapSchema(String::class.java, Integer::class.java)
                )
            )
        )

        // store incoming stock level records, replacing any existing records
        topology.source("*stockLevelDepot")
            .out("*batch")
            .explodeMicrobatch("*batch")
            .out("*record")
            .macro(extractJavaFields("*record", "*productId", "*stockLevel")).hashPartition("*productId")
            .localTransform(
                stockLevelsPState, Path.key("*productId").multiPath(
                    Path.key("productId").termVal("*productId"),
                    Path.key("stockLevel").termVal("*stockLevel")
                )
            )

        // when a reservation arrives, subtract the reserved amount from the stock levels
        topology.source("*reservationDepot")
            .out("*batch")
            .explodeMicrobatch("*batch")
            .out("*reservation")
            .macro(extractJavaFields("*reservation", "*reservationId", "*lines"))

            .anchor("reservation")

            .hook("reservation")
            .each(Ops.EXPLODE, "*lines").out("*line")
            .macro(extractJavaFields("*line", "*productId", "*quantity"))
            .anchor("lines")

            // find current available quantity for each line
            .hashPartition("*reservationId")
            .localSelect(reservationPState, Path.key("*reservationId", "lines", "*productId"))
            .out("*quantityBefore")

            // update stock level state
            .hashPartition("*productId").localSelect(stockLevelsPState, Path.key("*productId"))
            .out("*stockLevelRecordBefore").localTransform(
                stockLevelsPState, Path.key("*productId", "stockLevel").term(
                    { stockLevel: Int, productId: String, toReserve: Int, previouslyReserved: Int? ->
                        updateStockLevel(
                            stockLevel = stockLevel,
                            productId = productId,
                            toReserve = toReserve,
                            previouslyReserved = previouslyReserved
                        )
                    }, "*productId", "*quantity", "*quantityBefore"
                )
            )

            // emit stock level diffs to a topic for reporting & audits
            .localSelect(stockLevelsPState, Path.key("*productId")).out("*stockLevelRecordAfter")
            .eachAsync({ before: Any, after: Any ->
                EventQueue.send("stockLevel", mapOf("before" to before, "after" to after))
            }, "*stockLevelRecordBefore", "*stockLevelRecordAfter")

            // emit cases where stockLevel crosses 0 to an external event queue for reporting
            .select("*stockLevelRecordBefore", Path.key("stockLevel")).out("*stockLevelBefore")
            .select("*stockLevelRecordAfter", Path.key("stockLevel")).out("*stockLevelAfter").keepTrue(
                Expr(
                    Ops.OR, Expr(
                        Ops.AND,
                        // NOTE: why cant we use paths here?
                        Expr(Ops.GREATER_THAN, "*stockLevelBefore", 0),
                        Expr(Ops.LESS_THAN_OR_EQUAL, "*stockLevelAfter", 0)
                    ), Expr(
                        Ops.AND,
                        Expr(Ops.GREATER_THAN, "*stockLevelAfter", 0),
                        Expr(Ops.LESS_THAN_OR_EQUAL, "*stockLevelBefore", 0)
                    )
                )
            ).eachAsync({ after: Any ->
                EventQueue.send("stockLevelZeroCrossing", after)
            }, "*stockLevelRecordAfter")

            // emit reservations to an external event queue for reporting & audits
            .hook("reservation")
            .eachAsync({ x: Any -> EventQueue.send("stockReservation", x) }, "*reservation")

            // store latest reservation state
            .hook("reservation")
            .each(Ops.PRINTLN, "reservation: ", "*reservation").hashPartition("*reservationId")
            .localTransform(reservationPState, Path.key("*reservationId", "reservationId").termVal("*reservationId"))

            //store lines in reservation state
            .hook("lines")
            .hashPartition("*reservationId")
            .localTransform(
                reservationPState, Path.key("*reservationId", "lines", "*productId").termVal("*quantity")
            )


    }

    companion object {
        fun updateStockLevel(
            stockLevel: Int,
            productId: String,
            toReserve: Int,
            previouslyReserved: Int?
        ): Int {

            val change = toReserve - (previouslyReserved ?: 0)

            if (change > stockLevel) {
                throw IllegalArgumentException("insufficient stock requested: $toReserve, previously reserved $previouslyReserved, change $change, stockLevel $stockLevel ")
            }

            val after = stockLevel - change
            println("Updating stock level for $productId, is $stockLevel, requested: $toReserve prev. requested: $previouslyReserved, change $change, new level: $after")
            return after
        }
    }
}
