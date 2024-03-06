package rama.examples.stocklevel

import com.rpl.rama.Agg
import com.rpl.rama.Block
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

    val stockLevelsPState = "\$\$stockLevels"
    val reservationPState = "\$\$reservation"

    override fun define(setup: Setup, topologies: Topologies) {
        setup.declareDepot("*stockLevelDepot", Depot.hashBy(ProductIdExtract::class.java))
        setup.declareDepot("*reservationDepot", Depot.hashBy(OrderIdExtract::class.java))
        declareStockLevelTopology(topologies)
    }


    private fun declareStockLevelTopology(topologies: Topologies) {
        val topology = topologies.stream("stocklevel")

        // state to store stock levels of products
        topology.pstate(
            stockLevelsPState, PState.mapSchema(
                String::class.java,  // indexed by productId
                PState.fixedKeysSchema(
                    "productId", String::class.java,
                    "stockLevel", Integer::class.java
                )
            )
        )

        // state that stores the last state of a reservation
        topology.pstate(
            reservationPState, PState.mapSchema(
                String::class.java,  // indexed by reservationId
                PState.fixedKeysSchema(
                    "reservationId", String::class.java,
                    "lines", PState.mapSchema(
                        String::class.java,
                        java.lang.Integer::class.java
                    ) // indexed by productId
                )
            )
        )

        // flow to process stock level resets
        // sets the incoming stock level records, overwriting the previous level
        topology
            .source("*stockLevelDepot").out("*stockLevelRecord")
            .macro(extractJavaFields("*stockLevelRecord", "*productId", "*stockLevel"))
            .hashPartition("*productId")
            .localTransform(
                stockLevelsPState, Path.key("*productId").multiPath(
                    Path.key("productId").termVal("*productId"),
                    Path.key("stockLevel").termVal("*stockLevel")
                )
            )

        // flow to process reservations
        topology.source("*reservationDepot")


            .out("*reservation")
            .macro(extractJavaFields("*reservation", "*reservationId", "*lines", "*requestId"))
            //.each(Instant::now).out("*timestamp")
            .anchor("reservation")

            // check if there is sufficient stock for each line:

            //.freshBatchSource()
            .each(Ops.EXPLODE, "*lines").out("*line")
            .macro(extractJavaFields("*line", "*productId", "*quantity"))
            // get previously reserved quantity for the line, or 0
            .hashPartition("*reservationId")

            .localSelect(reservationPState, Path.key("*reservationId", "lines", "*productId").nullToVal(0))
            .out("*reservedPreviously")

            .hashPartition("*productId")
            .localSelect(stockLevelsPState, Path.key("*productId", "stockLevel")).out("*stockLevelBefore")
            .each(Ops.MINUS, "*quantity", "*reservedPreviously").out("*toReserve")
            .each(Ops.LESS_THAN_OR_EQUAL, "*toReserve", "*stockLevelBefore").out("*sufficientStock")
            .each(Ops.PRINTLN, "*productId", "*quantity", "*sufficientStock")
            .agg(Agg.and("*sufficientStock")).out("*r")

            .ifTrue(
                Expr(Ops.NOT, "*sufficientStock"),

                Block.ackReturn(
                    "rejected"
                ),

                // there is enough stock to reserve all lines: reserve the stock
                Block

                    .each(Ops.MINUS, "*stockLevelBefore", "*toReserve").out("*stockLevelAfter")
                    .localTransform(
                        stockLevelsPState,
                        Path.key("*productId", "stockLevel").termVal("*stockLevelAfter")
                    )
                    .ackReturn(
                        "accepted"
                    )
            )

//            .branch(
//                "reservation",
//                Block.ackReturn("*reservationId")
//            )
        //.agg(Agg.count()).out("*sufficientCountA")
//
//            .ifTrue(
//                Expr(Ops.EQUAL, "*sufficientCountA", "*lineCount"),
//
//                // when all lines have sufficient stock  // TODO check all lines
//                // calculate and set new stock level
//                Block
//                    .each(Ops.EXPLODE, "*lines").out("*line")
//                    .macro(extractJavaFields("*line", "*productId", "*quantity"))
//                    .anchor("lines")
//
//                    .each(Ops.MINUS, "*stockLevelBefore", "*toReserve").out("*stockLevelAfter")
//                    .localTransform(stockLevelsPState, Path.key("*productId", "stockLevel").termVal("*stockLevelAfter"))
//                    // emit all stock level diffs to an external event queue for reporting & audit purposes
//                    .eachAsync(
//                        { productId: Any, before: Any, after: Any, reservationId: Any, requestId: Any, timestamp: Any ->
//                            EventQueue.send(
//                                "stockLevelChange", mapOf(
//                                    "productId" to productId,
//                                    "before" to before,
//                                    "after" to after,
//                                    "requestId" to requestId,
//                                    "reservationId" to reservationId,
//                                    "updatedAt" to timestamp
//                                )
//                            )
//                        },
//                        "*productId",
//                        "*stockLevelBefore",
//                        "*stockLevelAfter",
//                        "*reservationId",
//                        "*requestId",
//                        "*timestamp"
//
//                    )
//
//
//                    // emit cases where stockLevel crosses 0 to an external event queue for reporting
//                    .keepTrue(
//                        Expr(
//                            Ops.OR,
//                            Expr(
//                                Ops.AND,
//                                // NOTE: why cant we use paths here?
//                                Expr(Ops.GREATER_THAN, "*stockLevelBefore", 0),
//                                Expr(Ops.LESS_THAN_OR_EQUAL, "*stockLevelAfter", 0)
//                            ), Expr(
//                                Ops.AND,
//
//                                Expr(Ops.GREATER_THAN, "*stockLevelAfter", 0),
//                                Expr(Ops.LESS_THAN_OR_EQUAL, "*stockLevelBefore", 0)
//                            )
//                        )
//                    )
//                    .localSelect(stockLevelsPState, Path.key("*productId")).out("*stockLevelRecordBefore")
//                    .localSelect(stockLevelsPState, Path.key("*productId")).out("*stockLevelRecordAfter")
//                    .eachAsync({ after: Any, timestamp: Instant ->
//                        EventQueue.send(
//                            "stockLevelZeroCrossing",
//                            mapOf("state" to after, "timestamp" to timestamp)
//                        )
//                    }, "*stockLevelRecordAfter", "*timestamp")
//
//                    // emit reservations to an external event queue for reporting & audits
//                    .hook("reservation")
//                    .eachAsync({ x: Any -> EventQueue.send("stockReservation", x) }, "*reservation")
//
//                    // store latest reservation state
//                    .hashPartition("*reservationId").localTransform(
//                        reservationPState,
//                        Path.key("*reservationId", "reservationId").termVal("*reservationId")
//                    )
//                    // store reserved lines in reservation state
//                    .hook("lines").hashPartition("*reservationId").localTransform(
//                        reservationPState,
//                        Path.key("*reservationId", "lines", "*productId").termVal("*quantity")
//                    )
//                    // return succes result
//                    .ackReturn(ReservationResult.Accepted),
//
//                // else: emit rejected reservations
//                Block.eachAsync({ x: Any, t: Instant ->
//                    EventQueue.send(
//                        "rejectedReservation",
//                        mapOf("reservation" to x, "timestamp" to t)
//                    )
//                }, "*reservation", "*timestamp")
//
//                    .ackReturn(
//                        ReservationResult.Rejected(emptyMap())
//
//
//                    )
//            )
//
    }

}
