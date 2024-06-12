package rama.examples.stocklevel

import com.rpl.rama.test.InProcessCluster
import com.rpl.rama.test.LaunchConfig
import io.kotest.matchers.shouldBe
import org.junit.Test

class RamaStockLevelModuleTest {
    @Test
    fun basicTest() {

        InProcessCluster.create().use { ipc ->
            val stockLevelModule = RamaStockLevelModule()
            ipc.launchModule(stockLevelModule, LaunchConfig(4, 4))
            val client = RamaStockLevelClient(ipc)

            // set initial stock levels:
            client.appendStockLevelRecord("apple", 10)
            client.appendStockLevelRecord("banana", 20)
            client.appendStockLevelRecord("cherry", 30)

            // check that they are set
            //ipc.waitForMicrobatchProcessedCount(moduleName, topologyName, 3)

            client.getStockLevelRecord("apple").stockLevel.toLong() shouldBe 10
            client.getStockLevelRecord("banana").stockLevel.toLong() shouldBe 20
            client.getStockLevelRecord("cherry").stockLevel.toLong() shouldBe 30
//

            // there is enough stock for this reservation
            client.tryReserve(
                "order1",
                mapOf(
                    "apple" to 1,
                    "banana" to 2,
                    "cherry" to 3,
                ),
                1
            ) shouldBe "accepted"

            // the processed reservation decreased the stock levels:
            client.getStockLevelRecord("apple").stockLevel shouldBe 10 - 1
            client.getStockLevelRecord("banana").stockLevel shouldBe 20 - 2
            client.getStockLevelRecord("cherry").stockLevel shouldBe 30 - 3

            // there is not enough stock for this reservation
            client.tryReserve(
                orderId = "order2",
                lines = mapOf(
                    "apple" to 1000,
                    "banana" to 2,
                    "cherry" to 3,
                ),
                requestId = 2,
            ) // shouldBe false
//
//            // there are still 9 apples in stock, so this reservation can be processed:
//            client.tryReserve(
//                orderId = "order3",
//                lines = mapOf(
//                    "apple" to 9,
//                ),
//                requestId = 3
//            ) shouldBe true
//
//            //      ipc.waitForMicrobatchProcessedCount(moduleName, topologyName, 5)
//
//
//            val publishedStockReservationEvents = EventQueue.eventsByTopic["stockReservation"].shouldNotBeNull()
//
//            publishedStockReservationEvents.size shouldBe 2
//
//            publishedStockReservationEvents.first() shouldBe
//                    CreateStockReservation(
//                        "order1",
//                        listOf(
//                            StockReservationLine("apple", 1),
//                            StockReservationLine("banana", 2),
//                            StockReservationLine("cherry", 3)
//                        ),
//                        1
//                    )
//
//            publishedStockReservationEvents.last() shouldBe CreateStockReservation(
//                "order3",
//                listOf(
//                    StockReservationLine("apple", 9),
//                ),
//                3
//            )
//
//            EventQueue.eventsByTopic.forEach { (topic, events) ->
//                println("== $topic ==")
//                events.forEach {
//                    println("- $it")
//                }
//
//            }

        }
    }
}
