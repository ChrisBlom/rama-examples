package rama.examples.stocklevel

import com.rpl.rama.test.InProcessCluster
import com.rpl.rama.test.LaunchConfig
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import org.junit.Test
import rama.examples.stocklevel.data.CreateStockReservation
import rama.examples.stocklevel.data.StockReservationLine

class RamaStockLevelModuleTest {
    @Test
    fun basicTest() {

        InProcessCluster.create().use { ipc ->
            val topologyName = "stocklevel"
            val stockLevelModule = RamaStockLevelModule()
            val moduleName = stockLevelModule.javaClass.name
            ipc.launchModule(stockLevelModule, LaunchConfig(4, 4))
            val client = RamaStockLevelClient(ipc)

            // set initial stock levels:
            client.appendStockLevelRecord("apple", 10)
            client.appendStockLevelRecord("banana", 20)
            client.appendStockLevelRecord("cherry", 30)

            // check that they are set
            ipc.waitForMicrobatchProcessedCount(moduleName, topologyName, 3)

            client.getStockLevelRecord("apple").stockLevel.toLong() shouldBe 10
            client.getStockLevelRecord("banana").stockLevel.toLong() shouldBe 20
            client.getStockLevelRecord("cherry").stockLevel.toLong() shouldBe 30


            // there is enough stock for this reservation
            client.tryReserve(
                "order1",
                mapOf(
                    "apple" to 1,
                    "banana" to 2,
                    "cherry" to 3,
                )
            ) shouldBe true

            ipc.waitForMicrobatchProcessedCount(moduleName, topologyName, 4)

            // the processed reservation decreased the stock levels:
            client.getStockLevelRecord("apple").stockLevel shouldBe 10 - 1
            client.getStockLevelRecord("banana").stockLevel shouldBe 20 - 2
            client.getStockLevelRecord("cherry").stockLevel shouldBe 30 - 3


            // there is not enough stock for this reservation
            client.tryReserve(
                "order2",
                mapOf(
                    "apple" to 1000,
                    "banana" to 2,
                    "cherry" to 3,
                )
            ) shouldBe false
            ipc.waitForMicrobatchProcessedCount(moduleName, topologyName, 4)

            // there are still 9 apples in stock, so this reservation can be processed:
            client.tryReserve(
                "order3",
                mapOf(
                    "apple" to 9,
                )
            ) shouldBe true

            ipc.waitForMicrobatchProcessedCount(moduleName, topologyName, 5)


            val publishedStockReservationEvents = EventQueue.eventsByTopic["stockReservation"].shouldNotBeNull()

            publishedStockReservationEvents.size shouldBe 2

            client.getStockLevelRecord("apple").stockLevel shouldBe 0

            publishedStockReservationEvents.first() shouldBe CreateStockReservation(
                "order1",
                listOf(
                    StockReservationLine("apple", 1),
                    StockReservationLine("banana", 2),
                    StockReservationLine("cherry", 3)
                )
            )

            publishedStockReservationEvents.last() shouldBe CreateStockReservation(
                "order3",
                listOf(
                    StockReservationLine("apple", 9),
                )
            )

            EventQueue.eventsByTopic.forEach { (topic, events) ->
                println("== $topic ==")
                events.forEach {
                    println("- $it")
                }

            }

        }
    }
}
