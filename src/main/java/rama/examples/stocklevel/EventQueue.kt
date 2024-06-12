package rama.examples.stocklevel

import java.util.concurrent.BlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue

/** mock event queue **/
object EventQueue {

    val eventsByTopic: ConcurrentHashMap<String, BlockingQueue<Any>> = ConcurrentHashMap()
    fun send(topic: String, event: Any): CompletableFuture<Boolean> {
        val events = eventsByTopic.computeIfAbsent(topic) { LinkedBlockingQueue() }
        events.add(event)
        return CompletableFuture.completedFuture(true)
    }

}