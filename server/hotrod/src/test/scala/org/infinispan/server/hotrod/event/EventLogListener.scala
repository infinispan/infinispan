package org.infinispan.server.hotrod.event

import org.infinispan.server.hotrod.test.{TestCustomEvent, TestKeyEvent, TestKeyWithVersionEvent, TestClientListener}
import java.util.concurrent.{BlockingQueue, TimeUnit, ArrayBlockingQueue}
import org.infinispan.notifications.cachelistener.event.Event
import org.infinispan.server.hotrod.Bytes

/**
 * @author Galder Zamarreño
 */
private[event] class EventLogListener extends TestClientListener {
   private val createdEvents =  new ArrayBlockingQueue[TestKeyWithVersionEvent](128)
   private val modifiedEvents =  new ArrayBlockingQueue[TestKeyWithVersionEvent](128)
   private val removedEvents =  new ArrayBlockingQueue[TestKeyEvent](128)
   private val customEvents =  new ArrayBlockingQueue[TestCustomEvent](128)

   override def queueSize(eventType: Event.Type): Int =
      queue(eventType).size()

   override def pollEvent(eventType: Event.Type): AnyRef =
      queue(eventType).poll(10, TimeUnit.SECONDS)

   private def queue[T](eventType: Event.Type): BlockingQueue[T] = {
      val eventQueue = eventType match {
         case Event.Type.CACHE_ENTRY_CREATED => createdEvents
         case Event.Type.CACHE_ENTRY_MODIFIED => modifiedEvents
         case Event.Type.CACHE_ENTRY_REMOVED => removedEvents
         case _ => throw new IllegalStateException("Unexpected event type: " + eventType)
      }
      eventQueue.asInstanceOf[BlockingQueue[T]]
   }

   override def onCreated(event: TestKeyWithVersionEvent): Unit = createdEvents.add(event)
   override def onModified(event: TestKeyWithVersionEvent): Unit = modifiedEvents.add(event)
   override def onRemoved(event: TestKeyEvent): Unit = removedEvents.add(event)

   override def customQueueSize(): Int = customEvents.size()
   override def pollCustom(): TestCustomEvent = customEvents.poll(10, TimeUnit.SECONDS)
   override def onCustom(event: TestCustomEvent): Unit = customEvents.add(event)

   override def getId: Bytes = Array[Byte](1, 2, 3)
}
