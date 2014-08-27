package org.infinispan.server.hotrod.event

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}

import org.infinispan.container.versioning.NumericVersion
import org.infinispan.notifications.cachelistener.event.Event
import org.infinispan.server.hotrod._
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.server.hotrod.test.{TestClientListener, TestCustomEvent, TestKeyEvent, TestKeyWithVersionEvent}
import org.testng.AssertJUnit.{assertEquals, assertFalse, assertNotNull}

import scala.collection.mutable.ListBuffer

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

   def expectNoEvents(eventType: Option[Event.Type] = None): Unit = {
      eventType match {
         case None =>
            assertEquals(0, queueSize(Event.Type.CACHE_ENTRY_CREATED))
            assertEquals(0, queueSize(Event.Type.CACHE_ENTRY_MODIFIED))
            assertEquals(0, queueSize(Event.Type.CACHE_ENTRY_REMOVED))
            assertEquals(0, customQueueSize())
         case Some(t) =>
            assertEquals(0, queueSize(t))
      }
   }

   def expectOnlyRemovedEvent(k: Bytes)(implicit cache: Cache): Unit = {
      expectSingleEvent(k, Event.Type.CACHE_ENTRY_REMOVED)
      expectNoEvents(Some(Event.Type.CACHE_ENTRY_CREATED))
      expectNoEvents(Some(Event.Type.CACHE_ENTRY_MODIFIED))
   }

   def expectOnlyModifiedEvent(k: Bytes)(implicit cache: Cache): Unit = {
      expectSingleEvent(k, Event.Type.CACHE_ENTRY_MODIFIED)
      expectNoEvents(Some(Event.Type.CACHE_ENTRY_CREATED))
      expectNoEvents(Some(Event.Type.CACHE_ENTRY_REMOVED))
   }

   def expectOnlyCreatedEvent(k: Bytes)(implicit cache: Cache): Unit = {
      expectSingleEvent(k, Event.Type.CACHE_ENTRY_CREATED)
      expectNoEvents(Some(Event.Type.CACHE_ENTRY_MODIFIED))
      expectNoEvents(Some(Event.Type.CACHE_ENTRY_REMOVED))
   }

   def expectSingleEvent(k: Bytes, eventType: Event.Type)(implicit cache: Cache): Unit = {
      expectEvent(k, eventType)
      assertEquals(0, queueSize(eventType))
   }

   def expectEvent(k: Bytes, eventType: Event.Type)(implicit cache: Cache): Unit = {
      val event = pollEvent(eventType)
      assertNotNull(event)
      event match {
         case t: TestKeyWithVersionEvent =>
            assertByteArrayEquals(k, t.key)
            assertEquals(serverDataVersion(k, cache), t.dataVersion)
         case t: TestKeyEvent =>
            assertByteArrayEquals(k, t.key)
      }
   }

   def expectUnorderedEvents(keys: Seq[Bytes], eventType: Event.Type)(implicit cache: Cache): Unit = {
      val assertedKeys = ListBuffer[Bytes]()

      def checkUnorderedKeyEvent(key: Bytes, eventKey: Bytes): Boolean = {
         if (java.util.Arrays.equals(key, eventKey)) {
            assertFalse(assertedKeys.contains(key))
            assertedKeys += key
            true
         } else false
      }

      for (i <- 0 until keys.size) {
         val event = pollEvent(eventType)
         assertNotNull(event)
         val initialSize = assertedKeys.size
         keys.foreach { key =>
            event match {
               case t: TestKeyWithVersionEvent =>
                  val keyMatched = checkUnorderedKeyEvent(key, t.key)
                  if (keyMatched)
                     assertEquals(serverDataVersion(key, cache), t.dataVersion)
               case t: TestKeyEvent =>
                  checkUnorderedKeyEvent(key, t.key)
            }
         }
         val finalSize = assertedKeys.size
         assertEquals(initialSize + 1, finalSize)
      }
   }

   def expectSingleCustomEvent(eventData: Bytes)(implicit cache: Cache): Unit = {
      val event = pollCustom()
      assertNotNull(event)
      event match {
         case t: TestCustomEvent => assertByteArrayEquals(eventData, t.eventData)
      }
      val remaining = customQueueSize()
      assertEquals(0, remaining)
   }

   def serverDataVersion(k: Bytes, cache: Cache): Long =
      cache.getCacheEntry(k)
              .getMetadata.version().asInstanceOf[NumericVersion].getVersion

}
