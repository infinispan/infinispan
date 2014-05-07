package org.infinispan.server.hotrod.test

import org.infinispan.notifications.cachelistener.event.Event
import org.infinispan.server.hotrod.Bytes

/**
 * @author Galder Zamarreño
 */
trait TestClientListener {

   def onCreated(event: TestKeyWithVersionEvent) {} // no-op
   def onModified(event: TestKeyWithVersionEvent)  {} // no-op
   def onRemoved(event: TestKeyEvent)  {} // no-op
   def onCustom(event: TestCustomEvent)  {} // no-op

   def queueSize(eventType: Event.Type): Int = 0
   def pollEvent(eventType: Event.Type): AnyRef = null

   def customQueueSize(): Int = 0
   def pollCustom(): TestCustomEvent = null

   def getId : Bytes

}
