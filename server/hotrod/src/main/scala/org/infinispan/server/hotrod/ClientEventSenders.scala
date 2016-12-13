package org.infinispan.server.hotrod

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong

import io.netty.channel.Channel
import org.infinispan.container.versioning.NumericVersion
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachelistener.annotation.{CacheEntryCreated, CacheEntryExpired, CacheEntryModified, CacheEntryRemoved}
import org.infinispan.notifications.cachelistener.event.Event.Type
import org.infinispan.notifications.cachelistener.event.{CacheEntryCreatedEvent, CacheEntryEvent, CacheEntryModifiedEvent, CacheEntryRemovedEvent}
import org.infinispan.server.hotrod.ClientListenerRegistry.{CustomRaw, CustomPlain, Plain, ClientEventType}
import org.infinispan.server.hotrod.Events.{CustomEvent, CustomRawEvent, KeyEvent, KeyWithVersionEvent}
import org.infinispan.server.hotrod.OperationResponse._
import org.infinispan.server.hotrod.logging.Log

object ClientEventSenders extends Log {

  val isTrace = isTraceEnabled

  private val messageId = new AtomicLong()

  def createClientSender(includeState: Boolean, ch: Channel, version: Byte,
            cache: Cache, listenerId: Bytes, eventType: ClientEventType, listenerInterests: Byte): AnyRef = {
    val compatibility = cache.getCacheConfiguration.compatibility()
    (includeState, compatibility.enabled()) match {
      case (false, false) =>
        createStatelessClientEventSender(ch, listenerId, version, eventType, listenerInterests)
      case (true, false) =>
        createStatefulClientEventSender(ch, listenerId, version, eventType, listenerInterests)
      case (false, true) =>
        val delegate = new StatelessClientEventSender(ch, listenerId, version, eventType)
        createStatelessCompatibilityClientEventSender(delegate, HotRodTypeConverter(compatibility.marshaller()), listenerInterests)
      case (true, true) =>
        val delegate = new StatelessClientEventSender(ch, listenerId, version, eventType)
        createStatefulCompatibilityClientEventSender(delegate, HotRodTypeConverter(compatibility.marshaller()), listenerInterests)
    }
  }

  def createStatelessClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, eventType: ClientEventType, listenerInterests: Byte) = {
    listenerInterests match {
      case 0x00 =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with CreatedSender with ModifiedSender with RemovedSender
      case 0x01 =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with CreatedSender
      case 0x02 =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with ModifiedSender
      case 0x03 =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with CreatedSender with ModifiedSender
      case 0x04 =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with RemovedSender
      case 0x05 =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with CreatedSender with RemovedSender
      case 0x06 =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with ModifiedSender with RemovedSender
      case 0x07 =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with CreatedSender with ModifiedSender with RemovedSender
      case 0x08 =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with ExpiredSender
      case 0x09 =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with CreatedSender
      case 0x0A =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with ModifiedSender
      case 0x0B =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with CreatedSender with ModifiedSender
      case 0x0C =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with RemovedSender
      case 0x0D =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with CreatedSender with RemovedSender
      case 0x0E =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with ModifiedSender with RemovedSender
      case 0x0F =>
        new StatelessClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with CreatedSender with ModifiedSender with RemovedSender
    }
  }

  def createStatefulClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, eventType: ClientEventType, listenerInterests: Byte) = {
    listenerInterests match {
      case 0x00 =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with CreatedSender with ModifiedSender with RemovedSender
      case 0x01 =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with CreatedSender
      case 0x02 =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with ModifiedSender
      case 0x03 =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with CreatedSender with ModifiedSender
      case 0x04 =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with RemovedSender
      case 0x05 =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with CreatedSender with RemovedSender
      case 0x06 =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with ModifiedSender with RemovedSender
      case 0x07 =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with CreatedSender with ModifiedSender with RemovedSender
      case 0x08 =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with ExpiredSender
      case 0x09 =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with CreatedSender
      case 0x0A =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with ModifiedSender
      case 0x0B =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with CreatedSender with ModifiedSender
      case 0x0C =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with RemovedSender
      case 0x0D =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with CreatedSender with RemovedSender
      case 0x0E =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with ModifiedSender with RemovedSender
      case 0x0F =>
        new StatefulClientEventSender(ch, listenerId, version, eventType) with ExpiredSender with CreatedSender with ModifiedSender with RemovedSender
    }
  }

  def createStatelessCompatibilityClientEventSender(delegate: BaseClientEventSender, converter: HotRodTypeConverter, listenerInterests: Byte) = {
    listenerInterests match {
      case 0x00 =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatCreatedSender with CompatModifiedSender with CompatRemovedSender
      case 0x01 =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatCreatedSender
      case 0x02 =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatModifiedSender
      case 0x03 =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatCreatedSender with CompatModifiedSender
      case 0x04 =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatRemovedSender
      case 0x05 =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatCreatedSender with CompatRemovedSender
      case 0x06 =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatModifiedSender with CompatRemovedSender
      case 0x07 =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatCreatedSender with CompatModifiedSender with CompatRemovedSender
      case 0x08 =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender
      case 0x09 =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatCreatedSender
      case 0x0A =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatModifiedSender
      case 0x0B =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatCreatedSender with CompatModifiedSender
      case 0x0C =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatRemovedSender
      case 0x0D =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatCreatedSender with CompatRemovedSender
      case 0x0E =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatModifiedSender with CompatRemovedSender
      case 0x0F =>
        new StatelessCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatCreatedSender with CompatModifiedSender with CompatRemovedSender
    }
  }

  def createStatefulCompatibilityClientEventSender(delegate: BaseClientEventSender, converter: HotRodTypeConverter, listenerInterests: Byte) = {
    listenerInterests match {
      case 0x00 =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatCreatedSender with CompatModifiedSender with CompatRemovedSender
      case 0x01 =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatCreatedSender
      case 0x02 =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatModifiedSender
      case 0x03 =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatCreatedSender with CompatModifiedSender
      case 0x04 =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatRemovedSender
      case 0x05 =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatCreatedSender with CompatRemovedSender
      case 0x06 =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatModifiedSender with CompatRemovedSender
      case 0x07 =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatCreatedSender with CompatModifiedSender with CompatRemovedSender
      case 0x08 =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender
      case 0x09 =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatCreatedSender
      case 0x0A =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatModifiedSender
      case 0x0B =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatCreatedSender with CompatModifiedSender
      case 0x0C =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatRemovedSender
      case 0x0D =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatCreatedSender with CompatRemovedSender
      case 0x0E =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatModifiedSender with CompatRemovedSender
      case 0x0F =>
        new StatefulCompatibilityClientEventSender(delegate, converter) with CompatExpiredSender with CompatCreatedSender with CompatModifiedSender with CompatRemovedSender
    }
  }

  // Do not make sync=false, instead move cache operation causing
  // listener calls out of the Netty event loop thread
  @Listener(clustered = true, includeCurrentState = true)
  class StatefulClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, targetEventType: ClientEventType)
    extends BaseClientEventSender(ch, listenerId, version, targetEventType)

  @Listener(clustered = true, includeCurrentState = false)
  class StatelessClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, targetEventType: ClientEventType)
    extends BaseClientEventSender(ch, listenerId, version, targetEventType)

  @Listener(clustered = true, includeCurrentState = true)
  class StatefulCompatibilityClientEventSender(
      delegate: BaseClientEventSender, converter: HotRodTypeConverter)
    extends BaseCompatibilityClientEventSender(delegate, converter)

  @Listener(clustered = true, includeCurrentState = false)
  class StatelessCompatibilityClientEventSender(
      delegate: BaseClientEventSender, converter: HotRodTypeConverter)
    extends BaseCompatibilityClientEventSender(delegate, converter)

  abstract class BaseCompatibilityClientEventSender(
      delegate: BaseClientEventSender, converter: HotRodTypeConverter) {
//    @CacheEntryCreated
//    @CacheEntryModified
//    @CacheEntryRemoved
//    @CacheEntryExpired
    def onCompatCacheEvent(event: CacheEntryEvent[AnyRef, AnyRef]) {
      val key = converter.unboxKey(event.getKey)
      val value = converter.unboxValue(event.getValue)
      if (delegate.isSendEvent(event)) {
        // In compatibility mode, version could be null if stored via embedded
        val version = event.getMetadata.version()
        val dataVersion = if (version == null) 0 else version.asInstanceOf[NumericVersion].getVersion
        delegate.sendEvent(key.asInstanceOf[Array[Byte]], value.asInstanceOf[Array[Byte]], dataVersion, event)
      }
    }
  }

  trait Sender {
    def onCacheEvent(event: CacheEntryEvent[Bytes, Bytes])
  }

  trait CreatedSender extends Sender {
    @CacheEntryCreated
    def onCreated(event: CacheEntryEvent[Bytes, Bytes]): Unit = onCacheEvent(event)
  }

  trait ModifiedSender extends Sender {
    @CacheEntryModified
    def onModified(event: CacheEntryEvent[Bytes, Bytes]): Unit = onCacheEvent(event)
  }

  trait RemovedSender extends Sender {
    @CacheEntryRemoved
    def onRemoved(event: CacheEntryEvent[Bytes, Bytes]): Unit = onCacheEvent(event)
  }

  trait ExpiredSender extends Sender {
    @CacheEntryExpired
    def onExpired(event: CacheEntryEvent[Bytes, Bytes]): Unit = onCacheEvent(event)
  }

  trait CompatSender {
    def onCompatCacheEvent(event: CacheEntryEvent[AnyRef, AnyRef])
  }

  trait CompatCreatedSender extends CompatSender {
    @CacheEntryCreated
    def onCompatCreated(event: CacheEntryEvent[AnyRef, AnyRef]): Unit = onCompatCacheEvent(event)
  }

  trait CompatModifiedSender extends CompatSender {
    @CacheEntryModified
    def onCompatModified(event: CacheEntryEvent[AnyRef, AnyRef]): Unit = onCompatCacheEvent(event)
  }

  trait CompatRemovedSender extends CompatSender {
    @CacheEntryRemoved
    def onRemoved(event: CacheEntryEvent[AnyRef, AnyRef]): Unit = onCompatCacheEvent(event)
  }

  trait CompatExpiredSender extends CompatSender {
    @CacheEntryExpired
    def onExpired(event: CacheEntryEvent[AnyRef, AnyRef]): Unit = onCompatCacheEvent(event)
  }


  //  sealed trait ClientEventType
//  case object Plain extends ClientEventType
//  case object CustomPlain extends ClientEventType
//  case object CustomRaw extends ClientEventType

  abstract class BaseClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, targetEventType: ClientEventType) {
    val eventQueue = new LinkedBlockingQueue[AnyRef](100)

    def hasChannel(channel: Channel): Boolean = ch == channel

    def writeEventsIfPossible(): Unit = {
      var written = false
      while(!eventQueue.isEmpty && ch.isWritable) {
        val event = eventQueue.poll()
        if (isTrace) tracef("Write event: %s to channel %s", event, ch)
        ch.write(event, ch.voidPromise)
        written = true
      }
      if (written) {
        ch.flush()
      }
    }

//    @CacheEntryCreated
//    @CacheEntryModified
//    @CacheEntryRemoved
//    @CacheEntryExpired
    def onCacheEvent(event: CacheEntryEvent[Bytes, Bytes]) {
      if (isSendEvent(event)) {
        sendEvent(event.getKey, event.getValue, Option(event.getMetadata)
          .map(_.version().asInstanceOf[NumericVersion].getVersion)
          .getOrElse(null.asInstanceOf[Long]), event)

      }
    }

    def isSendEvent(event: CacheEntryEvent[_, _]): Boolean = {
      if (isChannelDisconnected()) {
        log.debug("Channel disconnected, remove event sender listener")
        event.getCache.removeListener(this)
        false
      } else {
        event.getType match {
          case Type.CACHE_ENTRY_CREATED | Type.CACHE_ENTRY_MODIFIED => !event.isPre
          case Type.CACHE_ENTRY_REMOVED =>
            val removedEvent = event.asInstanceOf[CacheEntryRemovedEvent[_, _]]
            !event.isPre && removedEvent.getOldValue != null
          case Type.CACHE_ENTRY_EXPIRED =>
            true;
          case _ =>
            throw unexpectedEvent(event)
        }
      }
    }

    def isChannelDisconnected(): Boolean = !ch.isOpen

    def sendEvent(key: Bytes, value: Bytes, dataVersion: Long, event: CacheEntryEvent[_, _]) {
      val remoteEvent = createRemoteEvent(key, value, dataVersion, event)
      if (isTrace)
        log.tracef("Queue event %s, before queuing event queue size is %d", remoteEvent, eventQueue.size())

      val waitingForFlush = !ch.isWritable
      eventQueue.put(remoteEvent)

      if (!waitingForFlush) {
        // Make sure we write any event in main event loop
        ch.eventLoop().submit(() => writeEventsIfPossible())
      }
    }

    private def createRemoteEvent(key: Bytes, value: Bytes, dataVersion: Long, event: CacheEntryEvent[_, _]): AnyRef = {
      messageId.incrementAndGet() // increment message id
      // Embedded listener event implementation implements all interfaces,
      // so can't pattern match on the event instance itself. Instead, pattern
      // match on the type and the cast down to the expected event instance type
      targetEventType match {
        case Plain =>
          event.getType match {
            case Type.CACHE_ENTRY_CREATED | Type.CACHE_ENTRY_MODIFIED =>
              val (op, isRetried) = getEventResponseType(event)
              keyWithVersionEvent(key, dataVersion, op, isRetried)
            case Type.CACHE_ENTRY_REMOVED | Type.CACHE_ENTRY_EXPIRED =>
              val (op, isRetried) = getEventResponseType(event)
              KeyEvent(version, messageId.get(), op, listenerId, isRetried, key)
            case _ =>
              throw unexpectedEvent(event)
          }
        case CustomPlain =>
          val (op, isRetried) = getEventResponseType(event)
          CustomEvent(version, messageId.get(), op, listenerId, isRetried, value)
        case CustomRaw =>
          val (op, isRetried) = getEventResponseType(event)
          CustomRawEvent(version, messageId.get(), op, listenerId, isRetried, value)
      }
    }

    private def getEventResponseType(event: CacheEntryEvent[_, _]): (OperationResponse, Boolean) = {
      event.getType match {
        case Type.CACHE_ENTRY_CREATED =>
          (CacheEntryCreatedEventResponse, event.asInstanceOf[CacheEntryCreatedEvent[_, _]].isCommandRetried)
        case Type.CACHE_ENTRY_MODIFIED =>
          (CacheEntryModifiedEventResponse, event.asInstanceOf[CacheEntryModifiedEvent[_, _]].isCommandRetried)
        case Type.CACHE_ENTRY_REMOVED =>
          (CacheEntryRemovedEventResponse, event.asInstanceOf[CacheEntryRemovedEvent[_, _]].isCommandRetried)
        case Type.CACHE_ENTRY_EXPIRED =>
          (CacheEntryExpiredEventResponse, false)
        case _ => throw unexpectedEvent(event)
      }
    }

    private def keyWithVersionEvent(key: Bytes, dataVersion: Long, op: OperationResponse, isRetried: Boolean): KeyWithVersionEvent = {
      KeyWithVersionEvent(version, messageId.get(), op, listenerId, isRetried, key, dataVersion)
    }

  }

}
