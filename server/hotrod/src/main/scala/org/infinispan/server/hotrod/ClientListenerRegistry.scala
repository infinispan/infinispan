package org.infinispan.server.hotrod

import java.io.{ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicLong

import io.netty.channel.Channel
import org.infinispan.commons.equivalence.{AnyEquivalence, ByteArrayEquivalence}
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller
import org.infinispan.commons.marshall.{AbstractExternalizer, Marshaller}
import org.infinispan.commons.util.CollectionFactory
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8
import org.infinispan.container.versioning.NumericVersion
import org.infinispan.metadata.Metadata
import org.infinispan.notifications._
import org.infinispan.notifications.cachelistener.annotation.{CacheEntryCreated, CacheEntryModified, CacheEntryRemoved}
import org.infinispan.notifications.cachelistener.event._
import org.infinispan.notifications.cachelistener.filter._
import org.infinispan.notifications.cachelistener.event.Event.Type
import org.infinispan.server.hotrod.Events.{CustomRawEvent, CustomEvent, KeyEvent, KeyWithVersionEvent}
import org.infinispan.server.hotrod.OperationResponse._
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration
import org.infinispan.server.hotrod.logging.Log

import scala.collection.JavaConversions._

/**
 * @author Galder Zamarreño
 */
class ClientListenerRegistry(configuration: HotRodServerConfiguration) extends Log {
   import ClientListenerRegistry._

   private val messageId = new AtomicLong()
   private val eventSenders = new EquivalentConcurrentHashMapV8[Bytes, AnyRef](
      ByteArrayEquivalence.INSTANCE, AnyEquivalence.getInstance())

   @volatile private var marshaller: Option[Marshaller] = Some(new GenericJBossMarshaller())
   private val cacheEventFilterFactories = CollectionFactory.makeConcurrentMap[String, CacheEventFilterFactory](4, 0.9f, 16)
   private val cacheEventConverterFactories = CollectionFactory.makeConcurrentMap[String, CacheEventConverterFactory](4, 0.9f, 16)

   def setEventMarshaller(eventMarshaller: Option[Marshaller]): Unit = {
      // Set a custom marshaller or reset to default if none
      marshaller = eventMarshaller.orElse(Some(new GenericJBossMarshaller()))
   }

   def addCacheEventFilterFactory(name: String, factory: CacheEventFilterFactory): Unit = {
      cacheEventFilterFactories.put(name, factory)
   }

   def removeCacheEventFilterFactory(name: String): Unit = {
      cacheEventFilterFactories.remove(name)
   }

   def addCacheEventConverterFactory(name: String, factory: CacheEventConverterFactory): Unit = {
      cacheEventConverterFactories.put(name, factory)
   }

   def removeCacheEventConverterFactory(name: String): Unit = {
      cacheEventConverterFactories.remove(name)
   }

   def addClientListener(ch: Channel, h: HotRodHeader, listenerId: Bytes, cache: Cache,
           includeState: Boolean, filterFactory: NamedFactory, converterFactory: NamedFactory, useRawData: Boolean): Unit = {
      val eventType = ClientEventType.apply(converterFactory.isDefined, useRawData, h.version)
      val clientEventSender = ClientEventSender(includeState, ch, h.version, cache, listenerId, eventType)
      val filterParams = unmarshallParams(filterFactory, useRawData)
      val converterParams = unmarshallParams(converterFactory, useRawData)
      val compatEnabled = cache.getCacheConfiguration.compatibility().enabled()

      val filter =
         for {
            namedFactory <- filterFactory
         } yield {
            findFactory(namedFactory._1, compatEnabled, cacheEventFilterFactories, "key/value filter", useRawData)
               .getFilter[Bytes, Bytes](filterParams.toArray)
         }

      val converter =
         for {
            namedFactory <- converterFactory
         } yield {
            findConverterFactory(namedFactory._1, compatEnabled, cacheEventConverterFactories, "converter", useRawData)
               .getConverter[Bytes, Bytes, Bytes](converterParams.toArray)
         }

      eventSenders.put(listenerId, clientEventSender)
      cache.addListener(clientEventSender, filter.orNull, converter.orNull)
   }

   def findConverterFactory(name: String, compatEnabled: Boolean, factories: ConcurrentMap[String, CacheEventConverterFactory], factoryType: String, useRawData: Boolean): CacheEventConverterFactory = {
      if (name == "___eager-key-value-version-converter") KeyValueVersionConverterFactorySingleton
      else findFactory(name, compatEnabled, factories, factoryType, useRawData)
   }

   def findFactory[T](name: String, compatEnabled: Boolean, factories: ConcurrentMap[String, T], factoryType: String, useRawData: Boolean): T = {
      val factory = Option(factories.get(name)).getOrElse(
         throw new MissingFactoryException(s"Listener $factoryType factory '$name' not found in server"))

      if (useRawData || compatEnabled)
         factory
      else
         marshaller.map(m => createFactory(factory, m.getClass)).getOrElse(factory)
   }

   def createFactory[T](factory: T, marshallerClass: Class[_ <: Marshaller]): T = {
      factory match {
         case c: CacheEventConverterFactory => new UnmarshallConverterFactory(c, marshallerClass).asInstanceOf[T]
         case f: CacheEventFilterFactory => new UnmarshallFilterFactory(f, marshallerClass).asInstanceOf[T]
      }
   }

   private def unmarshallParams(factory: NamedFactory, useRawData: Boolean): Iterable[AnyRef] = {
      factory match {
         case Some(namedFactory) =>
            namedFactory._2.map { paramBytes =>
               (useRawData, marshaller) match {
                  case (false, Some(m)) => m.objectFromByteBuffer(paramBytes)
                  case _ => paramBytes
               }
            }
         case None => List.empty
      }
   }

   def removeClientListener(listenerId: Array[Byte], cache: Cache): Boolean = {
      val sender = eventSenders.get(listenerId)
      if (sender != null) {
         cache.removeListener(sender)
         true
      } else false
   }

   def stop(): Unit = {
      eventSenders.clear()
      cacheEventFilterFactories.clear()
      cacheEventConverterFactories.clear()
   }

   @Listener(clustered = true, includeCurrentState = true)
   private class StatefulClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, targetEventType: ClientEventType)
           extends BaseClientEventSender(ch, listenerId, version, targetEventType)

   @Listener(clustered = true, includeCurrentState = false)
   private class StatelessClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, targetEventType: ClientEventType)
           extends BaseClientEventSender(ch, listenerId, version, targetEventType)

   private abstract class BaseClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, targetEventType: ClientEventType) {
      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      def onCacheEvent(event: CacheEntryEvent[Bytes, Bytes]) {
         if (isSendEvent(event)) {
            val dataVersion = event.getMetadata.version().asInstanceOf[NumericVersion].getVersion
            sendEvent(event.getKey, event.getValue, dataVersion, event)
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
               case _ =>
                  throw unexpectedEvent(event)
            }
         }
      }

      def isChannelDisconnected(): Boolean = !ch.isOpen

      def sendEvent(key: Bytes, value: Bytes, dataVersion: Long, event: CacheEntryEvent[_, _]) {
         val remoteEvent = createRemoteEvent(key, value, dataVersion, event)
         if (isTraceEnabled)
            log.tracef("Send %s to remote clients", remoteEvent)

         ch.writeAndFlush(remoteEvent)
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
                  case Type.CACHE_ENTRY_REMOVED =>
                     KeyEvent(version, messageId.get(), listenerId, getEventResponseType(event)._2, key)
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
            case _ => throw unexpectedEvent(event)
         }
      }

      private def keyWithVersionEvent(key: Bytes, dataVersion: Long, op: OperationResponse, isRetried: Boolean): KeyWithVersionEvent = {
         KeyWithVersionEvent(version, messageId.get(), op, listenerId, isRetried, key, dataVersion)
      }

   }

   object ClientEventSender {
      def apply(includeState: Boolean, ch: Channel, version: Byte,
              cache: Cache, listenerId: Bytes, eventType: ClientEventType): AnyRef = {
         val compatibility = cache.getCacheConfiguration.compatibility()
         (includeState, compatibility.enabled()) match {
            case (false, false) =>
               new StatelessClientEventSender(ch, listenerId, version, eventType)
            case (true, false) =>
               new StatefulClientEventSender(ch, listenerId, version, eventType)
            case (false, true) =>
               val delegate = new StatelessClientEventSender(ch, listenerId, version, eventType)
               new StatelessCompatibilityClientEventSender(delegate, HotRodTypeConverter(compatibility.marshaller()))
            case (true, true) =>
               val delegate = new StatelessClientEventSender(ch, listenerId, version, eventType)
               new StatefulCompatibilityClientEventSender(delegate, HotRodTypeConverter(compatibility.marshaller()))
         }
      }
   }

   @Listener(clustered = true, includeCurrentState = true)
   private class StatefulCompatibilityClientEventSender(
           delegate: BaseClientEventSender, converter: HotRodTypeConverter)
      extends BaseCompatibilityClientEventSender(delegate, converter)

   @Listener(clustered = true, includeCurrentState = false)
   private class StatelessCompatibilityClientEventSender(
           delegate: BaseClientEventSender, converter: HotRodTypeConverter)
           extends BaseCompatibilityClientEventSender(delegate, converter)

   private abstract class BaseCompatibilityClientEventSender(
           delegate: BaseClientEventSender, converter: HotRodTypeConverter) {
      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      def onCacheEvent(event: CacheEntryEvent[AnyRef, AnyRef]) {
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

   private class UnmarshallFilterFactory(filterFactory: CacheEventFilterFactory, marshallerClass: Class[_ <: Marshaller])
           extends CacheEventFilterFactory {
      override def getFilter[K, V](params: Array[AnyRef]): CacheEventFilter[K, V] = {
         new UnmarshallFilter(filterFactory.getFilter(params), marshallerClass)
            .asInstanceOf[CacheEventFilter[K, V]]
      }
   }

   private class UnmarshallConverterFactory(converterFactory: CacheEventConverterFactory, marshallerClass: Class[_ <: Marshaller])
           extends CacheEventConverterFactory {
      override def getConverter[K, V, C](params: Array[AnyRef]): CacheEventConverter[K, V, C] = {
         new UnmarshallConverter(converterFactory.getConverter(params), marshallerClass)
                 .asInstanceOf[CacheEventConverter[K, V, C]] // ugly but it works :|
      }
   }

}

object ClientListenerRegistry extends Constants {

   lazy val KeyValueVersionConverterFactorySingleton = new KeyValueVersionConverterFactory()

   sealed trait ClientEventType
   case object Plain extends ClientEventType
   case object CustomPlain extends ClientEventType
   case object CustomRaw extends ClientEventType

   object ClientEventType {
      def apply(isCustom: Boolean, useRawData: Boolean, version: Byte): ClientEventType = {
         (isCustom, useRawData) match {
            case (true, true) if version >= VERSION_21 => CustomRaw
            case (true, _) => CustomPlain
            case (false, _) => Plain
         }
      }
   }

   class UnmarshallFilter(val filter: CacheEventFilter[AnyRef, AnyRef], val marshallerClass: Class[_ <: Marshaller])
           extends CacheEventFilter[Bytes, Bytes] {
      val marshaller = marshallerClass.newInstance()

      override def accept(key: Bytes, prevValue: Bytes, prevMetadata: Metadata, value: Bytes, metadata: Metadata, eventType: EventType): Boolean = {
         val unmarshalledKey = marshaller.objectFromByteBuffer(key)
         val unmarshalledPrevValue = if (prevValue != null) marshaller.objectFromByteBuffer(prevValue) else null
         val unmarshalledValue = if (value != null) marshaller.objectFromByteBuffer(value) else null
         filter.accept(unmarshalledKey, unmarshalledPrevValue, prevMetadata, unmarshalledValue, metadata, eventType)
      }
   }

   class UnmarshallFilterExternalizer extends AbstractExternalizer[UnmarshallFilter] {
      override def writeObject(output: ObjectOutput, obj: UnmarshallFilter): Unit = {
         output.writeObject(obj.filter)
         output.writeObject(obj.marshallerClass)
      }

      override def readObject(input: ObjectInput): UnmarshallFilter = {
         val filter = input.readObject().asInstanceOf[CacheEventFilter[AnyRef, AnyRef]]
         val marshallerClass = input.readObject().asInstanceOf[Class[_ <: Marshaller]]
         new UnmarshallFilter(filter, marshallerClass)
      }

      override def getTypeClasses = setAsJavaSet(
         Set[java.lang.Class[_ <: UnmarshallFilter]](classOf[UnmarshallFilter]))
   }

   class UnmarshallConverter(val converter: CacheEventConverter[AnyRef, AnyRef, AnyRef], val marshallerClass: Class[_ <: Marshaller])
           extends CacheEventConverter[Bytes, Bytes, Bytes] {
      val marshaller = marshallerClass.newInstance()

      override def convert(key: Bytes, prevValue: Bytes, prevMetadata: Metadata, value: Bytes, metadata: Metadata, eventType: EventType): Bytes = {
         val unmarshalledKey = marshaller.objectFromByteBuffer(key)
         val unmarshalledPrevValue = if (prevValue != null) marshaller.objectFromByteBuffer(prevValue) else null
         val unmarshalledValue = if (value != null) marshaller.objectFromByteBuffer(value) else null
         val converted = converter.convert(unmarshalledKey, unmarshalledPrevValue, prevMetadata, unmarshalledValue, metadata, eventType)
         marshaller.objectToByteBuffer(converted)
      }
   }

   class UnmarshallConverterExternalizer extends AbstractExternalizer[UnmarshallConverter] {
      override def writeObject(output: ObjectOutput, obj: UnmarshallConverter): Unit = {
         output.writeObject(obj.converter)
         output.writeObject(obj.marshallerClass)
      }

      override def readObject(input: ObjectInput): UnmarshallConverter = {
         val converter = input.readObject().asInstanceOf[CacheEventConverter [AnyRef, AnyRef, AnyRef]]
         val marshallerClass = input.readObject().asInstanceOf[Class[_ <: Marshaller]]
         new UnmarshallConverter(converter, marshallerClass)
      }

      override def getTypeClasses = setAsJavaSet(
         Set[java.lang.Class[_ <: UnmarshallConverter]](classOf[UnmarshallConverter]))
   }

}

class MissingFactoryException(reason: String) extends IllegalArgumentException(reason)
