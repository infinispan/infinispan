package org.infinispan.server.hotrod

import java.io.{ObjectInput, ObjectOutput}
import java.lang.reflect.Constructor
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

   @volatile private var marshaller: Option[Marshaller] = None
   private val cacheEventFilterFactories = CollectionFactory.makeConcurrentMap[String, CacheEventFilterFactory](4, 0.9f, 16)
   private val cacheEventConverterFactories = CollectionFactory.makeConcurrentMap[String, CacheEventConverterFactory](4, 0.9f, 16)
   private val cacheEventFilterConverterFactories = CollectionFactory.makeConcurrentMap[String, CacheEventFilterConverterFactory](4, 0.9f, 16)

   def setEventMarshaller(eventMarshaller: Option[Marshaller]): Unit = {
      // Set a custom marshaller or reset to default if none
      marshaller = eventMarshaller
   }

   def addCacheEventFilterFactory(name: String, factory: CacheEventFilterFactory): Unit = {
      factory match {
         case x: CacheEventConverterFactory => throw illegalFilterConverterEventFactory(name)
         case _ => cacheEventFilterFactories.put(name, factory)
      }
   }

   def removeCacheEventFilterFactory(name: String): Unit = {
      cacheEventFilterFactories.remove(name)
   }

   def addCacheEventConverterFactory(name: String, factory: CacheEventConverterFactory): Unit = {
      factory match {
         case x: CacheEventFilterFactory => throw illegalFilterConverterEventFactory(name)
         case _ => cacheEventConverterFactories.put(name, factory)
      }
   }

   def removeCacheEventConverterFactory(name: String): Unit = {
      cacheEventConverterFactories.remove(name)
   }

   def addCacheEventFilterConverterFactory(name: String, factory: CacheEventFilterConverterFactory): Unit = {
      cacheEventFilterConverterFactories.put(name, factory)
   }

   def removeCacheEventFilterConverterFactory(name: String): Unit = {
      cacheEventFilterConverterFactories.remove(name)
   }

   def addClientListener(ch: Channel, h: HotRodHeader, listenerId: Bytes, cache: Cache,
           includeState: Boolean, namedFactories: NamedFactories, useRawData: Boolean): Unit = {
      val eventType = ClientEventType.apply(namedFactories._2.isDefined, useRawData, h.version)
      val clientEventSender = ClientEventSender(includeState, ch, h.version, cache, listenerId, eventType)
      val binaryFilterParams = namedFactories._1.map(_._2).getOrElse(List.empty)
      val binaryConverterParams = namedFactories._2.map(_._2).getOrElse(List.empty)
      val compatEnabled = cache.getCacheConfiguration.compatibility().enabled()

      val (filter, converter) = namedFactories match {
         case (Some(ff), Some(cf)) if ff._1 == cf._1 =>
            val binaryParams = if (binaryFilterParams.isEmpty) binaryConverterParams else binaryFilterParams
            val filterConverter = getFilterConverter(ff._1, compatEnabled, useRawData, binaryParams)
            (Some(filterConverter), Some(filterConverter))
         case (Some(ff), Some(cf)) =>
            (Some(getFilter(ff._1, compatEnabled, useRawData, binaryFilterParams)),
               Some(getConverter(cf._1, compatEnabled, useRawData, binaryConverterParams)))
         case (Some(ff), None) => (Some(getFilter(ff._1, compatEnabled, useRawData, binaryFilterParams)), None)
         case (None, Some(cf)) => (None, Some(getConverter(cf._1, compatEnabled, useRawData, binaryConverterParams)))
         case _ => (None, None)
      }

      eventSenders.put(listenerId, clientEventSender)
      cache.addListener(clientEventSender, filter.orNull, converter.orNull)
   }

   def getFilter(name: String, compatEnabled: Boolean, useRawData: Boolean, binaryParams: List[Bytes]): CacheEventFilter[Bytes, Bytes] = {
      val (factory, m) = findFactory(name, compatEnabled, cacheEventFilterFactories, "key/value filter", useRawData)
      val params = unmarshallParams(binaryParams, m, useRawData)
      factory.getFilter[Bytes, Bytes](params.toArray)
   }

   def getConverter(name: String, compatEnabled: Boolean, useRawData: Boolean, binaryParams: List[Bytes]): CacheEventConverter[Bytes, Bytes, Bytes] = {
      val (factory, m) = findConverterFactory(name, compatEnabled, cacheEventConverterFactories, "converter", useRawData)
      val params = unmarshallParams(binaryParams, m, useRawData)
      factory.getConverter[Bytes, Bytes, Bytes](params.toArray)
   }

   def getFilterConverter(name: String, compatEnabled: Boolean, useRawData: Boolean, binaryParams: List[Bytes]): CacheEventFilterConverter[Bytes, Bytes, Bytes] = {
      val (factory, m) = findFactory(name, compatEnabled, cacheEventFilterConverterFactories, "converter", useRawData)
      val params = unmarshallParams(binaryParams, m, useRawData)
      factory.getFilterConverter[Bytes, Bytes, Bytes](params.toArray)
   }

   def findConverterFactory(name: String, compatEnabled: Boolean, 
         factories: ConcurrentMap[String, CacheEventConverterFactory], factoryType: String, 
         useRawData: Boolean): (CacheEventConverterFactory, Marshaller) = {
      if (name == "___eager-key-value-version-converter") 
         (KeyValueVersionConverterFactorySingleton, new GenericJBossMarshaller())
      else
         findFactory(name, compatEnabled, factories, factoryType, useRawData)
   }

   def findFactory[T](name: String, compatEnabled: Boolean,
         factories: ConcurrentMap[String, T], factoryType: String, useRawData: Boolean): (T, Marshaller) = {
      val factory = Option(factories.get(name))
            .getOrElse(throw missingCacheEventFactory(factoryType, name))

      val m = marshaller.getOrElse(new GenericJBossMarshaller(factory.getClass.getClassLoader))
      if (useRawData || compatEnabled)
         (factory, m)
      else
         (createFactory(factory, m), m)
   }

   def createFactory[T](factory: T, marshaller: Marshaller): T = {
      factory match {
         case c: CacheEventConverterFactory => new UnmarshallConverterFactory(c, marshaller).asInstanceOf[T]
         case f: CacheEventFilterFactory =>
            new UnmarshallFilterFactory(f, marshaller).asInstanceOf[T]
         case cf: CacheEventFilterConverterFactory =>
            new UnmarshallFilterConverterFactory(cf, marshaller).asInstanceOf[T]
      }
   }

   private def unmarshallParams(binaryParams: List[Bytes], marshaller: Marshaller, useRawData: Boolean): List[AnyRef] = {
      if (!useRawData) binaryParams.map(bp => marshaller.objectFromByteBuffer(bp))
      else binaryParams
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

   private class UnmarshallFilterFactory(filterFactory: CacheEventFilterFactory, marshaller: Marshaller)
           extends CacheEventFilterFactory {
      override def getFilter[K, V](params: Array[AnyRef]): CacheEventFilter[K, V] = {
         new UnmarshallFilter(filterFactory.getFilter(params), marshaller)
            .asInstanceOf[CacheEventFilter[K, V]]
      }
   }

   private class UnmarshallConverterFactory(converterFactory: CacheEventConverterFactory, marshaller: Marshaller)
           extends CacheEventConverterFactory {
      override def getConverter[K, V, C](params: Array[AnyRef]): CacheEventConverter[K, V, C] = {
         new UnmarshallConverter(converterFactory.getConverter(params), marshaller)
                 .asInstanceOf[CacheEventConverter[K, V, C]] // ugly but it works :|
      }
   }

   private class UnmarshallFilterConverterFactory(filterConverterFactory: CacheEventFilterConverterFactory, marshaller: Marshaller)
         extends CacheEventFilterConverterFactory {
      override def getFilterConverter[K, V, C](params: Array[AnyRef]): CacheEventFilterConverter[K, V, C] = {
         new UnmarshallFilterConverter(filterConverterFactory.getFilterConverter(params), marshaller)
               .asInstanceOf[CacheEventFilterConverter[K, V, C]] // ugly but it works :|
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

   class UnmarshallFilter(val filter: CacheEventFilter[AnyRef, AnyRef], val marshaller: Marshaller)
           extends CacheEventFilter[Bytes, Bytes] {
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
         output.writeObject(obj.marshaller.getClass)
      }

      override def readObject(input: ObjectInput): UnmarshallFilter = {
         val filter = input.readObject().asInstanceOf[CacheEventFilter[AnyRef, AnyRef]]
         val marshallerClass = input.readObject().asInstanceOf[Class[_ <: Marshaller]]
         // See if the marshaller can be constructed
         val marshaller = constructMarshaller(filter, marshallerClass)
         new UnmarshallFilter(filter, marshaller)
      }

      override def getTypeClasses = setAsJavaSet(
         Set[java.lang.Class[_ <: UnmarshallFilter]](classOf[UnmarshallFilter]))
   }

   private def constructMarshaller[T](t: T, marshallerClass: Class[_ <: Marshaller]): Marshaller = {
      findClassloaderConstructor(marshallerClass)
            .map(_.newInstance(t.getClass.getClassLoader))
            .getOrElse(marshallerClass.newInstance())
   }

   private def findClassloaderConstructor[T](clazz: Class[_ <: Marshaller]): Option[Constructor[_ <: Marshaller]] = {
      try {
         Option(clazz.getConstructor(classOf[ClassLoader]))
      } catch {
         case e: NoSuchMethodException => None
      }
   }

   class UnmarshallConverter(val converter: CacheEventConverter[AnyRef, AnyRef, AnyRef], val marshaller: Marshaller)
           extends CacheEventConverter[Bytes, Bytes, Bytes] {
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
         output.writeObject(obj.marshaller.getClass)
      }

      override def readObject(input: ObjectInput): UnmarshallConverter = {
         val converter = input.readObject().asInstanceOf[CacheEventConverter [AnyRef, AnyRef, AnyRef]]
         val marshallerClass = input.readObject().asInstanceOf[Class[_ <: Marshaller]]
         val marshaller = constructMarshaller(converter, marshallerClass)
         new UnmarshallConverter(converter, marshaller)
      }

      override def getTypeClasses = setAsJavaSet(
         Set[java.lang.Class[_ <: UnmarshallConverter]](classOf[UnmarshallConverter]))
   }

   class UnmarshallFilterConverter(val filterConverter: CacheEventFilterConverter[AnyRef, AnyRef, AnyRef], val marshaller: Marshaller)
         extends AbstractCacheEventFilterConverter[Bytes, Bytes, Bytes] {
      override def filterAndConvert(key: Bytes, oldValue: Bytes, oldMetadata: Metadata,
            newValue: Bytes, newMetadata: Metadata, eventType: EventType): Bytes = {
         val unmarshalledKey = marshaller.objectFromByteBuffer(key)
         val unmarshalledPrevValue = if (oldValue != null) marshaller.objectFromByteBuffer(oldValue) else null
         val unmarshalledValue = if (newValue != null) marshaller.objectFromByteBuffer(newValue) else null
         val converted = filterConverter.filterAndConvert(unmarshalledKey, unmarshalledPrevValue,
            oldMetadata, unmarshalledValue, newMetadata, eventType)
         marshaller.objectToByteBuffer(converted)
      }
   }

   class UnmarshallFilterConverterExternalizer extends AbstractExternalizer[UnmarshallFilterConverter] {
      override def writeObject(output: ObjectOutput, obj: UnmarshallFilterConverter): Unit = {
         output.writeObject(obj.filterConverter)
         output.writeObject(obj.marshaller.getClass)
      }

      override def readObject(input: ObjectInput): UnmarshallFilterConverter = {
         val filterConverter = input.readObject().asInstanceOf[CacheEventFilterConverter[AnyRef, AnyRef, AnyRef]]
         val marshallerClass = input.readObject().asInstanceOf[Class[_ <: Marshaller]]
         val marshaller = constructMarshaller(filterConverter, marshallerClass)
         new UnmarshallFilterConverter(filterConverter, marshaller)
      }

      override def getTypeClasses = setAsJavaSet(
         Set[java.lang.Class[_ <: UnmarshallFilterConverter]](classOf[UnmarshallFilterConverter]))
   }


}

class MissingFactoryException(reason: String) extends IllegalArgumentException(reason)
