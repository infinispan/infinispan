package org.infinispan.server.hotrod

import java.io.{ObjectInput, ObjectOutput}
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicLong

import io.netty.channel.Channel
import org.infinispan.commons.equivalence.{AnyEquivalence, ByteArrayEquivalence}
import org.infinispan.commons.marshall.{AbstractExternalizer, Marshaller}
import org.infinispan.commons.util.CollectionFactory
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8
import org.infinispan.container.versioning.NumericVersion
import org.infinispan.filter.{Converter, ConverterFactory, KeyValueFilter, KeyValueFilterFactory}
import org.infinispan.metadata.Metadata
import org.infinispan.notifications._
import org.infinispan.notifications.cachelistener.annotation.{CacheEntryCreated, CacheEntryModified, CacheEntryRemoved}
import org.infinispan.notifications.cachelistener.event.{CacheEntryRemovedEvent, CacheEntryModifiedEvent, CacheEntryCreatedEvent, CacheEntryEvent}
import org.infinispan.notifications.cachelistener.event.Event.Type
import org.infinispan.server.hotrod.ClientListenerRegistry.{BinaryConverter, BinaryFilter}
import org.infinispan.server.hotrod.Events.{CustomEvent, KeyEvent, KeyWithVersionEvent}
import org.infinispan.server.hotrod.OperationResponse._
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration
import org.infinispan.server.hotrod.logging.Log

import scala.collection.JavaConversions._

/**
 * @author Galder Zamarreño
 */
class ClientListenerRegistry(configuration: HotRodServerConfiguration) extends Log {
   private val messageId = new AtomicLong()
   private val eventSenders = new EquivalentConcurrentHashMapV8[Bytes, AnyRef](
      ByteArrayEquivalence.INSTANCE, AnyEquivalence.getInstance())

   private val marshaller = Option(configuration.marshallerClass()).map(_.newInstance())
   private val keyValueFilterFactories = CollectionFactory.makeConcurrentMap[String, KeyValueFilterFactory](4, 0.9f, 16)
   private val converterFactories = CollectionFactory.makeConcurrentMap[String, ConverterFactory](4, 0.9f, 16)

   def addKeyValueFilterFactory(name: String, factory: KeyValueFilterFactory): Unit = {
      keyValueFilterFactories.put(name, factory)
   }

   def removeKeyValueFilterFactory(name: String): Unit = {
      keyValueFilterFactories.remove(name)
   }

   def addConverterFactory(name: String, factory: ConverterFactory): Unit = {
      converterFactories.put(name, factory)
   }

   def removeConverterFactory(name: String): Unit = {
      converterFactories.remove(name)
   }

   def addClientListener(ch: Channel, h: HotRodHeader, listenerId: Bytes, cache: Cache,
           includeState: Boolean, filterFactory: NamedFactory, converterFactory: NamedFactory): Unit = {
      val isCustom = converterFactory.isDefined
      val clientEventSender = ClientEventSender(includeState, ch, h.version, cache, listenerId, isCustom)
      val filterParams = unmarshallParams(filterFactory)
      val converterParams = unmarshallParams(converterFactory)
      val compatEnabled = cache.getCacheConfiguration.compatibility().enabled()

      val filter =
         for {
            namedFactory <- filterFactory
         } yield {
            findFactory(namedFactory._1, compatEnabled, keyValueFilterFactories, "key/value filter")
               .getKeyValueFilter[Bytes, Bytes](filterParams.toArray)
         }

      val converter =
         for {
            namedFactory <- converterFactory
         } yield {
            findFactory(namedFactory._1, compatEnabled, converterFactories, "converter")
               .getConverter[Bytes, Bytes, Bytes](converterParams.toArray)
         }

      eventSenders.put(listenerId, clientEventSender)
      cache.addListener(clientEventSender, filter.orNull, converter.orNull)
   }

   def findFactory[T](name: String, compatEnabled: Boolean, factories: ConcurrentMap[String, T], factoryType: String): T = {
      val factory = Option(factories.get(name)).getOrElse(
         throw new MissingFactoryException(s"Listener $factoryType factory '$name' not found in server"))
      val marshallerClass = configuration.marshallerClass()
      if (marshallerClass != null && !compatEnabled) toBinaryFactory(factory, marshallerClass)
      else factory
   }

   def toBinaryFactory[T](factory: T, marshallerClass: Class[_ <: Marshaller]): T = {
      factory match {
         case c: ConverterFactory => new BinaryConverterFactory(c, marshallerClass).asInstanceOf[T]
         case f: KeyValueFilterFactory => new BinaryFilterFactory(f, marshallerClass).asInstanceOf[T]
      }
   }

   private def unmarshallParams(factory: NamedFactory): Iterable[AnyRef] = {
      factory match {
         case Some(namedFactory) =>
            namedFactory._2.map { paramBytes =>
               marshaller match {
                  case None => paramBytes
                  case Some(m) => m.objectFromByteBuffer(paramBytes)
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
      keyValueFilterFactories.clear()
      converterFactories.clear()
   }

   @Listener(clustered = true, includeCurrentState = true)
   private class StatefulClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, isCustom: Boolean)
           extends BaseClientEventSender(ch, listenerId, version, isCustom)

   @Listener(clustered = true, includeCurrentState = false)
   private class StatelessClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, isCustom: Boolean)
           extends BaseClientEventSender(ch, listenerId, version, isCustom)

   private abstract class BaseClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, isCustom: Boolean) {
      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      def onCacheEvent(event: CacheEntryEvent[Bytes, Bytes]) {
         if (isSendEvent(event)) {
            val dataVersion = event.getMetadata.version().asInstanceOf[NumericVersion].getVersion
            sendEvent(event.getKey, event.getValue, dataVersion, event)
         } else {
            log.debug("Channel disconnected, remove event sender listener")
            event.getCache.removeListener(this)
         }
      }

      def isSendEvent(event: CacheEntryEvent[_, _]): Boolean = !event.isPre && ch.isOpen

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
         event.getType match {
            case Type.CACHE_ENTRY_CREATED =>
               val isRetried = event.asInstanceOf[CacheEntryCreatedEvent[_, _]].isCommandRetried
               if (isCustom) createCustomEvent(value, CacheEntryCreatedEventResponse, isRetried)
               else keyWithVersionEvent(key, dataVersion, CacheEntryCreatedEventResponse, isRetried)
            case Type.CACHE_ENTRY_MODIFIED =>
               val isRetried = event.asInstanceOf[CacheEntryModifiedEvent[_, _]].isCommandRetried
               if (isCustom) createCustomEvent(value, CacheEntryModifiedEventResponse, isRetried)
               else keyWithVersionEvent(key, dataVersion, CacheEntryModifiedEventResponse, isRetried)
            case Type.CACHE_ENTRY_REMOVED =>
               val isRetried = event.asInstanceOf[CacheEntryRemovedEvent[_, _]].isCommandRetried
               if (isCustom) createCustomEvent(value, CacheEntryRemovedEventResponse, isRetried)
               else keyEvent(key, isRetried)
            case _ => throw unexpectedEvent(event)
         }
      }

      private def keyWithVersionEvent(key: Bytes, dataVersion: Long, op: OperationResponse, isRetried: Boolean): KeyWithVersionEvent = {
         KeyWithVersionEvent(version, messageId.get(), op, listenerId, isRetried, key, dataVersion)
      }

      private def keyEvent(key: Bytes, isRetried: Boolean): KeyEvent =
         KeyEvent(version, messageId.get(), listenerId, isRetried, key)

      private def createCustomEvent(value: Bytes, op: OperationResponse, isRetried: Boolean): CustomEvent = {
         // Event's value contains the transformed payload that should be sent
         // It takes advantage of the converter logic existing in cluster listeners
         CustomEvent(version, messageId.get(), op, listenerId, isRetried, value)
      }
   }

   object ClientEventSender {
      def apply(includeState: Boolean, ch: Channel, version: Byte,
              cache: Cache, listenerId: Bytes, isCustom: Boolean): AnyRef = {
         val compatibility = cache.getCacheConfiguration.compatibility()
         (includeState, compatibility.enabled()) match {
            case (false, false) =>
               new StatelessClientEventSender(ch, listenerId, version, isCustom)
            case (true, false) =>
               new StatefulClientEventSender(ch, listenerId, version, isCustom)
            case (false, true) =>
               val delegate = new StatelessClientEventSender(ch, listenerId, version, isCustom)
               new StatelessCompatibilityClientEventSender(delegate, HotRodTypeConverter(compatibility.marshaller()))
            case (true, true) =>
               val delegate = new StatelessClientEventSender(ch, listenerId, version, isCustom)
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
            delegate.sendEvent(key, value, dataVersion, event)
         } else {
            log.debug("Channel disconnected, remove event sender listener")
            event.getCache.removeListener(this)
         }
      }
   }

   private class BinaryFilterFactory(filterFactory: KeyValueFilterFactory, marshallerClass: Class[_ <: Marshaller])
           extends KeyValueFilterFactory {
      override def getKeyValueFilter[K, V](params: Array[AnyRef]): KeyValueFilter[K, V] = {
         new BinaryFilter(filterFactory.getKeyValueFilter(params), marshallerClass)
            .asInstanceOf[KeyValueFilter[K, V]]
      }
   }

   private class BinaryConverterFactory(converterFactory: ConverterFactory, marshallerClass: Class[_ <: Marshaller])
           extends ConverterFactory {
      override def getConverter[K, V, C](params: Array[AnyRef]): Converter[K, V, C] = {
         new BinaryConverter(converterFactory.getConverter(params), marshallerClass)
                 .asInstanceOf[Converter[K, V, C]] // ugly but it works :|
      }
   }

}

object ClientListenerRegistry {

   class BinaryFilter(val filter: KeyValueFilter[AnyRef, AnyRef], val marshallerClass: Class[_ <: Marshaller])
           extends KeyValueFilter[Bytes, Bytes] {
      val marshaller = marshallerClass.newInstance()

      override def accept(key: Bytes, value: Bytes, metadata: Metadata): Boolean = {
         val unmarshalledKey = marshaller.objectFromByteBuffer(key)
         val unmarshalledValue = if (value != null) marshaller.objectFromByteBuffer(value) else null
         filter.accept(unmarshalledKey, unmarshalledValue, metadata)
      }
   }

   class BinaryFilterExternalizer extends AbstractExternalizer[BinaryFilter] {
      override def writeObject(output: ObjectOutput, obj: BinaryFilter): Unit = {
         output.writeObject(obj.filter)
         output.writeObject(obj.marshallerClass)
      }

      override def readObject(input: ObjectInput): BinaryFilter = {
         val filter = input.readObject().asInstanceOf[KeyValueFilter[AnyRef, AnyRef]]
         val marshallerClass = input.readObject().asInstanceOf[Class[_ <: Marshaller]]
         new BinaryFilter(filter, marshallerClass)
      }

      override def getTypeClasses = setAsJavaSet(
         Set[java.lang.Class[_ <: BinaryFilter]](classOf[BinaryFilter]))
   }

   class BinaryConverter(val converter: Converter[AnyRef, AnyRef, AnyRef], val marshallerClass: Class[_ <: Marshaller])
           extends Converter[Bytes, Bytes, Bytes] {
      val marshaller = marshallerClass.newInstance()

      override def convert(key: Bytes, value: Bytes, metadata: Metadata): Bytes = {
         val unmarshalledKey = marshaller.objectFromByteBuffer(key)
         val unmarshalledValue = if (value != null) marshaller.objectFromByteBuffer(value) else null
         val converted = converter.convert(unmarshalledKey, unmarshalledValue, metadata)
         marshaller.objectToByteBuffer(converted)
      }
   }

   class BinaryConverterExternalizer extends AbstractExternalizer[BinaryConverter] {
      override def writeObject(output: ObjectOutput, obj: BinaryConverter): Unit = {
         output.writeObject(obj.converter)
         output.writeObject(obj.marshallerClass)
      }

      override def readObject(input: ObjectInput): BinaryConverter = {
         val converter = input.readObject().asInstanceOf[Converter[AnyRef, AnyRef, AnyRef]]
         val marshallerClass = input.readObject().asInstanceOf[Class[_ <: Marshaller]]
         new BinaryConverter(converter, marshallerClass)
      }

      override def getTypeClasses = setAsJavaSet(
         Set[java.lang.Class[_ <: BinaryConverter]](classOf[BinaryConverter]))
   }

}