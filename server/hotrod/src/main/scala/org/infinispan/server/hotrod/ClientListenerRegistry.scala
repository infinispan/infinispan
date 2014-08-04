package org.infinispan.server.hotrod

import java.io.{ObjectInput, ObjectOutput}

import io.netty.channel.Channel
import java.util.concurrent.atomic.AtomicLong
import org.infinispan.commons.equivalence.{AnyEquivalence, ByteArrayEquivalence}
import org.infinispan.commons.marshall.{AbstractExternalizer, Marshaller}
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8
import org.infinispan.container.versioning.NumericVersion
import org.infinispan.filter.{Converter, KeyValueFilter}
import org.infinispan.metadata.Metadata
import org.infinispan.notifications._
import org.infinispan.notifications.cachelistener.annotation.{CacheEntryRemoved, CacheEntryModified, CacheEntryCreated}
import org.infinispan.notifications.cachelistener.event.Event.Type
import org.infinispan.server.hotrod.ClientListenerRegistry.{BinaryConverter, BinaryFilter}
import org.infinispan.server.hotrod.Events.CustomEvent
import org.infinispan.server.hotrod.Events.KeyEvent
import org.infinispan.server.hotrod.Events.KeyWithVersionEvent
import org.infinispan.server.hotrod.OperationResponse._
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration
import org.infinispan.server.hotrod.logging.Log
import org.infinispan.server.hotrod.event.{KeyValueFilterFactory, ConverterFactory}

import scala.collection.JavaConversions._

/**
 * @author Galder Zamarreño
 */
class ClientListenerRegistry(configuration: HotRodServerConfiguration) extends Log {
   private val messageId = new AtomicLong()
   private val eventSenders = new EquivalentConcurrentHashMapV8[Bytes, AnyRef](
      ByteArrayEquivalence.INSTANCE, AnyEquivalence.getInstance())

   private val marshaller = Option(configuration.marshallerClass()).map(_.newInstance())

   def addClientListener(ch: Channel, h: HotRodHeader, listenerId: Bytes, cache: Cache,
           filterFactory: NamedFactory, converterFactory: NamedFactory): Unit = {
      val isCustom = converterFactory.isDefined
      val clientEventSender = new ClientEventSender(ch, listenerId, h.version, isCustom)

      val filterParams = unmarshallParams(filterFactory)
      val converterParams = unmarshallParams(converterFactory)

      val filter =
         for {
            namedFactory <- filterFactory
            factory <- findFilterFactory(namedFactory._1)
         } yield factory.getKeyValueFilter[Bytes, Bytes](filterParams.toArray)

      val converter =
         for {
            namedFactory <- converterFactory
            factory <- findConverterFactory(namedFactory._1)
         } yield factory.getConverter[Bytes, Bytes, Bytes](converterParams.toArray)

      eventSenders.put(listenerId, clientEventSender)
      cache.addListener(clientEventSender, filter.orNull, converter.orNull)
   }

   def findConverterFactory(name: String): Option[ConverterFactory] = {
      Option(configuration.converterFactory(name)).map { converterFactory =>
         val marshallerClass = configuration.marshallerClass()
         if (marshallerClass != null) new BinaryConverterFactory(converterFactory, marshallerClass)
         else converterFactory
      }
   }

   def findFilterFactory(name: String): Option[KeyValueFilterFactory] = {
      Option(configuration.keyValueFilterFactory(name)).map { filterFactory =>
         val marshallerClass = configuration.marshallerClass()
         if (marshallerClass != null) new BinaryFilterFactory(filterFactory, marshallerClass)
         else filterFactory
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

   @Listener(clustered = true, includeCurrentState = true)
   private class ClientEventSender(ch: Channel, listenerId: Bytes, version: Byte, isCustom: Boolean) {
      @CacheEntryCreated
      @CacheEntryModified
      @CacheEntryRemoved
      def onCacheEvent(event: CacheEntryEvent) {
         if (!event.isPre) {
            if (ch.isOpen) {
               val remoteEvent = createRemoteEvent(event)
               if (isTraceEnabled)
                  log.tracef("Send %s to remote clients", remoteEvent)

               ch.writeAndFlush(remoteEvent)
            } else {
               log.debug("Channel disconnected, remove event sender listener")
               event.getCache.removeListener(this)
            }
         }
      }

      private def createRemoteEvent(event: CacheEntryEvent): AnyRef = {
         messageId.incrementAndGet() // increment message id
         event.getType match {
            case Type.CACHE_ENTRY_CREATED =>
               if (isCustom) createCustomEvent(event, CacheEntryCreatedEventResponse)
               else keyWithVersionEvent(event, CacheEntryCreatedEventResponse)
            case Type.CACHE_ENTRY_MODIFIED =>
               if (isCustom) createCustomEvent(event, CacheEntryModifiedEventResponse)
               else keyWithVersionEvent(event, CacheEntryModifiedEventResponse)
            case Type.CACHE_ENTRY_REMOVED =>
               if (isCustom) createCustomEvent(event, CacheEntryRemovedEventResponse)
               else keyEvent(event)
            case _ => throw unexpectedEvent(event)
         }
      }

      private def keyWithVersionEvent(event: CacheEntryEvent, op: OperationResponse): KeyWithVersionEvent = {
         val key = event.getKey
         val dataVersion = getDataVersion(event)
         KeyWithVersionEvent(version, messageId.get(), op, listenerId, key, dataVersion)
      }

      private def keyEvent(event: CacheEntryEvent): KeyEvent =
         KeyEvent(version, messageId.get(), listenerId, event.getKey)

      private def getDataVersion(event: CacheEntryEvent): Long = {
         // Safe cast since this is a private class and it's fully controlled
         val metadata = event.getMetadata
         metadata.version().asInstanceOf[NumericVersion].getVersion
      }

      private def createCustomEvent(event: CacheEntryEvent, op: OperationResponse): CustomEvent = {
         // Event's value contains the transformed payload that should be sent
         // It takes advantage of the converter logic existing in cluster listeners
         CustomEvent(version, messageId.get(), op, listenerId, event.getValue)
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