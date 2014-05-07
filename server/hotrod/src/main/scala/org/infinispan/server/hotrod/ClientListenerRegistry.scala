package org.infinispan.server.hotrod

import io.netty.channel.Channel
import java.util.concurrent.atomic.AtomicLong
import org.infinispan.commons.equivalence.{AnyEquivalence, ByteArrayEquivalence}
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller
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
import scala.Some
import org.infinispan.server.hotrod.event.{KeyValueFilterFactory, ConverterFactory}

/**
 * @author Galder Zamarreño
 */
class ClientListenerRegistry(configuration: HotRodServerConfiguration) extends Log {
   private val messageId = new AtomicLong()
   private val eventSenders = new EquivalentConcurrentHashMapV8[Bytes, AnyRef](
      ByteArrayEquivalence.INSTANCE, AnyEquivalence.getInstance())

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
         // TODO: Leave marshaller check around until a solution for the filter/converter initialization has been found
         val marshaller = configuration.marshaller()
         if (marshaller != null) new BinaryConverterFactory(converterFactory)
         else converterFactory
      }
   }

   def findFilterFactory(name: String): Option[KeyValueFilterFactory] = {
      Option(configuration.keyValueFilterFactory(name)).map { filterFactory =>
         // TODO: Leave marshaller check around until a solution for the filter/converter initialization has been found
         val marshaller = configuration.marshaller()
         if (marshaller != null) new BinaryFilterFactory(filterFactory)
         else filterFactory
      }
   }

   private def unmarshallParams(factory: NamedFactory): Iterable[AnyRef] = {
      factory match {
         case Some(namedFactory) =>
            namedFactory._2.map { paramBytes =>
               val marshaller = configuration.marshaller()
               if (marshaller == null) paramBytes
               else marshaller.objectFromByteBuffer(paramBytes)
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

   private class BinaryFilterFactory(filterFactory: KeyValueFilterFactory)
           extends KeyValueFilterFactory {
      override def getKeyValueFilter[K, V](params: Array[AnyRef]): KeyValueFilter[K, V] = {
         new BinaryFilter(filterFactory.getKeyValueFilter(params))
            .asInstanceOf[KeyValueFilter[K, V]]
      }
   }

   private class BinaryConverterFactory(converterFactory: ConverterFactory)
           extends ConverterFactory {
      override def getConverter[K, V, C](params: Array[AnyRef]): Converter[K, V, C] = {
         new BinaryConverter(converterFactory.getConverter(params))
                 .asInstanceOf[Converter[K, V, C]] // ugly but it works :|
      }
   }

}

object ClientListenerRegistry {

   private class BinaryFilter(filter: KeyValueFilter[AnyRef, AnyRef])
           extends KeyValueFilter[Bytes, Bytes] with Serializable {
      override def accept(key: Bytes, value: Bytes, metadata: Metadata): Boolean = {
         // TODO: Hardcoded temporarily. There needs to be a way to initialise filter/converter instances in remote nodes
         val marshaller = new GenericJBossMarshaller
         val unmarshalledKey = marshaller.objectFromByteBuffer(key)
         val unmarshalledValue = if (value != null) marshaller.objectFromByteBuffer(value) else null
         filter.accept(unmarshalledKey, unmarshalledValue, metadata)
      }
   }

   private class BinaryConverter(converter: Converter[AnyRef, AnyRef, AnyRef])
           extends Converter[Bytes, Bytes, Bytes] with Serializable {
      override def convert(key: Bytes, value: Bytes, metadata: Metadata): Bytes = {
         // TODO: Hardcoded temporarily. There needs to be a way to initialise filter/converter instances in remote nodes
         val marshaller = new GenericJBossMarshaller
         val unmarshalledKey = marshaller.objectFromByteBuffer(key)
         val unmarshalledValue = if (value != null) marshaller.objectFromByteBuffer(value) else null
         val converted = converter.convert(unmarshalledKey, unmarshalledValue, metadata)
         marshaller.objectToByteBuffer(converted)
      }
   }

}