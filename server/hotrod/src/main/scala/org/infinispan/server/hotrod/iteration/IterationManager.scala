package org.infinispan.server.hotrod.iteration

import java.util
import java.util.UUID

import org.infinispan.commons.marshall.Marshaller
import org.infinispan.commons.util.{CloseableIterator, CollectionFactory, InfinispanCollections}
import org.infinispan.configuration.cache.CompatibilityModeConfiguration
import org.infinispan.container.entries.CacheEntry
import org.infinispan.filter.{KeyValueFilterConverter, KeyValueFilterConverterFactory}
import org.infinispan.iteration.impl.EntryRetriever
import org.infinispan.iteration.impl.EntryRetriever.SegmentListener
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.hotrod.OperationStatus.OperationStatus
import org.infinispan.server.hotrod._
import org.infinispan.server.hotrod.logging.Log
import org.infinispan.util.concurrent.ConcurrentHashSet
import java.util.{BitSet => JavaBitSet}

/**
 * @author gustavonalle
 * @since 8.0
 */
trait IterationManager {
   type IterationId = String
   def start(cacheName: String, segments: Option[JavaBitSet], filterConverterFactory: Option[String], batch: Integer): IterationId
   def next(cacheName: String, iterationId: IterationId): IterableIterationResult
   def close(cacheName: String, iterationId: IterationId): Boolean
   def addKeyValueFilterConverterFactory[K, V, C](name: String, factory: KeyValueFilterConverterFactory[K, V, C]): Unit
   def removeKeyValueFilterConverterFactory(name: String): Unit
   def setMarshaller(maybeMarshaller: Option[Marshaller]): Unit
   def activeIterations: Int
}

class IterationSegmentsListener extends SegmentListener {
   var finished: java.util.Set[Int] = new ConcurrentHashSet[Int]()

   def clear() = finished.clear()

   override def segmentTransferred(segment: Int, sentLastEntry: Boolean): Unit = finished.add(segment)
}

class IterationState(val listener: IterationSegmentsListener, val iterator: CloseableIterator[CacheEntry[AnyRef, AnyRef]], val batch: Integer, val compatInfo: CompatInfo)

class IterableIterationResult(finishedSegments: java.util.Set[Int], val statusCode: OperationStatus, entries: List[CacheEntry[AnyRef, AnyRef]], compatInfo: CompatInfo) {
   def segmentsToBytes = {
      val bs = new util.BitSet
      finishedSegments.stream().forEach((i: Int) => bs.set(i))
      bs.toByteArray
   }

   def entrySeq: Seq[(AnyRef, AnyRef)] = {
      entries.map { entry =>
         val key = entry.getKey
         val value = entry.getValue
         if (compatInfo.enabled && compatInfo.hotRodTypeConverter.isDefined) {
            unbox(key, value)
         } else (key, value)
      }
   }

   private def unbox(key: AnyRef, value: AnyRef) = {
      val converter = compatInfo.hotRodTypeConverter.get
      (converter.unboxKey(key), converter.unboxValue(value))
   }
}

class CompatInfo(val enabled: Boolean, val hotRodTypeConverter: Option[HotRodTypeConverter])

object CompatInfo {
   def apply(config: CompatibilityModeConfiguration) =
      new CompatInfo(config.enabled(), Option(config.marshaller()).map(HotRodTypeConverter(_)))
}

class DefaultIterationManager(val cacheManager: EmbeddedCacheManager) extends IterationManager with Log {
   @volatile var marshaller: Option[_ <: Marshaller] = None

   private val iterationStateMap = CollectionFactory.makeConcurrentMap[String, IterationState]()
   private val filterConverterFactoryMap = CollectionFactory.makeConcurrentMap[String, KeyValueFilterConverterFactory[_, _, _]]()

   override def start(cacheName: String, segments: Option[JavaBitSet], filterConverterFactory: Option[String], batch: Integer): IterationId = {
      val iterationId = UUID.randomUUID().toString
      val entryRetriever = cacheManager.getCache(cacheName).getAdvancedCache.getComponentRegistry.getComponent(classOf[EntryRetriever[_, _]])
      val segmentListener = new IterationSegmentsListener
      val compatInfo = CompatInfo(cacheManager.getCacheConfiguration(cacheName).compatibility())

      val filter = {
         val customFilter = buildCustomFilter(filterConverterFactory)
         if (customFilter.isDefined || segments.isDefined) {
            new IterationFilter(compatInfo.enabled, customFilter, segments, marshaller)
         } else null
      }

      val iterator = entryRetriever.retrieveEntries(filter.asInstanceOf[KeyValueFilterConverter[Any, Any, Any]], null, null, segmentListener)
      val iterationState = new IterationState(segmentListener, iterator.asInstanceOf[CloseableIterator[CacheEntry[AnyRef, AnyRef]]], batch, compatInfo)

      iterationStateMap.put(iterationId, iterationState)
      iterationId
   }

   private def buildCustomFilter[K, V, Any](optName: Option[String]) = {
      optName match {
         case None => None
         case Some(name) => Option(filterConverterFactoryMap.get(name))
                 .map(_.getFilterConverter)
                 .orElse(throw log.missingKeyValueFilterConverterFactory(name))
      }
   }

   override def next(cacheName: String, iterationId: IterationId): IterableIterationResult = {
      val iterationState = Option(iterationStateMap.get(iterationId))
      iterationState.map { state =>
         val iterator = state.iterator
         val listener = state.listener
         val batch = state.batch
         val entries = for (i <- 0 to batch - 1; if iterator.hasNext) yield iterator.next
         new IterableIterationResult(listener.finished, OperationStatus.Success, entries.toList, state.compatInfo)
      }.getOrElse(new IterableIterationResult(InfinispanCollections.emptySet(), OperationStatus.InvalidIteration, List.empty, null))
   }

   override def close(cacheName: String, iterationId: IterationId): Boolean = {
      val iterationState = Option(iterationStateMap.get(iterationId))
      val removed = iterationState.map { state =>
         state.iterator.close()
         iterationStateMap.remove(iterationId)
      }
      Option(removed).isDefined
   }

   override def addKeyValueFilterConverterFactory[K, V, C](name: String, factory: KeyValueFilterConverterFactory[K, V, C]): Unit = filterConverterFactoryMap.put(name, factory)

   override def removeKeyValueFilterConverterFactory(name: String) = filterConverterFactoryMap.remove(name)

   override def activeIterations: Int = iterationStateMap.size()

   override def setMarshaller(maybeMarshaller: Option[Marshaller]): Unit = this.marshaller = maybeMarshaller
}
