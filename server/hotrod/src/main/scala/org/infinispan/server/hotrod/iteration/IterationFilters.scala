package org.infinispan.server.hotrod.iteration

import java.io.{ObjectInput, ObjectOutput}

import org.infinispan.commons.marshall.{AbstractExternalizer, Marshaller}
import org.infinispan.distribution.ch.ConsistentHash
import org.infinispan.factories.annotations.Inject
import org.infinispan.filter.{AbstractKeyValueFilterConverter, KeyValueFilterConverter}
import org.infinispan.metadata.Metadata
import org.infinispan.server.hotrod._

import scala.collection.JavaConversions._

/**
 * @author gustavonalle
 * @since 8.0
 */

class IterationFilter[K, V, Any](val compat: Boolean,
                                 val providedFilter: Option[KeyValueFilterConverter[K, V, Any]],
                                 val marshaller: Option[Marshaller]) extends AbstractKeyValueFilterConverter[K, V, Any] {

   private var filterMarshaller: Marshaller = _
   private var consistentHash: Option[ConsistentHash] = _

   @Inject
   def injectDependencies(cache: Cache) {
      this.consistentHash = Option(cache.getDistributionManager).map(_.getConsistentHash)
      filterMarshaller = if (compat)
         cache.getCacheConfiguration.compatibility().marshaller()
      else
         marshaller.getOrElse(MarshallerBuilder.genericFromInstance(providedFilter))
   }

   override def filterAndConvert(key: K, value: V, metadata: Metadata): Any = {
      filterByProvidedFilter(key, value, metadata).getOrElse(null.asInstanceOf[Any])
   }

   private def filterByProvidedFilter(key: K, value: V, metadata: Metadata): Option[Any] = {
      if (providedFilter.isEmpty) Some(value.asInstanceOf[Any])
      else {
         if (!compat) {
            val unmarshalledKey = filterMarshaller.objectFromByteBuffer(key.asInstanceOf[Bytes]).asInstanceOf[K]
            val unmarshalledValue = filterMarshaller.objectFromByteBuffer(value.asInstanceOf[Bytes]).asInstanceOf[V]
            val result = providedFilter.flatMap(c => Option(c.filterAndConvert(unmarshalledKey, unmarshalledValue, metadata)))
            result.map(filterMarshaller.objectToByteBuffer).map(_.asInstanceOf[Any])
         } else {
            providedFilter.map(_.filterAndConvert(key, value, metadata))
         }
      }
   }
}

class IterationFilterExternalizer[K, V, C] extends AbstractExternalizer[IterationFilter[K, V, C]] {
   override def getTypeClasses = setAsJavaSet(Set(classOf[IterationFilter[K, V, C]]))

   override def readObject(input: ObjectInput): IterationFilter[K, V, C] = {
      val compat = input.readBoolean()
      val filter = input.readObject().asInstanceOf[Option[KeyValueFilterConverter[K, V, C]]]
      val marshallerClass = input.readObject().asInstanceOf[Class[Marshaller]]
      val marshaller = MarshallerBuilder.fromClass(Option(marshallerClass), filter)
      new IterationFilter[K, V, C](compat, filter, Option(marshaller))
   }

   override def writeObject(output: ObjectOutput, obj: IterationFilter[K, V, C]) = {
      output.writeBoolean(obj.compat)
      output.writeObject(obj.providedFilter)
      output.writeObject(MarshallerBuilder.toClass(obj))
   }
}
