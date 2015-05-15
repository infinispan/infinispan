package org.infinispan.server.hotrod.iteration

import java.io.{ObjectInput, ObjectOutput}
import java.util.{BitSet => JavaBitSet}

import org.infinispan.commons.io.UnsignedNumeric.{readUnsignedInt, writeUnsignedInt}
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller
import org.infinispan.commons.marshall.{AbstractExternalizer, Marshaller}
import org.infinispan.distribution.ch.ConsistentHash
import org.infinispan.factories.annotations.Inject
import org.infinispan.filter.{AbstractKeyValueFilterConverter, KeyValueFilterConverter}
import org.infinispan.metadata.Metadata
import org.infinispan.server.hotrod._

import scala.collection.JavaConversions._
import scala.util._

/**
 * @author gustavonalle
 * @since 8.0
 */

class IterationFilter[K, V, Any](val compat: Boolean,
                                 val providedFilter: Option[KeyValueFilterConverter[K, V, Any]],
                                 val segments: Bytes,
                                 val marshaller: Option[Marshaller]) extends AbstractKeyValueFilterConverter[K, V, Any] {

   private lazy val bitSet = JavaBitSet.valueOf(segments)
   private var filterMarshaller: Marshaller = _
   private var consistentHash: Option[ConsistentHash] = _

   @Inject
   def injectDependencies(cache: Cache) {
      this.consistentHash = Option(cache.getDistributionManager).map(_.getConsistentHash)
      filterMarshaller = if (compat)
         cache.getCacheConfiguration.compatibility().marshaller()
      else
         marshaller.getOrElse(MarshallerBuilder.genericFromFilter(providedFilter))
   }

   override def filterAndConvert(key: K, value: V, metadata: Metadata): Any = {
      val result = for {
         result1 <- filterBySegment(key, value, metadata)
         result2 <- filterByProvidedFilter(key, result1, metadata)
      } yield result2
      result.getOrElse(null.asInstanceOf[Any])
   }

   private def filterBySegment(key: K, value: V, metadata: Metadata): Option[V] = {
      if (segments.isEmpty) Some(value)
      else {
         for {
            ch <- consistentHash
            f <- Option(ch.getSegment(key))
            if bitSet.get(f)
         } yield value
      }
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
      val size = readUnsignedInt(input)
      val bytes = new Bytes(size)
      input.readFully(bytes)
      val marshallerClass = input.readObject().asInstanceOf[Class[Marshaller]]
      val marshaller = MarshallerBuilder.fromClass(Option(marshallerClass),filter)
      new IterationFilter[K, V, C](compat, filter, bytes, Option(marshaller))
   }

   override def writeObject(output: ObjectOutput, obj: IterationFilter[K, V, C]) = {
      output.writeBoolean(obj.compat)
      output.writeObject(obj.providedFilter)
      writeUnsignedInt(output, obj.segments.length)
      output.write(obj.segments)
      output.writeObject(MarshallerBuilder.toClass(obj))
   }
}

object MarshallerBuilder {
   def toClass[K, V, C](filter: IterationFilter[K,V,C]) = filter.marshaller.map(_.getClass).orNull

   def fromClass[K, V, C](marshallerClass: Option[Class[Marshaller]], filter: Option[KeyValueFilterConverter[K, V, C]]): Marshaller = {
      val withClassLoaderCtor = for {
         f <- filter
         m <- marshallerClass
         c <- Try(m.getConstructor(classOf[ClassLoader])).toOption
      } yield c.newInstance(filter.getClass.getClassLoader)

      withClassLoaderCtor.getOrElse {
         withEmptyCtor(marshallerClass).getOrElse(genericFromFilter(filter))
      }
   }

   private def withEmptyCtor(marshallerClass: Option[Class[Marshaller]]) = marshallerClass.map(_.newInstance())
   
   def genericFromFilter[K, V, C](filter: Option[KeyValueFilterConverter[K, V, C]]): Marshaller = {
      new GenericJBossMarshaller(filter.map(_.getClass.getClassLoader).orNull)
   }
}
