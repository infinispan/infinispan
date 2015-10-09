package org.infinispan.server.hotrod.iteration

import org.infinispan.commons.marshall.Marshaller
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller
import org.infinispan.filter.KeyValueFilterConverter

import scala.util.Try

/**
 * @author gustavonalle
 * @since 8.0
 */
object MarshallerBuilder {
   def toClass[K, V, C](filter: IterationFilter[K, V, C]) = filter.marshaller.map(_.getClass).orNull

   def fromClass[K, V, C](marshallerClass: Option[Class[Marshaller]], filter: Option[KeyValueFilterConverter[K, V, C]]): Marshaller = {
      val withClassLoaderCtor = for {
         f <- filter
         m <- marshallerClass
         c <- Try(m.getConstructor(classOf[ClassLoader])).toOption
      } yield c.newInstance(filter.getClass.getClassLoader)

      withClassLoaderCtor.getOrElse {
         withEmptyCtor(marshallerClass).getOrElse(genericFromInstance(filter))
      }
   }

   private def withEmptyCtor(marshallerClass: Option[Class[Marshaller]]) = marshallerClass.map(_.newInstance())

   def genericFromInstance(instance: Option[AnyRef]): Marshaller = {
      new GenericJBossMarshaller(instance.map(_.getClass.getClassLoader).orNull)
   }
}
