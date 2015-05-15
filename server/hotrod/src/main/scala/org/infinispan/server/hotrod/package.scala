package org.infinispan.server

import java.util.function.{Function => J8Function}
import java.util.function.{Consumer => J8Consumer}

import org.infinispan.remoting.transport.Address

/**
 * @author Galder Zamarreño
 */
package object hotrod {

   type Bytes = Array[Byte]
   type Cache = org.infinispan.AdvancedCache[Bytes, Bytes]
   type AddressCache = org.infinispan.Cache[Address, ServerAddress]
   type InternalCacheEntry = org.infinispan.container.entries.InternalCacheEntry[Bytes, Bytes]
   type NamedFactory = Option[(String, List[Bytes])]
   type NamedFactories = (NamedFactory, NamedFactory)

   implicit def asScalaFunction[T, U](f: J8Function[T, U]): T => U = new Function[T, U] {
      override def apply(a: T): U = f(a)
   }

   implicit def asJavaFunction[T, U](f: T => U): J8Function[T, U] = new J8Function[T, U] {
      override def apply(a: T): U = f(a)
   }

   implicit def asJavaConsumer[T](f: T => Unit): J8Consumer[T] = new J8Consumer[T] {
      override def accept(t: T): Unit = f(t)
   }
}
