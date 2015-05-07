package org.infinispan.server.hotrod.event

import java.io.{ObjectInput, ObjectOutput}

import org.infinispan.commons.marshall.AbstractExternalizer

import scala.collection.JavaConversions._

/**
 * Externalizer for KeyValueWithPreviousEventConverter
 *
 * @author gustavonalle
 * @since 7.2
 */
class KeyValueWithPreviousEventConverterExternalizer[K,V] extends AbstractExternalizer[KeyValueWithPreviousEventConverter[K,V]] {
   def getTypeClasses = setAsJavaSet(Set(classOf[KeyValueWithPreviousEventConverter[K,V]]))

   override def readObject(input: ObjectInput) = new KeyValueWithPreviousEventConverter[K,V]()

   override def writeObject(output: ObjectOutput, `object`: KeyValueWithPreviousEventConverter[K,V]): Unit = {}
}
