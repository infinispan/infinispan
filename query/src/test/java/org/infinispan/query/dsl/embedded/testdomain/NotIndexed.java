package org.infinispan.query.dsl.embedded.testdomain;

import java.io.Serializable;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class NotIndexed implements Serializable {

   @ProtoField(number = 1)
   public String notIndexedField;

   @ProtoFactory
   public NotIndexed(String notIndexedField) {
      this.notIndexedField = notIndexedField;
   }
}
