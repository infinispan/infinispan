package org.infinispan.protostream.sampledomain;

import java.io.Serializable;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class NotIndexed implements Serializable {

   @ProtoField(1)
   public final String notIndexedField;

   @ProtoFactory
   public NotIndexed(String notIndexedField) {
      this.notIndexedField = notIndexedField;
   }
}
