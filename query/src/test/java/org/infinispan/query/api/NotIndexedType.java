package org.infinispan.query.api;

import java.io.Serializable;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * A test value having a non-standard constructor, no setter and not indexed
 */
public class NotIndexedType implements Serializable {

   @ProtoFactory
   public NotIndexedType(String name) {
      this.name = name;
   }

   private final String name;

   @ProtoField(number = 1)
   public String getName() {
      return name;
   }

}
