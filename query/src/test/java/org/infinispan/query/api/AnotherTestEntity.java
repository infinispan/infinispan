package org.infinispan.query.api;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "indexB")
public class AnotherTestEntity {

   private final String value;

   @ProtoFactory
   AnotherTestEntity(String value) {
      this.value = value;
   }

   @Basic(name = "name")
   @ProtoField(1)
   public String getValue() {
      return value;
   }
}
