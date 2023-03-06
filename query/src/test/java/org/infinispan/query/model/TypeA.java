package org.infinispan.query.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed(index = "index-A")
public class TypeA {

   private String value;

   @ProtoFactory
   public TypeA(String value) {
      this.value = value;
   }

   @Basic
   @ProtoField(value = 1)
   public String getValue() {
      return value;
   }
}
