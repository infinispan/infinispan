package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

@Indexed
@ProtoName("Model")
public class ModelA implements Model {

   private String original;

   @ProtoFactory
   public ModelA(String original) {
      this.original = original;
   }

   @ProtoField(value = 1)
   @Basic
   public String getOriginal() {
      return original;
   }
}
