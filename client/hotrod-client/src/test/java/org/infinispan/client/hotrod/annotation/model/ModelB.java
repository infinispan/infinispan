package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

@Indexed
@ProtoName("Model")
public class ModelB implements Model {

   @Deprecated
   public String original;

   public String different;

   @ProtoFactory
   public ModelB(String original, String different) {
      this.original = original;
      this.different = different;
   }

   @Deprecated
   @ProtoField(value = 1)
   @Basic
   public String getOriginal() {
      return original;
   }

   @ProtoField(value = 2)
   @Basic
   public String getDifferent() {
      return different;
   }
}
