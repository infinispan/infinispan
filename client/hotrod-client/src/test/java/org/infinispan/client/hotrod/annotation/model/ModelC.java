package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

@Indexed
@ProtoName("Model")
public class ModelC implements Model {

   @Deprecated(forRemoval=true)
   public String original;

   @Deprecated(forRemoval=true)
   public String different;

   public String divergent;

   @ProtoFactory
   public ModelC(String original, String different, String divergent) {
      this.original = original;
      this.different = different;
      this.divergent = divergent;
   }

   @Deprecated(forRemoval=true)
   @ProtoField(value = 1)
   @Basic
   public String getOriginal() {
      return original;
   }

   @Deprecated(forRemoval=true)
   @ProtoField(value = 2)
   @Basic
   public String getDifferent() {
      return different;
   }

   @ProtoField(value = 3)
   @Basic
   public String getDivergent() {
      return divergent;
   }
}
