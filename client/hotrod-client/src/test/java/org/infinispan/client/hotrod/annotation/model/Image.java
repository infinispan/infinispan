package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

@Indexed
@ProtoName("Image")
public class Image {

   private String name;

   @ProtoFactory
   public Image(String name) {
      this.name = name;
   }

   @ProtoField(value = 1)
   @Basic
   public String getName() {
      return name;
   }
}
