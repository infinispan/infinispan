package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Author {

   private String name;

   @ProtoFactory
   public Author(String name) {
      this.name = name;
   }

   @Keyword(projectable = true, sortable = true, normalizer = "lowercase", indexNullAs = "Ugo Foscolo", norms = false)
   @ProtoField(value = 1)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }
}
