package org.infinispan.query.dsl.embedded.impl.model;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@Indexed
public class TheEntity {

   /**
    * Set a different name to demonstrate field mapping.
    */
   private String fieldX;

   private TheEmbeddedEntity embeddedEntity;

   public TheEntity(String fieldX, TheEmbeddedEntity embeddedEntity) {
      this.fieldX = fieldX;
      this.embeddedEntity = embeddedEntity;
   }

   @Basic(name = "theField", projectable = true, sortable = true)
   public String getField() {
      return fieldX;
   }

   @Embedded
   public TheEmbeddedEntity getEmbeddedEntity() {
      return embeddedEntity;
   }

   public static class TheEmbeddedEntity {

      private String fieldY;

      public TheEmbeddedEntity(String fieldY) {
         this.fieldY = fieldY;
      }

      @Basic(name = "anotherField", projectable = true)
      public String getFieldY() {
         return fieldY;
      }
   }
}
