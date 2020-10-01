package org.infinispan.query.dsl.embedded.impl.model;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;

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

   @SortableField(forField = "theField")
   @Field(name = "theField", store = Store.YES, analyze = Analyze.NO)
   public String getField() {
      return fieldX;
   }

   @IndexedEmbedded
   public TheEmbeddedEntity getEmbeddedEntity() {
      return embeddedEntity;
   }

   public static class TheEmbeddedEntity {

      private String fieldY;

      public TheEmbeddedEntity(String fieldY) {
         this.fieldY = fieldY;
      }

      @Field(name = "anotherField", store = Store.YES, analyze = Analyze.NO)
      public String getFieldY() {
         return fieldY;
      }
   }
}
