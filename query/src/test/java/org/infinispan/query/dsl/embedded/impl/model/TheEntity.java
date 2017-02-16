package org.infinispan.query.dsl.embedded.impl.model;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.hibernate.search.annotations.Store;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@Indexed
public class TheEntity {

   /**
    * Set a different name to demonstrate field mapping.
    */
   @Field(name = "theField", store = Store.YES, analyze = Analyze.NO)
   private String fieldX;

   @IndexedEmbedded(indexNullAs = Field.DEFAULT_NULL_TOKEN)
   private TheEmbeddedEntity embeddedEntity;

   public TheEntity(String fieldX, TheEmbeddedEntity embeddedEntity) {
      this.fieldX = fieldX;
      this.embeddedEntity = embeddedEntity;
   }

   public String getField() {
      return fieldX;
   }

   public TheEmbeddedEntity getEmbeddedEntity() {
      return embeddedEntity;
   }

   public static class TheEmbeddedEntity {

      @Field(name = "anotherField", store = Store.YES, analyze = Analyze.NO)
      private String fieldY;

      public TheEmbeddedEntity(String fieldY) {
         this.fieldY = fieldY;
      }

      public String getFieldY() {
         return fieldY;
      }
   }
}
