package org.infinispan.query.dsl.embedded.impl.model;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.engine.backend.types.Projectable;
import org.hibernate.search.engine.backend.types.Sortable;
import org.hibernate.search.mapper.pojo.bridge.builtin.annotation.GeoPointBinding;
import org.hibernate.search.mapper.pojo.bridge.builtin.annotation.Latitude;
import org.hibernate.search.mapper.pojo.bridge.builtin.annotation.Longitude;
import org.hibernate.search.mapper.pojo.mapping.definition.annotation.DocumentId;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@Indexed
@GeoPointBinding(fieldName = "location", markerSet = "location", projectable = Projectable.YES, sortable = Sortable.YES)
@GeoPointBinding(fieldName = "officeLocation", markerSet = "officeLocation", projectable = Projectable.YES, sortable = Sortable.YES)
public class Employee {

   public String id;

   public String name;

   public long position;

   public Long code;

   public String text;

   public String title;

   public String otherInfo;

   public Company author;

   public List<ContactDetails> contactDetails = new ArrayList<>();

   public List<ContactDetails> alternativeContactDetails = new ArrayList<>();

   @Latitude(markerSet = "location")
   private Double locationLat;

   @Longitude(markerSet = "location")
   private Double locationLon;

   @Latitude(markerSet = "officeLocation")
   private Double officeLocationLat;

   @Longitude(markerSet = "officeLocation")
   private Double officeLocationLon;

   @DocumentId
   @Field(analyze = Analyze.NO, store = Store.YES)
   public String getId() {
      return id;
   }

   @Field(analyze = Analyze.NO, store = Store.YES)
   public String getName() {
      return name;
   }

   @SortableField
   @Field(analyze = Analyze.NO)
   public Long getPosition() {
      return position;
   }

   @SortableField
   @Field(indexNullAs = "-1", analyze = Analyze.NO)
   public Long getCode() {
      return code;
   }

   @Field(store = Store.YES)
   public String getText() {
      return text;
   }

   @SortableField
   @Field(analyze = Analyze.NO)
   public String getTitle() {
      return title;
   }

   @Field(name = "analyzedInfo", analyze = Analyze.YES)
   @Field(name = "someMoreInfo", analyze = Analyze.NO)
   @Field(name = "sameInfo", analyze = Analyze.NO)
   public String getOtherInfo() {
      return otherInfo;
   }

   @IndexedEmbedded
   public Company getAuthor() {
      return author;
   }

   @IndexedEmbedded
   public List<ContactDetails> getContactDetails() {
      return contactDetails;
   }

   @IndexedEmbedded
   public List<ContactDetails> getAlternativeContactDetails() {
      return alternativeContactDetails;
   }

   public static class ContactDetails {

      public String email;

      public String phoneNumber;

      public ContactAddress address;

      @Field(analyze = Analyze.NO, store = Store.YES)
      public String getEmail() {
         return email;
      }

      @Field(analyze = Analyze.NO)
      public String getPhoneNumber() {
         return phoneNumber;
      }

      @IndexedEmbedded
      public ContactAddress getAddress() {
         return address;
      }

      public static class ContactAddress {

         public String address;

         public String postCode;

         public List<ContactAddress> alternatives = new ArrayList<>();

         @Field(analyze = Analyze.NO)
         public String getAddress() {
            return address;
         }

         @Field(analyze = Analyze.NO)
         public String getPostCode() {
            return postCode;
         }

         @IndexedEmbedded(depth = 3)
         public List<ContactAddress> getAlternatives() {
            return alternatives;
         }
      }
   }
}
