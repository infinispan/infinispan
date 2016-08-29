package org.infinispan.query.dsl.embedded.impl.model;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.DocumentId;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Fields;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@Indexed
public class Employee {

   @DocumentId
   public String id;

   @Field(analyze = Analyze.NO, indexNullAs = Field.DEFAULT_NULL_TOKEN)
   public String name;

   @Field
   public long position;

   @Field(indexNullAs = "-1")
   public long code;

   @Field
   public String text;

   @Field(analyze = Analyze.NO)
   public String title;

   @Fields({
         @Field(name = "analyzedInfo", analyze = Analyze.YES),
         @Field(name = "someMoreInfo", analyze = Analyze.NO),
         @Field(name = "sameInfo", analyze = Analyze.NO)
   })
   public String otherInfo;

   @IndexedEmbedded(indexNullAs = Field.DEFAULT_NULL_TOKEN)
   public Company author;

   @IndexedEmbedded
   public List<ContactDetails> contactDetails = new ArrayList<>();

   @IndexedEmbedded
   public List<ContactDetails> alternativeContactDetails = new ArrayList<>();

   public static class ContactDetails {

      @Field(analyze = Analyze.NO)
      public String email;

      @Field(analyze = Analyze.NO)
      public String phoneNumber;

      @IndexedEmbedded
      public ContactAddress address;

      public static class ContactAddress {

         @Field(analyze = Analyze.NO)
         public String address;

         @Field(analyze = Analyze.NO)
         public String postCode;

         @IndexedEmbedded(depth = 3)
         public List<ContactAddress> alternatives = new ArrayList<>();
      }
   }
}
