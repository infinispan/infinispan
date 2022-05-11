package org.infinispan.query.dsl.embedded.impl.model;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.DocumentId;
import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.api.annotations.indexing.option.Structure;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@Indexed
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

   // When an entity is created with Infinispan,
   // the document id is reserved to link the cache entry key to the value.
   // In this case Hibernate Search is used standalone,
   // so we need to provide explicitly the document id,
   // using the Search annotation.
   @DocumentId
   @Basic(projectable = true)
   public String getId() {
      return id;
   }

   @Keyword(projectable = true)
   public String getName() {
      return name;
   }

   @Basic(sortable = true)
   public Long getPosition() {
      return position;
   }

   @Basic(sortable = true, indexNullAs = "-1")
   public Long getCode() {
      return code;
   }

   @Text(projectable = true)
   public String getText() {
      return text;
   }

   @Basic(sortable = true)
   public String getTitle() {
      return title;
   }

   @Text(name = "analyzedInfo")
   @Basic(name = "someMoreInfo")
   @Basic(name = "sameInfo")
   public String getOtherInfo() {
      return otherInfo;
   }

   @Embedded(structure = Structure.FLATTENED)
   public Company getAuthor() {
      return author;
   }

   @Embedded(structure = Structure.FLATTENED)
   public List<ContactDetails> getContactDetails() {
      return contactDetails;
   }

   @Embedded(structure = Structure.FLATTENED)
   public List<ContactDetails> getAlternativeContactDetails() {
      return alternativeContactDetails;
   }

   public static class ContactDetails {

      public String email;

      public String phoneNumber;

      public ContactAddress address;

      @Basic(projectable = true)
      public String getEmail() {
         return email;
      }

      @Basic
      public String getPhoneNumber() {
         return phoneNumber;
      }

      @Embedded(structure = Structure.FLATTENED)
      public ContactAddress getAddress() {
         return address;
      }

      public static class ContactAddress {

         public String address;

         public String postCode;

         public List<ContactAddress> alternatives = new ArrayList<>();

         @Basic
         public String getAddress() {
            return address;
         }

         @Basic
         public String getPostCode() {
            return postCode;
         }

         @Embedded(structure = Structure.FLATTENED)
         public List<ContactAddress> getAlternatives() {
            return alternatives;
         }
      }
   }
}
