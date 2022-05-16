package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Reviewer {

   private String firstName;
   private String lastName;

   public Reviewer() {
      // Default constructor for use by protostream
   }

   public Reviewer(String firstName, String lastName) {
      this.firstName = firstName;
      this.lastName = lastName;
   }

   @ProtoField(number = 1, required = true)
   @Field(index= Index.NO, store= Store.NO, analyze= Analyze.NO)
   public String getFirstName() {
      return firstName;
   }

   public void setFirstName(String name) {
      this.firstName = name;
   }

   @ProtoField(number = 2, required = true)
   @Field(index=Index.NO, store=Store.NO, analyze=Analyze.NO)
   public String getLastName() {
      return lastName;
   }

   public void setLastName(String name) {
      this.lastName = name;
   }

   @AutoProtoSchemaBuilder(includeClasses = {Reviewer.class, Revision.class})
   public interface ReviewerSchema extends GeneratedSchema {
      ReviewerSchema INSTANCE = new ReviewerSchemaImpl();
   }
}
