package org.infinispan.test.integration.data;

import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Book {

   @ProtoField(1)
   @Text
   final String title;

   @ProtoField(2)
   @Text
   final String author;

   @ProtoField(number = 3, defaultValue = "0")
   @Text
   final int publicationYear;


   @ProtoFactory
   public Book(String title, String author, int publicationYear) {
      this.title = title;
      this.author = author;
      this.publicationYear = publicationYear;
   }
}
