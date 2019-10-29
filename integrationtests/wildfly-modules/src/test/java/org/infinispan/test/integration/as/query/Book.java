package org.infinispan.test.integration.as.query;

import java.io.Serializable;
import java.util.Date;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.DateBridge;
import org.hibernate.search.annotations.EncodingType;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Resolution;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public final class Book implements Serializable {

   private String publisher;
   private String title;
   private Date pubDate;

   // Can't use @ProtoFactory due to IPROTO-114
   Book() {}

   Book(String title, String publisher) {
      this.title = title;
      this.publisher = publisher;
   }

   Book(String title, String publisher, Date pubDate) {
      this.title = title;
      this.publisher = publisher;
      this.pubDate = pubDate;
   }

   @Field(analyze = Analyze.NO)
   @ProtoField(number = 1)
   String getPublisher() {
      return publisher;
   }

   @Field
   @ProtoField(number = 2)
   String getTitle() {
      return title;
   }

   @Field(analyze = Analyze.NO)
   @DateBridge(encoding= EncodingType.STRING, resolution= Resolution.DAY)
   @ProtoField(number = 3)
   Date getPubDate() {
      return pubDate;
   }

   void setPublisher(String publisher) {
      this.publisher = publisher;
   }

   void setTitle(String title) {
      this.title = title;
   }

   void setPubDate(Date pubDate) {
      this.pubDate = pubDate;
   }
}
