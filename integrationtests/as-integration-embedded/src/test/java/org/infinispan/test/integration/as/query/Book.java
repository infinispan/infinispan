package org.infinispan.test.integration.as.query;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.DateBridge;
import org.hibernate.search.annotations.EncodingType;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Resolution;

import java.io.Serializable;
import java.util.Date;

@Indexed
public final class Book implements Serializable {

   public Book(String title, String publisher) {
      this.title = title;
      this.publisher = publisher;
   }

   public Book(String title, String publisher, Date pubDate) {
      this.title = title;
      this.publisher = publisher;
      this.pubDate = pubDate;
   }

   @Field(analyze = Analyze.NO)
   final String publisher;

   @Field
   final String title;

   @Field(analyze = Analyze.NO)
   @DateBridge(encoding= EncodingType.STRING, resolution= Resolution.DAY)
   private Date pubDate;

}
