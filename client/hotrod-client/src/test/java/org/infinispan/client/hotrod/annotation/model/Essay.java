package org.infinispan.client.hotrod.annotation.model;

import org.infinispan.protostream.annotations.ProtoField;

public class Essay {

   private String title;

   private String content;

   public Essay() {
   }

   public Essay(String title, String content) {
      this.title = title;
      this.content = content;
   }

   @ProtoField(number = 1)
   public String getTitle() {
      return title;
   }

   public void setTitle(String title) {
      this.title = title;
   }

   @ProtoField(number = 2)
   public String getContent() {
      return content;
   }

   public void setContent(String content) {
      this.content = content;
   }
}
