package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;

@ProtoDoc("@Indexed")
public class Revision {

   private String messageId;
   private String message;
   private List<Reviewer> reviewers;

   public Revision() {
   }

   public Revision(String messageId, String message) {
      this.messageId = messageId;
      this.message = message;
   }

   @ProtoField(number = 1, required = true)
   @ProtoDoc("@Field(index=Index.YES, store=Store.NO, analyze=Analyze.NO)")
   public String getKey() {
      return messageId;
   }

   public void setKey(String key) {
      this.messageId = key;
   }

   @ProtoField(number = 2, required = true)
   @ProtoDoc("@Field(index=Index.NO, store=Store.NO, analyze=Analyze.NO)")
   public String getMessage() {
      return message;
   }

   public void setMessage(String text) {
      this.message = text;
   }

   @ProtoField(number = 3, collectionImplementation = ArrayList.class)
   @ProtoDoc("@Field(index=Index.NO, store=Store.NO, analyze=Analyze.NO)")
   public List<Reviewer> getReviewers() {
      if (reviewers == null) {
         reviewers = new ArrayList<>();
      }
      return reviewers;
   }

   public void setReviewers(List<Reviewer> reviewers) {
      this.reviewers = reviewers;
   }
}
