package org.infinispan.query.model;

import java.io.Serializable;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class Developer implements Serializable {

   @Keyword
   private String nick;

   @Keyword
   private String email;

   @Text
   private String biography;

   @Basic
   private Integer contributions = 0;

   @ProtoFactory
   public Developer(String nick, String email, String biography, Integer contributions) {
      this.nick = nick;
      this.email = email;
      this.biography = biography;
      this.contributions = contributions;
   }

   @ProtoField(number = 1)
   public String getNick() {
      return nick;
   }

   public void setNick(String nick) {
      this.nick = nick;
   }

   @ProtoField(number = 2)
   public String getEmail() {
      return email;
   }

   public void setEmail(String email) {
      this.email = email;
   }

   @ProtoField(number = 3)
   public String getBiography() {
      return biography;
   }

   public void setBiography(String biography) {
      this.biography = biography;
   }

   @ProtoField(number = 4, defaultValue = "0")
   public Integer getContributions() {
      return contributions;
   }

   public void setContributions(Integer contributions) {
      this.contributions = contributions;
   }

   @Override
   public String toString() {
      return "Developer{" +
            "nick='" + nick + '\'' +
            ", email='" + email + '\'' +
            ", biography='" + biography + '\'' +
            ", contributions=" + contributions +
            '}';
   }
}
