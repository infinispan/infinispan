package org.infinispan.query.model;

import java.io.Serializable;
import java.util.Objects;
import java.util.StringJoiner;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

@Indexed
public class Developer implements Serializable {

   private String nick;

   private String email;

   private String biography;

   private Integer contributions = 0;

   private String nonIndexed;

   @ProtoFactory
   public Developer(String nick, String email, String biography, Integer contributions, String nonIndexed) {
      this.nick = nick;
      this.email = email;
      this.biography = biography;
      this.contributions = contributions;
      this.nonIndexed = nonIndexed;
   }

   @Keyword(projectable = true)
   @ProtoField(number = 1)
   public String getNick() {
      return nick;
   }

   public void setNick(String nick) {
      this.nick = nick;
   }

   @Keyword
   @ProtoField(number = 2)
   public String getEmail() {
      return email;
   }

   public void setEmail(String email) {
      this.email = email;
   }

   @Text
   @ProtoField(number = 3)
   public String getBiography() {
      return biography;
   }

   public void setBiography(String biography) {
      this.biography = biography;
   }

   @Basic(projectable = true)
   @ProtoField(number = 4, defaultValue = "0")
   public Integer getContributions() {
      return contributions;
   }

   public void setContributions(Integer contributions) {
      this.contributions = contributions;
   }

   @ProtoField(value = 5)
   public String getNonIndexed() {
      return nonIndexed;
   }

   public void setNonIndexed(String nonIndexed) {
      this.nonIndexed = nonIndexed;
   }

   @Override
   public String toString() {
      return new StringJoiner(", ", Developer.class.getSimpleName() + "[", "]")
            .add("nick='" + nick + "'")
            .add("email='" + email + "'")
            .add("biography='" + biography + "'")
            .add("contributions=" + contributions)
            .add("nonIndexed='" + nonIndexed + "'")
            .toString();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Developer developer = (Developer) o;
      return Objects.equals(nick, developer.nick) && Objects.equals(email, developer.email) && Objects.equals(biography, developer.biography) && Objects.equals(contributions, developer.contributions) && Objects.equals(nonIndexed, developer.nonIndexed);
   }

   @Override
   public int hashCode() {
      return Objects.hash(nick, email, biography, contributions, nonIndexed);
   }

   @ProtoSchema(
         includeClasses = Developer.class,
         schemaFileName = "developer.proto",
         schemaPackageName = "io.dev"
   )
   public interface DeveloperSchema extends GeneratedSchema {
      DeveloperSchema INSTANCE = new DeveloperSchemaImpl();
   }
}
