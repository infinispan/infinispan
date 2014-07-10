package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.query.dsl.embedded.testdomain.Account;

import java.util.Date;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class AccountPB implements Account {

   private int id;
   private String description;
   private Date creationDate;

   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   public String getDescription() {
      return description;
   }

   public void setDescription(String description) {
      this.description = description;
   }

   public Date getCreationDate() {
      return creationDate;
   }

   public void setCreationDate(Date creationDate) {
      this.creationDate = creationDate;
   }

   @Override
   public String toString() {
      return "AccountPB{" +
            "id=" + id +
            ", description='" + description + '\'' +
            ", creationDate='" + creationDate + '\'' +
            '}';
   }
}
