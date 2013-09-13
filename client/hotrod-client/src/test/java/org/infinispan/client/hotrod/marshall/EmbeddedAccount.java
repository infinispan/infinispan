package org.infinispan.client.hotrod.marshall;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

import java.util.Date;

/**
 * A class similar to {@code org.infinispan.protostream.sampledomain.Account}, that maps to the same protobuf type,
 * {@code sample_bank_account.Account}.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Indexed
public class EmbeddedAccount {

   @Field(store = Store.YES, analyze = Analyze.NO)
   private int id;

   @Field(store = Store.YES, analyze = Analyze.NO)
   private String description;

   @Field(store = Store.YES, analyze = Analyze.NO)
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
      return "EmbeddedAccount{" +
            "id=" + id +
            ", description='" + description + '\'' +
            ", creationDate='" + creationDate + '\'' +
            "}";

   }
}
