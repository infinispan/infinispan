package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import java.io.Serializable;
import java.util.Date;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.DateBridge;
import org.hibernate.search.annotations.EncodingType;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Resolution;
import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.query.dsl.embedded.testdomain.Account;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Indexed
public class AccountHS implements Account, Serializable, ExternalPojo {

   @Field(store = Store.YES, analyze = Analyze.NO)
   @SortableField
   private int id;

   @Field(store = Store.YES, analyze = Analyze.NO)
   @SortableField
   private String description;

   @Field(store = Store.YES, analyze = Analyze.NO)
   @DateBridge(encoding=EncodingType.STRING, resolution=Resolution.MILLISECOND)
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
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AccountHS other = (AccountHS) o;

      if (id != other.id) return false;
      if (creationDate != null ? !creationDate.equals(other.creationDate) : other.creationDate != null)
         return false;
      if (description != null ? !description.equals(other.description) : other.description != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = id;
      result = 31 * result + (description != null ? description.hashCode() : 0);
      result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "AccountHS{" +
            "id=" + id +
            ", description='" + description + '\'' +
            ", creationDate='" + creationDate + '\'' +
            '}';
   }
}
