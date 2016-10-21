package org.infinispan.test.integration.as;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 * @since 7.0
 */
@Entity
public class KeyValueEntity implements Serializable {
   @Id
   private String k; // key is reserved word in SQL

   @Basic
   private String value;

   public KeyValueEntity() {
   }

   public KeyValueEntity(String key, String value) {
      this.k = key;
      this.value = value;
   }

   public String getK() {
      return k;
   }

   public void setK(String k) {
      this.k = k;
   }

   public String getValue() {
      return value;
   }

   public void setValue(String value) {
      this.value = value;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((k == null) ? 0 : k.hashCode());
      result = prime * result + ((value == null) ? 0 : value.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      KeyValueEntity other = (KeyValueEntity) obj;
      if (k == null) {
         if (other.k != null)
            return false;
      } else if (!k.equals(other.k))
         return false;
      if (value == null) {
         if (other.value != null)
            return false;
      } else if (!value.equals(other.value))
         return false;
      return true;
   }

   @Override
   public String toString() {
      return String.format("{key=%s, value=%s}", k, value);
   }
}
