package org.infinispan.persistence.jpa.entity;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.Id;
import java.io.Serializable;

/**
* @author Radim Vansa &lt;rvansa@redhat.com&gt;
*/
@Entity
public class KeyValueEntity implements Serializable {
   @Id
   public String k; // key is reserved word in SQL

   @Basic
   public String value;

   public KeyValueEntity() {
   }

   public KeyValueEntity(String key, String value) {
      this.k = key;
      this.value = value;
   }

   @Override
   public String toString() {
      return String.format("{key=%s, value=%s}", k, value);
   }
}
