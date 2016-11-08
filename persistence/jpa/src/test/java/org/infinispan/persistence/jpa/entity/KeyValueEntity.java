package org.infinispan.persistence.jpa.entity;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.infinispan.marshall.core.ExternalPojo;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Entity
public class KeyValueEntity implements Serializable, ExternalPojo {
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
   public String toString() {
      return String.format("{key=%s, value=%s}", k, value);
   }
}
