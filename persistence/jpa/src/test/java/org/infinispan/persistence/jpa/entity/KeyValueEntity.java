package org.infinispan.persistence.jpa.entity;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Entity
public class KeyValueEntity {
   @Id
   private String k; // key is reserved word in SQL

   @Basic
   private String v; // value is reserved word in h2 SQL

   public KeyValueEntity() {
   }

   public KeyValueEntity(String key, String value) {
      this.k = key;
      this.v = value;
   }

   @ProtoField(1)
   public String getK() {
      return k;
   }

   public void setK(String k) {
      this.k = k;
   }

   @ProtoField(2)
   public String getValue() {
      return v;
   }

   public void setValue(String value) {
      this.v = value;
   }

   @Override
   public String toString() {
      return String.format("{key=%s, value=%s}", k, v);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      KeyValueEntity that = (KeyValueEntity) o;

      if (k != null ? !k.equals(that.k) : that.k != null) return false;
      return v != null ? v.equals(that.v) : that.v == null;
   }

   @Override
   public int hashCode() {
      int result = k != null ? k.hashCode() : 0;
      result = 31 * result + (v != null ? v.hashCode() : 0);
      return result;
   }
}
