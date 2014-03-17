package org.infinispan.persistence.jpa;

import org.infinispan.commons.io.ByteBuffer;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

/**
 * Entity which should hold serialized metadata
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Entity
@Table(name = "__ispn_metadata__")
class MetadataEntity {
   public static final String EXPIRATION = "expiration";

   @Id
   // Some DBs require primary columns to have specified length, 767 is max length for InnoDB
   @Column(columnDefinition = "VARBINARY(767)", length = 767)
   public byte[] name;
   @Column(length = 65535)
   public byte[] metadata;
   @Column(name = EXPIRATION)
   public long expiration; // to simplify query for expired entries
   @Version
   public int version;

   public MetadataEntity() {
   }

   public MetadataEntity(ByteBuffer key, ByteBuffer metadata, long expiration) {
      this.name = key.getBuf();
      if (metadata != null) {
         this.metadata = metadata.getBuf();
      }
      this.expiration = expiration < 0 ? Long.MAX_VALUE : expiration;
   }

   public byte[] getMetadata() {
      return metadata;
   }

   public boolean hasBytes() {
      return metadata != null;
   }

}
