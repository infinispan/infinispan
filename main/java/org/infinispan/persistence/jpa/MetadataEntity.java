package org.infinispan.persistence.jpa;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

import org.infinispan.commons.io.ByteBuffer;

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
   @Column(columnDefinition = "VARBINARY(255)") // Some DBs require primary columns to have specified length
   public byte[] name;
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
