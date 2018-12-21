package org.infinispan.persistence.jpa.impl;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Version;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.spi.MarshallableEntry;

/**
 * Entity which should hold serialized metadata
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Entity
@Table(name = "`__ispn_metadata__`")
public class MetadataEntity {
   public static final String EXPIRATION = "expiration";
   private static final String CREATED = "created";
   private static final String LAST_USED = "lastused";

   @EmbeddedId
   private MetadataEntityKey key;
   @Lob
   @Column(length = 65535)
   private byte[] keyBytes;
   @Lob
   @Column(length = 65535)
   private byte[] metadata;
   @Column(name = EXPIRATION)
   private long expiration; // to simplify query for expired entries
   @Column(name = CREATED)
   private long created;
   @Column(name = LAST_USED)
   private long lastUsed;
   @Version
   private int version;

   public MetadataEntity() {
   }

   public MetadataEntity(MarshallableEntry me) {
      this.keyBytes = MarshallUtil.toByteArray(me.getKeyBytes());
      this.key = new MetadataEntityKey(keyBytes);
      Metadata meta = me.getMetadata();
      if (meta != null) {
         this.metadata = MarshallUtil.toByteArray(me.getMetadataBytes());
      }
      this.expiration = (meta == null || me.expiryTime() < 0) ? Long.MAX_VALUE : me.expiryTime();
      this.created = me.created();
      this.lastUsed = me.lastUsed();
   }

   public MetadataEntityKey getKey() {
      return key;
   }

   public void setKey(MetadataEntityKey key) {
      this.key = key;
   }

   public byte[] getKeyBytes() {
      return keyBytes;
   }

   public void setKeyBytes(byte[] keyBytes) {
      this.keyBytes = keyBytes;
   }

   public byte[] getMetadata() {
      return metadata;
   }

   public void setMetadata(byte[] metadata) {
      this.metadata = metadata;
   }

   public long getExpiration() {
      return expiration;
   }

   public void setExpiration(long expiration) {
      this.expiration = expiration;
   }

   public long getCreated() {
      return created;
   }

   public void setCreated(long created) {
      this.created = created;
   }

   public long getLastUsed() {
      return lastUsed;
   }

   public void setLastUsed(long lastUsed) {
      this.lastUsed = lastUsed;
   }

   public int getVersion() {
      return version;
   }

   public void setVersion(int version) {
      this.version = version;
   }

   public boolean hasBytes() {
      return metadata != null;
   }

}
