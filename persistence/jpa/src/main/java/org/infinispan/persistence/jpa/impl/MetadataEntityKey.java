package org.infinispan.persistence.jpa.impl;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.persistence.Embeddable;

import org.infinispan.commons.util.Base64;
import org.infinispan.persistence.jpa.JpaStoreException;

/**
 * 
 * Embedded entity which serves as primary key for metadata. Bytes representing the key are hashed
 * via SHA-256.
 * 
 * @author vjuranek
 */
@Embeddable
public class MetadataEntityKey implements Serializable {

   private static final long serialVersionUID = 73757405630621L;
   private static final String DIGEST_ALG = "SHA-256";

   private String keySha;

   public MetadataEntityKey() {
   }

   public MetadataEntityKey(byte[] keyBytes) {
      keySha = getKeyBytesSha(keyBytes);
   }
   
   public String getKeySha() {
      return keySha;
   }

   public void setKeySha(String keySha) {
      this.keySha = keySha;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || !(o instanceof MetadataEntityKey))
         return false;
      return keySha.equals(((MetadataEntityKey) o).getKeySha());
   }

   @Override
   public int hashCode() {
      return keySha.hashCode();
   }
   
   public static String getKeyBytesSha(byte[] keyBytes) {
      String keyBytesSha;
      try {
         MessageDigest digest = MessageDigest.getInstance(DIGEST_ALG);
         byte[] sha = digest.digest(keyBytes);
         keyBytesSha = Base64.encodeBytes(sha);
      } catch(NoSuchAlgorithmException e) {
         throw new JpaStoreException("Failed to create SHA hash of metadata key", e);
      }
      return keyBytesSha;
   }

   @Override
   public String toString() {
      return "MetadataEntityKey{keySha='" + keySha + "'}";
   }
}
