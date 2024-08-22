package org.infinispan.server.resp.meta;

/**
 * Stores RESP related metadata and statistics.
 *
 * @author José Bolina
 * @since 15.0
 */
public final class MetadataRepository {

   private final ClientMetadata client;

   public MetadataRepository() {
      this.client = new ClientMetadata();
   }

   /**
    * Acquire the handler to read and writes metadata for client connections.
    *
    * @return The handler to manage client metadata.
    */
   public ClientMetadata client() {
      return client;
   }
}
