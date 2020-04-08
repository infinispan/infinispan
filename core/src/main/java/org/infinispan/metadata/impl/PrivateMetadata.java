package org.infinispan.metadata.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

import net.jcip.annotations.Immutable;

/**
 * A class to store internal metadata.
 * <p>
 * This class should not be exposed to users.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Immutable
@ProtoTypeId(ProtoStreamTypeIds.PRIVATE_METADATA)
public final class PrivateMetadata {

   /**
    * A cached empty {@link PrivateMetadata}.
    */
   private static final PrivateMetadata EMPTY = new PrivateMetadata();

   //The following fields will be added in later PRs:
   //IracVersion => used by IRAC (async xsite new algorithm)
   //SimpleClusteredVersion => used by Optimistic Transactions

   /**
    * @return An empty instance of {@link PrivateMetadata}, i.e., without any metadata stored.
    */
   public static PrivateMetadata empty() {
      return EMPTY;
   }

   /**
    * Returns a {@link Builder} with the metadata stored by {@code metadata}.
    * <p>
    * If {@code metadata} is {@code null}, an empty {@link Builder} instance is created.
    *
    * @param metadata The {@link PrivateMetadata} to copy from.
    * @return The {@link Builder} instance.
    */
   public static Builder getBuilder(PrivateMetadata metadata) {
      return metadata == null ? new Builder() : metadata.builder();
   }

   /**
    * Factory used by protostream.
    * <p>
    * It allows to do some logic before instantiate a new instance.
    */
   @ProtoFactory
   static PrivateMetadata protostreamFactory(/*add fields later*/) {
      return EMPTY;
   }

   /**
    * @return A {@link Builder} pre-filled with the data stored in this instance.
    */
   public Builder builder() {
      return new Builder();
   }

   /**
    * @return {@code true} if not metadata is stored in this instance.
    */
   public boolean isEmpty() {
      return true;
   }

   public static class Builder {

      public Builder() {
      }

      /**
       * @return A new instance of {@link PrivateMetadata}.
       */
      public PrivateMetadata build() {
         return EMPTY;
      }

   }


}
