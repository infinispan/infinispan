package org.infinispan.metadata.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
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
   private static final PrivateMetadata EMPTY = new PrivateMetadata(null);

   @ProtoField(number = 1)
   final IracMetadata iracMetadata;

   private PrivateMetadata(IracMetadata iracMetadata) {
      this.iracMetadata = iracMetadata;
   }

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
   static PrivateMetadata protoFactory(IracMetadata iracMetadata) {
      return iracMetadata == null ? EMPTY : new PrivateMetadata(iracMetadata);
   }

   /**
    * @return A {@link Builder} pre-filled with the data stored in this instance.
    */
   public Builder builder() {
      return new Builder(this);
   }

   /**
    * @return The {@link IracMetadata} stored. It can be {@code null}.
    */
   public IracMetadata iracMetadata() {
      return iracMetadata;
   }

   /**
    * @return {@code true} if not metadata is stored in this instance.
    */
   public boolean isEmpty() {
      return iracMetadata == null;
   }

   public static class Builder {

      private IracMetadata iracMetadata;

      public Builder() {
      }

      private Builder(PrivateMetadata metadata) {
         this.iracMetadata = metadata.iracMetadata;
      }

      /**
       * @return A new instance of {@link PrivateMetadata}.
       */
      public PrivateMetadata build() {
         return protoFactory(iracMetadata);
      }

      /**
       * Sets the {@link IracMetadata} to store.
       *
       * @param metadata The {@link IracMetadata} to store.
       * @return This instance.
       */
      public Builder iracMetadata(IracMetadata metadata) {
         this.iracMetadata = metadata;
         return this;
      }

   }


}
