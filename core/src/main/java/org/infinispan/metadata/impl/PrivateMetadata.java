package org.infinispan.metadata.impl;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
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
   private static final PrivateMetadata EMPTY = new PrivateMetadata(null, null);

   @ProtoField(1)
   final IracMetadata iracMetadata;

   private final IncrementableEntryVersion entryVersion;

   private PrivateMetadata(IracMetadata iracMetadata, IncrementableEntryVersion entryVersion) {
      this.iracMetadata = iracMetadata;
      this.entryVersion = entryVersion;
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
   static PrivateMetadata protoFactory(IracMetadata iracMetadata, NumericVersion numericVersion,
         SimpleClusteredVersion clusteredVersion) {
      IncrementableEntryVersion entryVersion = numericVersion == null ? clusteredVersion : numericVersion;
      return newInstance(iracMetadata, entryVersion);
   }

   private static PrivateMetadata newInstance(IracMetadata iracMetadata, IncrementableEntryVersion entryVersion) {
      return iracMetadata == null && entryVersion == null ? EMPTY : new PrivateMetadata(iracMetadata, entryVersion);
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
    * @return The {@link IncrementableEntryVersion} associated with the entry.
    */
   public IncrementableEntryVersion entryVersion() {
      return entryVersion;
   }

   /**
    * @return {@code true} if not metadata is stored in this instance.
    */
   public boolean isEmpty() {
      return iracMetadata == null && entryVersion == null;
   }

   @Override
   public String toString() {
      return "PrivateMetadata{" +
            "iracMetadata=" + iracMetadata +
            ", entryVersion=" + entryVersion +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }
      PrivateMetadata metadata = (PrivateMetadata) o;
      return Objects.equals(iracMetadata, metadata.iracMetadata) &&
            Objects.equals(entryVersion, metadata.entryVersion);
   }

   @Override
   public int hashCode() {
      return Objects.hash(iracMetadata, entryVersion);
   }

   @ProtoField(2)
   public NumericVersion getNumericVersion() {
      return entryVersion instanceof NumericVersion ? (NumericVersion) entryVersion : null;
   }

   @ProtoField(3)
   public SimpleClusteredVersion getClusteredVersion() {
      return entryVersion instanceof SimpleClusteredVersion ? (SimpleClusteredVersion) entryVersion : null;
   }

   public static class Builder {

      private IracMetadata iracMetadata;
      private IncrementableEntryVersion entryVersion;

      public Builder() {
      }

      private Builder(PrivateMetadata metadata) {
         this.iracMetadata = metadata.iracMetadata;
         this.entryVersion = metadata.entryVersion;
      }

      /**
       * @return A new instance of {@link PrivateMetadata}.
       */
      public PrivateMetadata build() {
         return newInstance(iracMetadata, entryVersion);
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

      /**
       * Sets the {@link IncrementableEntryVersion} to store.
       *
       * @param entryVersion The version to store.
       * @return This instance.
       */
      public Builder entryVersion(IncrementableEntryVersion entryVersion) {
         this.entryVersion = entryVersion;
         return this;
      }

   }


}
