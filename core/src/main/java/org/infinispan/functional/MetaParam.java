package org.infinispan.functional;

import java.util.Optional;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Experimental;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * An easily extensible metadata parameter that's stored along with the value
 * in the the functional map.
 *
 * <p>Some metadata parameters can be provided by the user in which case they
 * need to implement {@link MetaParam.Writable}. Examples of writable metadata
 * parameters are version information, lifespan of the cached value...etc.
 *
 * <p>Those metadata parameters not extending {@link MetaParam.Writable} are
 * created by internal logic and hence can only be queried, for example:
 * time when value was added into the functional map, or last time value
 * was accessed or modified.
 *
 * <p>What makes {@link MetaParam} different from {@link Param} is that {@link MetaParam}
 * values are designed to be stored along with key/value pairs in the functional map,
 * to provide extra information. On the other hand, {@link Param} instances
 * merely act as ways to tweak how operations are executed, and their contents
 * are never stored permanently.
 *
 * <p>This interface replaces Infinispan's Metadata interface providing
 * a more flexible way to add new metadata parameters to be stored with
 * the cached entries.
 *
 * <p>{@link MetaParam} design has been geared towards making a clear
 * separation between the metadata that can be provided by the user on
 * per-entry basis, e.g. lifespan, maxIdle, version...etc, as opposed to
 * metadata that's produced by the internal logic that cannot be modified
 * directly by the user, e.g. cache entry created time, last time cache entry
 * was modified or accessed ...etc.
 *
 * @param <T> type of MetaParam instance, implementations should assign it to
 * the implementation's type.
 * @since 8.0
 */
@Experimental
public interface MetaParam<T> {

   /**
    * Returns the value of the meta parameter.
    */
   T get();

   /**
    * Provides metadata parameter lookup capabilities using {@link Class} as
    * lookup key.
    *
    * <p>A user that queries meta parameters can never assume that the
    * meta parameter will always exist because some of them depends on the
    * cache usage.
    *
    * @since 8.0
    */
   @Experimental
   interface Lookup {
      /**
       * Returns a non-empty {@link Optional} instance containing a metadata
       * parameter instance that can be assigned to the type {@link Class}
       * passed in, or an empty {@link Optional} if no metadata can be assigned
       * to that type.
       *
       * @param <T> metadata parameter type
       */
      <T extends MetaParam> Optional<T> findMetaParam(Class<T> type);
   }

   /**
    * Writable {@link MetaParam} instances are those that the user can provide
    * to be stored as part of the cache entry. RESTful HTTTP MIME metadata, version
    * information or lifespan are examples.
    *
    * @param <T> type of MetaParam instance, implementations should assign it to
    * the implementation's type.
    * @since 8.0
    */
   @Experimental
   interface Writable<T> extends MetaParam<T> {}

   /**
    * Writable metadata parameter representing a cached entry's millisecond lifespan.
    *
    * @since 8.0
    */
   @Experimental
   @ProtoTypeId(ProtoStreamTypeIds.META_PARAMS_LIFESPAN)
   final class MetaLifespan extends MetaLong implements Writable<Long> {
      private static final MetaLifespan DEFAULT = new MetaLifespan(-1);

      @ProtoFactory
      public MetaLifespan(long lifespan) {
         super(lifespan);
      }

      @ProtoField(1)
      long getLifespan() {
         return value;
      }

      @Override
      public String toString() {
         return "MetaLifespan=" + value;
      }

      public static MetaLifespan defaultValue() {
         return DEFAULT;
      }
   }

   /**
    * Read only metadata parameter representing a cached entry's created time
    * in milliseconds.
    *
    * @since 8.0
    */
   @Experimental
   final class MetaCreated extends MetaLong {
      public MetaCreated(long created) {
         super(created);
      }

      @Override
      public String toString() {
         return "MetaCreated=" + value;
      }
   }

   /**
    * Writable metadata parameter representing a cached entry's millisecond
    * max idle time.
    *
    * @since 8.0
    */
   @Experimental
   @ProtoTypeId(ProtoStreamTypeIds.META_PARAMS_MAX_IDLE)
   final class MetaMaxIdle extends MetaLong implements Writable<Long> {
      private static final MetaMaxIdle DEFAULT = new MetaMaxIdle(-1);

      @ProtoFactory
      public MetaMaxIdle(long maxIdle) {
         super(maxIdle);
      }

      @ProtoField(1)
      long getMaxIdle() {
         return value;
      }

      @Override
      public String toString() {
         return "MetaMaxIdle=" + value;
      }

      public static MetaMaxIdle defaultValue() {
         return DEFAULT;
      }
   }

   /**
    * Read only metadata parameter representing a cached entry's last used time
    * in milliseconds.
    *
    * @since 8.0
    */
   @Experimental
   final class MetaLastUsed extends MetaLong {
      public MetaLastUsed(long lastUsed) {
         super(lastUsed);
      }

      @Override
      public String toString() {
         return "MetaLastUsed=" + value;
      }
   }

   /**
    * Writable metadata parameter representing a cached entry's generic version.
    *
    * @since 8.0
    */
   @Experimental
   @ProtoTypeId(ProtoStreamTypeIds.META_PARAMS_ENTRY_VERSION)
   class MetaEntryVersion implements Writable<EntryVersion> {
      private final EntryVersion entryVersion;

      public MetaEntryVersion(EntryVersion entryVersion) {
         this.entryVersion = entryVersion;
      }

      @ProtoFactory
      MetaEntryVersion(NumericVersion numericVersion, SimpleClusteredVersion clusteredVersion) {
         this.entryVersion = numericVersion != null ? numericVersion : clusteredVersion;
      }

      @ProtoField(1)
      NumericVersion getNumericVersion() {
         return entryVersion instanceof NumericVersion ? (NumericVersion) entryVersion : null;
      }

      @ProtoField(2)
      SimpleClusteredVersion getClusteredVersion() {
         return entryVersion instanceof SimpleClusteredVersion ? (SimpleClusteredVersion) entryVersion : null;
      }

      @Override
      public EntryVersion get() {
         return entryVersion;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         MetaEntryVersion that = (MetaEntryVersion) o;
         return entryVersion.equals(that.entryVersion);
      }

      @Override
      public int hashCode() {
         return entryVersion.hashCode();
      }

      @Override
      public String toString() {
         return "MetaEntryVersion=" + entryVersion;
      }
   }

   /**
    * Abstract class for numeric long-based metadata parameter instances.
    *
    * @since 8.0
    */
   @Experimental
   abstract class MetaLong implements MetaParam<Long> {
      protected final long value;

      public MetaLong(long value) {
         this.value = value;
      }

      public Long get() {
         return value;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         MetaLong longMeta = (MetaLong) o;

         return value == longMeta.value;
      }

      @Override
      public int hashCode() {
         return (int) (value ^ (value >>> 32));
      }
   }

   @Experimental
   abstract class MetaBoolean implements MetaParam<Boolean> {
      protected final boolean value;

      public MetaBoolean(boolean value) {
         this.value = value;
      }

      public Boolean get() {
         return value;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         MetaBoolean metaBoolean = (MetaBoolean) o;

         return value == metaBoolean.value;
      }

      @Override
      public int hashCode() {
         return value ? 1 : 0;
      }
   }

   /**
    * Non-writable parameter telling if the entry was loaded from a persistence tier
    * ({@link org.infinispan.persistence.spi.NonBlockingStore}) or not.
    * This information may be available only to write commands due to implementation reasons.
    */
   @Experimental
   final class MetaLoadedFromPersistence extends MetaBoolean {
      public static final MetaLoadedFromPersistence LOADED = new MetaLoadedFromPersistence(true);
      public static final MetaLoadedFromPersistence NOT_LOADED = new MetaLoadedFromPersistence(false);

      private MetaLoadedFromPersistence(boolean loaded) {
         super(loaded);
      }

      public static MetaLoadedFromPersistence of(boolean loaded) {
         return loaded ? LOADED : NOT_LOADED;
      }
   }

   /**
    * A parameter to tell if the creation timestamp should be updated for modified entries.
    * <p>
    * Created entries will always update its creation timestamp.
    */
   @Experimental
   final class MetaUpdateCreationTime extends MetaBoolean implements Writable<Boolean> {

      private static final MetaUpdateCreationTime UPDATE = new MetaUpdateCreationTime(true);
      private static final MetaUpdateCreationTime NOT_UPDATE = new MetaUpdateCreationTime(false);

      public MetaUpdateCreationTime(boolean value) {
         super(value);
      }

      public static MetaUpdateCreationTime of(boolean update) {
         return update ? UPDATE : NOT_UPDATE;
      }
   }

}
