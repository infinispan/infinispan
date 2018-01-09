package org.infinispan.functional;

import java.util.Optional;

import org.infinispan.commons.util.Experimental;
import org.infinispan.container.versioning.EntryVersion;

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
    * <p>When the {@link MetaParam} type is generic, e.g. {@link MetaEntryVersion},
    * passing the correct {@link Class} information so that the return of
    * {@link #findMetaParam} is of the expected type can be a bit tricky.
    * {@link MetaEntryVersion#type()} offers an easy way to retrieve the
    * expected {@link MetaParam} type from {@link #findMetaParam} at the
    * expense of some type safety:
    *
    * <pre>{@code
    *     Class<MetaEntryVersion<Long>> type = MetaEntryVersion.type();
    *     Optional<MetaEntryVersion<Long>> metaVersion =
    *          metaParamLookup.findMetaParam(type);
    * }</pre>
    *
    * In the future, the API might be adjusted to provide additional lookup
    * methods where this situation is improved. Also, if the {@link MetaParam}
    * type is not generic, e.g. {@link MetaLifespan}, the problem is avoided
    * altogether:
    *
    * <pre>{@code
    *     Optional<MetaLifespan<Long>> metaLifespan =
    *          metaParamLookup.findMetaParam(MetaLifespan.class);
    * }</pre>
    *
    * <p>A user that queries meta parameters can never assume that the
    * meta parameter will always exist because there are scenarios, such as
    * when compatibility mode is enabled, when meta parameters that are
    * assumed to be present are not due to the API multiplexing that occurs.
    * For example, when compatibility mode is enabled, the REST server can't
    * assume that MIME metadata will be present since data might have been
    * stored with embedded or remote (Hot Rod) API.
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
   final class MetaLifespan extends MetaLong implements Writable<Long> {
      private static final MetaLifespan DEFAULT = new MetaLifespan(-1);

      public MetaLifespan(long lifespan) {
         super(lifespan);
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
   final class MetaMaxIdle extends MetaLong implements Writable<Long> {
      private static final MetaMaxIdle DEFAULT = new MetaMaxIdle(-1);

      public MetaMaxIdle(long maxIdle) {
         super(maxIdle);
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
   class MetaEntryVersion implements Writable<EntryVersion> {
      private final EntryVersion entryVersion;

      public MetaEntryVersion(EntryVersion entryVersion) {
         this.entryVersion = entryVersion;
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

   /**
    * Non-writable parameter telling if the entry was loaded from a persistence tier
    * ({@link org.infinispan.persistence.spi.CacheLoader}) or not.
    * This information may be available only to write commands due to implementation reasons.
    */
   @Experimental
   final class MetaLoadedFromPersistence implements MetaParam<Boolean> {
      public static MetaLoadedFromPersistence LOADED = new MetaLoadedFromPersistence(true);
      public static MetaLoadedFromPersistence NOT_LOADED = new MetaLoadedFromPersistence(false);

      private boolean loaded;

      private MetaLoadedFromPersistence(boolean loaded) {
         this.loaded = loaded;
      }

      @Override
      public Boolean get() {
         return loaded;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         MetaLoadedFromPersistence that = (MetaLoadedFromPersistence) o;

         return loaded == that.loaded;
      }

      @Override
      public int hashCode() {
         return (loaded ? 1 : 0);
      }

      public static MetaLoadedFromPersistence of(boolean loaded) {
         return loaded ? LOADED : NOT_LOADED;
      }
   }

}
