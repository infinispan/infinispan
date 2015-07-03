package org.infinispan.commons.api.functional;

import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * An easily extensible metadata parameter that's stored along with the value
 * in the the functional map.
 *
 * Some metadata parameters can be provided by the user in which case they
 * need to implement {@link MetaParam.Writable}. Examples of writable metadata
 * parameters are version information, lifespan of the cached value...etc.
 *
 * Those metadata parameters not extending {@link MetaParam.Writable} are
 * created by internal logic and hence can only be queried, for example:
 * time when value was added into the functional map, or last time value
 * was accessed or modified.
 *
 * What makes {@link MetaParam} different from {@link Param} is that {@link MetaParam}
 * values are designed to be stored along with key/value pairs in the functional map,
 * to provide extra information. On the other hand, {@link Param} instances
 * merely act as ways to tweak how operations are executed, and their contents
 * are never stored permanently.
 *
 * DESIGN RATIONALES:
 * <ul>
 *    <il>This interface replaces Infinispan's Metadata interface providing
 *    a more flexible way to add new metadata parameters to be stored with
 *    the cached entries.
 *    </il>
 *    <il>Another benefit of the design is that it makes a clear separation
 *    between the metadata that can be provided by the user on per-entry basis,
 *    e.g. lifespan, maxIdle, version...etc, versus metadata that's produced
 *    by the internal logic that cannot be modified directly by the user, e.g.
 *    cache entry created time, last time cache entry was modified or accessed
 *    ...etc.
 *    </il>
 * </ul>
 *
 * @param <T> type of MetaParam instance, implementations should assign it to
 * the implementation's type.
 * @since 8.0
 */
public interface MetaParam<T> {

   /**
    * Provides metadata parameter lookup capabilities using {@link Class} as
    * lookup key.
    *
    * DESIGN RATIONALES:
    * <ul>
    *    <li>Why have both getMetaParam() and findMetaParam()? Convenience,
    *    for exactly the same reasons why {@link EntryView.ReadEntryView}
    *    exposes {@link EntryView.ReadEntryView#get()} and
    *    {@link EntryView.ReadEntryView#find()}
    *    </li>
    * </ul>
    */
   interface Lookup {
      /**
       * Returns a non-null metadata parameter implementation of the can be
       * assigned to the type {@link Class} being looked up, or throws
       * {@link NoSuchElementException} if no metadata parameter exists.
       *
       * @throws NoSuchElementException if no metadata parameter exists.
       *
       * @param <T> metadata parameter type
       */
      <T> T getMetaParam(Class<T> type) throws NoSuchElementException;

      /**
       * Returns a non-empty {@link Optional} instance containing a metadata
       * parameter instance that can be assigned to the type {@link Class}
       * passed in, or an empty {@link Optional} if no metadata can be assigned
       * to that type.
       *
       * @param <T> metadata parameter type
       */
      <T> Optional<T> findMetaParam(Class<T> type);
   }

   /**
    * Writable {@link MetaParam} instances are those that the user can provide
    * to be stored as part of the cache entry. RESTful HTTTP MIME metadata, version
    * information or lifespan are examples.
    *
    * @param <T> type of MetaParam instance, implementations should assign it to
    * the implementation's type.
    */
   interface Writable<T> extends MetaParam<T> {}

   /**
    * Writable metadata parameter representing a cached entry's millisecond lifespan.
    */
   final class Lifespan extends LongMetadata<Lifespan> implements Writable<Lifespan> {
      private static final Lifespan DEFAULT = new Lifespan(-1);

      public Lifespan(long lifespan) {
         super(lifespan);
      }

      @Override
      public String toString() {
         return "Lifespan=" + value;
      }

      public static Lifespan defaultValue() {
         return DEFAULT;
      }
   }

   /**
    * Read only metadata parameter representing a cached entry's created time
    * in milliseconds.
    */
   final class Created extends LongMetadata {
      public Created(long created) {
         super(created);
      }

      @Override
      public String toString() {
         return "Created=" + value;
      }
   }

   /**
    * Writable metadata parameter representing a cached entry's millisecond
    * max idle time.
    */
   final class MaxIdle extends LongMetadata<MaxIdle> implements Writable<MaxIdle> {
      private static final MaxIdle DEFAULT = new MaxIdle(-1);

      public MaxIdle(long maxIdle) {
         super(maxIdle);
      }

      @Override
      public String toString() {
         return "MaxIdle=" + value;
      }

      public static MaxIdle defaultValue() {
         return DEFAULT;
      }
   }

   /**
    * Read only metadata parameter representing a cached entry's last used time
    * in milliseconds.
    */
   final class LastUsed extends LongMetadata {
      public LastUsed(long lastUsed) {
         super(lastUsed);
      }

      @Override
      public String toString() {
         return "LastUsed=" + value;
      }
   }

   /**
    * Writable metadata parameter representing a cached entry's version.
    */
   final class EntryVersionParam<V> implements Writable<EntryVersionParam<V>> {
      @SuppressWarnings("unchecked")
      public static <T> T getType() {
         return (T) EntryVersionParam.class;
      }

      private final EntryVersion<V> entryVersion;

      public EntryVersionParam(EntryVersion<V> entryVersion) {
         this.entryVersion = entryVersion;
      }

      public EntryVersion<V> get() {
         return entryVersion;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         EntryVersionParam<?> that = (EntryVersionParam<?>) o;

         return entryVersion.equals(that.entryVersion);
      }

      @Override
      public int hashCode() {
         return entryVersion.hashCode();
      }

      @Override
      public String toString() {
         return "MetaParam=" + entryVersion;
      }
   }

   abstract class LongMetadata<T> implements MetaParam<T> {
      protected final long value;

      public LongMetadata(long value) {
         this.value = value;
      }

      public Long get() {
         return value;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         LongMetadata longMeta = (LongMetadata) o;

         return value == longMeta.value;
      }

      @Override
      public int hashCode() {
         return (int) (value ^ (value >>> 32));
      }
   }

}
