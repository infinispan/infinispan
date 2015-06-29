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
    * A parameter's identifier. Each parameter must have a different id.
    *
    * DESIGN RATIONALES:
    * <ul>
    *    <il>Why does a metadata parameter need an id? It needs it mostly for
    *    lookup, but a numeric identifier is not enough here.
    *    </il>
    *    <li>Why is a numeric identifier not enough here? The main reason is
    *    that contrary to {@link Param}, users will be querying {@link MetaParam}
    *    instances via entry view instances. So, we need a typesafe way to
    *    retrieve {@link MetaParam} instances. Numeric integer-based lookup
    *    works well when the caller knows what to expect but users might not
    *    know that. By having {@link Id} typed with the {@link MetaParam} type,
    *    users can use this id() method to lookup the metadata parameter via
    *    {@link MetaParam.Lookup}. Also, metadata parameter instances can
    *    define static identifiers to lookup a metadata of that type.
    *    </li>
    *    <li>Please see org.infinispan.api.v8.impl.MetaParams test for examples
    *    on how metadata parameters are looked up.</li>
    * </ul>
    */
   Id<T> id();

   /**
    * Provides metadata parameter lookup capabilities using {@link Id} as
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
       * Returns a non-null metadata parameter implementation of the same type
       * as the {@link Id} being looked up, or throws {@link NoSuchElementException}
       * if no metadata parameter exists.
       *
       * @throws NoSuchElementException if no metadata parameter exists.
       *
       * @param <T> metadata parameter type
       */
      <T> T getMetaParam(MetaParam.Id<T> id) throws NoSuchElementException;

      /**
       * Optional metadata parameter implementation of the same type as the
       * {@link Id} being looked up. It'll return a non-empty metadata
       * parameter implementation instance when the metadata parameter with
       * the given ID exists, or empty when the metadata parameter is not present.
       *
       * @param <T> metadata parameter type
       */
      <T> Optional<T> findMetaParam(MetaParam.Id<T> id);
   }

   /**
    * Typed identifier for a {@link MetaParam} type, backed by a numeric
    * integer identifier.
    *
    * Each instance of a {@link MetaParam} type must provide the same {@link Id},
    * because only one instance of that {@link MetaParam} is expected amongst
    * the metadata parameters stored with the cache entry.
    *
    * If multiple instances of the metadata parameter are to be stored, a
    * {@link MetaParam} implementation must be constructed backed by the
    * collection of those metadata parameters.
    *
    * Two distinct {@link MetaParam} types are not allowed to have the same
    * {@link Id}, but they're free to choose to be backed by any positive or
    * negative number as long as no other metadata parameter is using it.
    *
    * @apiNote Why does Id have a type parameter if it's not used within it?
    * It helps with lookup. Each metadata parameter must provide an id with
    * the metadata parameter type, and hence when using the id to lookup,
    * we can deduce the returned type from the Id's type parameter.
    *
    * @param <T> Type of {@link MetaParam} for which it provides identifier.
    */
   final class Id<T> {
      private final int id;

      public Id(int id) {
         this.id = id;
      }

      @Override
      public String toString() {
         return "Id=" + id;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         Id<?> id1 = (Id<?>) o;

         return id == id1.id;
      }

      @Override
      public int hashCode() {
         return id;
      }
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
      public static final Id<Lifespan> ID = new Id<>(MetaParamIds.LIFESPAN_ID);
      private static final Lifespan DEFAULT = new Lifespan(-1);

      public Lifespan(long lifespan) {
         super(lifespan);
      }

      @Override
      public Id<Lifespan> id() {
         return ID;
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
      public static final Id<Created> ID = new Id<>(MetaParamIds.CREATED_ID);

      public Created(long created) {
         super(created);
      }

      @Override
      public Id<Created> id() {
         return ID;
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
      public static final Id<MaxIdle> ID = new Id<>(MetaParamIds.MAX_IDLE_ID);
      private static final MaxIdle DEFAULT = new MaxIdle(-1);

      public MaxIdle(long maxIdle) {
         super(maxIdle);
      }

      @Override
      public Id<MaxIdle> id() {
         return ID;
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
      public static final Id<LastUsed> ID = new Id<>(MetaParamIds.LAST_USED_ID);

      public LastUsed(long lastUsed) {
         super(lastUsed);
      }

      @Override
      public Id<LastUsed> id() {
         return ID;
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
      public static <V> Id<EntryVersionParam<V>> ID() {
         return new Id<>(MetaParamIds.ENTRY_VERSION_ID);
      }

      private final EntryVersion<V> entryVersion;

      public EntryVersionParam(EntryVersion<V> entryVersion) {
         this.entryVersion = entryVersion;
      }

      @Override
      public Id<EntryVersionParam<V>> id() {
         return ID();
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
