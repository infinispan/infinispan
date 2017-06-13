package org.infinispan.functional;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.commons.util.Experimental;

/**
 * Entry views expose cached entry information to the user. Depending on the
 * type of entry view, different operations are available. Currently, three
 * type of entry views are supported:
 *
 * <ul>
 *    <il>{@link ReadEntryView}: read-only entry view</il>
 *    <il>{@link WriteEntryView}: write-only entry view</il>
 *    <il>{@link ReadWriteEntryView}: read-write entry view</il>
 * </ul>
 *
 * @since 8.0
 */
@Experimental
public final class EntryView {

   private EntryView() {
      // Cannot be instantiated, it's just a holder class
   }

   /**
    * Expose read-only information about a cache entry potentially associated
    * with a key in the functional map. Typically, if the key is associated
    * with a cache entry, that information will include value and optional
    * {@link MetaParam} information.
    *
    * <p>It exposes both {@link #get()} and {@link #find()} methods for
    * convenience. If the caller knows for sure that the value will be
    * present, {@link #get()} offers the convenience of retrieving the value
    * directly without having to get an {@link Optional} first. As a result
    * of this, {@link #get()} throws {@link NoSuchElementException} if
    * there's no value associated with the entry. If the caller is unsure
    * of whether the value is present, {@link #find()} should be used.
    * This approach avoids the user having to do null checks.
    *
    * @since 8.0
    */
   @Experimental
   public interface ReadEntryView<K, V> extends MetaParam.Lookup {
      /**
       * Key of the read-only entry view. Guaranteed to return a non-null value.
       */
      K key();

      /**
       * Returns a non-null value if the key has a value associated with it or
       * throws {@link NoSuchElementException} if no value is associated with
       * the.
       *
       * @throws NoSuchElementException if no value is associated with the key.
       */
      V get() throws NoSuchElementException;

      /**
       * Optional value. It'll return a non-empty value when the value is present,
       * and empty when the value is not present.
       */
      Optional<V> find();
   }

   /**
    * Expose a write-only facade for a cache entry potentially associated with a key
    * in the functional map which allows the cache entry to be written with
    * new value and/or new metadata parameters.
    *
    * @since 8.0
    */
   @Experimental
   public interface WriteEntryView<V> {
      /**
       * Set this value along with optional metadata parameters.
       *
       * <p>This method returns {@link Void} instead of 'void' to avoid
       * having to add overloaded methods in functional map that take
       * {@link Consumer} instead of {@link Function}. This is an
       * unfortunate side effect of the Java language itself which does
       * not consider 'void' to be an {@link Object}.
       */
      Void set(V value, MetaParam.Writable... metas);

      /**
       * Removes the value and any metadata parameters associated with it.
       *
       * <p>This method returns {@link Void} instead of 'void' to avoid
       * having to add overloaded methods in functional map that take
       * {@link Consumer} instead of {@link Function}. This is an
       * unfortunate side effect of the Java language itself which does
       * not consider 'void' to be an {@link Object}.
       */
      Void remove();
   }

   /**
    * Expose information about a cache entry potentially associated with a key
    * in the functional map, and allows that cache entry to be written with
    * new value and/or new metadata parameters.
    *
    * @since 8.0
    */
   @Experimental
   public interface ReadWriteEntryView<K, V> extends ReadEntryView<K, V>, WriteEntryView<V> {}

}
