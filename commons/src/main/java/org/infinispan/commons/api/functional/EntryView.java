package org.infinispan.commons.api.functional;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

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
    * DESIGN RATIONALES:
    * <ul>
    *    <li>Why does ReadEntryView expose both get() and find() methods for
    *    retrieving the value? Convenience. If the caller knows for sure
    *    that the value will be present, get() offers the convenience of
    *    retrieving the value directly without having to get an {@link Optional}
    *    first.
    *    </li>
    *    <li>Why have find() return {@link Optional}? Why not get rid of it and only have
    *    get() method return null? Because nulls are evil. If a value might not
    *    be there, the user can use {@link Optional} to find out if the value might
    *    be there and deal with non-present values in a more functional way.
    *    </li>
    *    <li>Why does get() throw NoSuchElementException? Because you should only
    *    use it if you know for sure that the value will be there. If unsure,
    *    use find(). We don't want to return null to avoid people doing null checks.
    *    </li>
    * </ul>
    */
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
    */
   public interface WriteEntryView<V> {
      /**
       * Set this value along with optional metadata parameters.
       *
       * DESIGN RATIONALE:
       * <ul>
       *    <li>It returns 'Void' instead of 'void' in order to avoid the need
       *    to add overloaded methods in functional map that take {@link Consumer}
       *    instead of {@link Function}. This is what of those annoying side
       *    effects of the java language, which didn't make `void` an Object.
       *    </li>
       * </ul>
       */
      Void set(V value, MetaParam.Writable... metas);

      /**
       * Removes the value and any metadata parameters associated with it.
       *
       * DESIGN RATIONALE:
       * <ul>
       *    <li>It returns 'Void' instead of 'void' in order to avoid the need
       *    to add overloaded methods in functional map that take {@link Consumer}
       *    instead of {@link Function}. This is what of those annoying side
       *    effects of the java language, which didn't make `void` an Object.
       *    </li>
       *    <li>Why not have a single `set(Optional<V>...)` operation that takes
       *    {@link Optional#empty()} instead of having a
       *    {@link #set(Object, MetaParam.Writable[])} and remove()?
       *    The two-method approach feels cleaner and less cumbersome than
       *    having to always pass in Optional to set().
       *    </li>
       * </ul>
       */
      Void remove();
   }

   /**
    * Expose information about a cache entry potentially associated with a key
    * in the functional map, and allows that cache entry to be written with
    * new value and/or new metadata parameters.
    */
   public interface ReadWriteEntryView<K, V> extends ReadEntryView<K, V>, WriteEntryView<V> {}

}
