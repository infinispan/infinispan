package org.infinispan.functional;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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
       * The instance of the key must not be mutated.
       */
      K key();

      /**
       * Returns a non-null value if the key has a value associated with it or
       * throws {@link NoSuchElementException} if no value is associated with
       * the entry.
       *
       * <p>The value instance is read-only and must not be mutated. If the function
       * accessing this value is about to update the entry, it has to create
       * a defensive copy (or completely new instance) and store it using
       * {@link WriteEntryView#set(Object, MetaParam.Writable[])}.
       *
       * @throws NoSuchElementException if no value is associated with the key.
       */
      V get() throws NoSuchElementException;

      /**
       * Optional value. It'll return a non-empty value when the value is present,
       * and empty when the value is not present.
       *
       * <p>The value instance is read-only and must not be mutated. If the function
       * accessing this value is about to update the entry, it has to create
       * a defensive copy (or completely new instance) and store it using
       * {@link WriteEntryView#set(Object, MetaParam.Writable[])}.
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
   public interface ReadWriteEntryView<K, V> extends ReadEntryView<K, V>, WriteEntryView<V> {
      /**
       * Sets this value to the result of <code>updateFunction</code> applied
       * to the current value, or <code>null</code> if absent.
       * If <code>updateFunction</code> returns <code>null</code>, the mapping
       * is removed.
       *
       * @param updateFunction
       * @return <code>null</code>
       * @since 9.1
       */
      default Void update(Function<? super V, ? extends V> updateFunction) {
         return set(updateFunction.apply(find().orElse(null)));
      }

      /**
       * Sets this value to the result of <code>computeFunction</code>
       * applied to the current value, or <code>null</code> if absent.
       * If <code>computeFunction</code> returns <code>null</code>, the mapping
       * is removed.
       *
       * @param computeFunction
       * @return Value returned by the <code>computeFunction</code>
       * @since 9.1
       */
      default V compute(Function<? super V, ? extends V> computeFunction) {
         V newValue = computeFunction.apply(find().orElse(null));
         set(newValue);
         return newValue;
      }

      /**
       * Set this value to value provided by the <code>supplier</code> if there
       * is no current mapping.
       *
       * @param supplier
       * @return <code>null</code>
       * @since 9.1
       */
      default Void setIfAbsent(Supplier<? extends V> supplier) {
         if (!find().isPresent()) {
            set(supplier.get());
         }
         return null;
      }

      /**
       * Sets this value to the result of <code>updateFunction</code> applied
       * to the current value. If there is no mapping the function is not
       * executed and the value is not set.
       * If <code>updateFunction</code> returns <code>null</code>, the mapping
       * is removed.
       *
       * @param updateFunction
       * @return <code>null</code>
       * @since 9.1
       */
      default Void updateIfPresent(Function<? super V, ? extends V> updateFunction) {
         find().ifPresent(value -> set(updateFunction.apply(value)));
         return null;
      }

      /**
       * Sets this value to the result of <code>computeFunction</code> applied
       * to the current value. If there is no mapping the function is not
       * executed and the value is not set.
       * If <code>computeFunction</code> returns <code>null</code>, the mapping
       * is removed.
       *
       * @param computeFunction
       * @return Value returned by the <code>computeFunction</code> or
       *         <code>null</code> if there's no current mapping.
       * @since 9.1
       */
      default V computeIfPresent(Function<? super V, ? extends V> computeFunction) {
         if (find().isPresent()) {
            V newValue = computeFunction.apply(get());
            set(newValue);
            return newValue;
         }
         return null;
      }
   }

}
