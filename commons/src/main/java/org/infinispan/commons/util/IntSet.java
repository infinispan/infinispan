package org.infinispan.commons.util;

import java.util.Iterator;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.IntConsumer;
import java.util.function.IntPredicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A set that represents primitive ints. This interface describes methods that can be used without having to box an int.
 * This set does not support negative numbers.
 * @author wburns
 * @since 9.2
 */
public interface IntSet extends Set<Integer> {

   /**
    * Adds the given int to this set and returns {@code true} if it was set or {@code false} if it was already present
    * @param i the int value to add
    * @return whether this int was already present
    */
   boolean add(int i);

   /**
    * Adds or sets the int without returning whether it was previously set
    * @param i the value to make sure is in the set
    */
   void set(int i);

   /**
    * Removes, if present, the int from the set and returns if it was present or not
    * @param i the int to remove
    * @return whether the int was present in the set before it was removed
    */
   boolean remove(int i);

   /**
    * Whether this set contains the given int
    * @param i the int to check
    * @return if the set contains the int
    */
   boolean contains(int i);

   /**
    * Adds all ints from the provided set into this one
    * @param set the set of ints to add
    * @return if this set has a new int in it
    */
   boolean addAll(IntSet set);

   /**
    * Whether this set contains all ints in the given IntSet
    * @param set the set to check if all are present
    * @return if the set contains all the ints
    */
   boolean containsAll(IntSet set);

   /**
    * Removes all ints from this IntSet that are in the provided IntSet
    * @param set the ints to remove from this IntSet
    * @return if this set removed any ints
    */
   boolean removeAll(IntSet set);

   /**
    * Modifies this set to remove all ints that are not present in the provided IntSet
    * @param c the ints this set should kep
    * @return if this set removed any ints
    */
   boolean retainAll(IntSet c);

   /**
    * A primtive iterator that allows iteration over the int values. This iterator supports removal if the set is
    * modifiable.
    * @return the iterator
    */
   PrimitiveIterator.OfInt iterator();

   /**
    * A stream of ints representing the data in this set
    * @return the stream
    */
   IntStream intStream();

   @Override
   default Stream<Integer> stream() {
      return Set.super.stream();
   }

   /**
    * Performs the given action for each int of the {@code IntSet}
    * until all elements have been processed or the action throws an
    * exception.  Unless otherwise specified by the implementing class,
    * actions are performed in the order of iteration (if an iteration order
    * is specified).  Exceptions thrown by the action are relayed to the
    * caller.
    *
    * @implSpec
    * <p>The default implementation behaves as if:
    * <pre>{@code
    *     PrimitiveIterator.OfInt iterator = iterator();
    *     while (iterator.hasNext()) {
    *        action.accept(iterator.nextInt());
    *     }
    * }</pre>
    *
    * @param action The action to be performed for each element
    * @throws NullPointerException if the specified action is null
    * @since 9.3
    */
   default void forEach(IntConsumer action) {
      Objects.requireNonNull(action);
      PrimitiveIterator.OfInt iterator = iterator();
      while (iterator.hasNext()) {
         action.accept(iterator.nextInt());
      }
   }

   /**
    * Creates a {@code Spliterator.OfInt} over the ints in this set.
    *
    * <p>The {@code Spliterator.OfInt} reports {@link Spliterator#DISTINCT}.
    * Implementations should document the reporting of additional
    * characteristic values.
    *
    * @implSpec
    * The default implementation creates a
    * <em><a href="Spliterator.html#binding">late-binding</a></em> spliterator
    * from the set's {@code Iterator}.  The spliterator inherits the
    * <em>fail-fast</em> properties of the set's iterator.
    * <p>
    * The created {@code Spliterator.OfInt} additionally reports
    * {@link Spliterator#SIZED}.
    *
    * @implNote
    * The created {@code Spliterator.OfInt} additionally reports
    * {@link Spliterator#SUBSIZED}.
    *
    * @return a {@code Spliterator.OfInt} over the ints in this set
    * @since 9.3
    */
   default Spliterator.OfInt intSpliterator() {
      return Spliterators.spliterator(iterator(), size(), Spliterator.DISTINCT);
   }

   /**
    * Removes all of the ints of this set that satisfy the given
    * predicate.  Errors or runtime exceptions thrown during iteration or by
    * the predicate are relayed to the caller.
    *
    * @implSpec
    * The default implementation traverses all elements of the collection using
    * its {@link #iterator}.  Each matching element is removed using
    * {@link Iterator#remove()}.  If the collection's iterator does not
    * support removal then an {@code UnsupportedOperationException} will be
    * thrown on the first matching element.
    *
    * @param filter a predicate which returns {@code true} for ints to be
    *        removed
    * @return {@code true} if any ints were removed
    * @throws NullPointerException if the specified filter is null
    * @throws UnsupportedOperationException if elements cannot be removed
    *         from this collection.  Implementations may throw this exception if a
    *         matching element cannot be removed or if, in general, removal is not
    *         supported.
    * @since 9.3
    */
   default boolean removeIf(IntPredicate filter) {
      Objects.requireNonNull(filter);
      boolean removed = false;
      PrimitiveIterator.OfInt iterator = iterator();
      while (iterator.hasNext()) {
         if (filter.test(iterator.nextInt())) {
            iterator.remove();
            removed = true;
         }
      }
      return removed;
   }

   /**
    * Returns an array containing all of the elements in this set.
    * If this set makes any guarantees as to what order its elements
    * are returned by its iterator, this method must return the
    * elements in the same order.
    * @return this int set as an array
    * @since 9.3
    */
   default int[] toIntArray() {
      int[] array = new int[size()];
      PrimitiveIterator.OfInt iter = iterator();
      int i = 0;
      while (iter.hasNext()) {
         array[i] = iter.next();
      }
      return array;
   }
}
