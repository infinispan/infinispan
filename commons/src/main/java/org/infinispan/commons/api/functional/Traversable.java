package org.infinispan.commons.api.functional;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableSpliterator;

import java.util.Comparator;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Unsorted traversable stream. Design is inspired on {@link java.util.stream.Stream}.
 * Some functions that would be typically found in {@link java.util.stream.Stream}
 * are not included here:
 *
 * <ul>
 *    <li>{@link java.util.stream.Stream#distinct()}: This operation would be
 *    extremely costly in a distributed environment as all data up to that
 *    point of the traversable stream must be read by a single node.
 *    Due to the memory and performance constraints this method wouldn't be supported.
 *    </li>
 *    <li>{@link java.util.stream.Stream#sorted()}: Providing a sorted
 *    traversable stream in a distributed environment is an expensive operation.
 *    </li>
 *    <li>{@link java.util.stream.Stream#limit(long)}: In a distributed
 *    environment this would require sequential requests for each node to
 *    verify that we haven't reached the limit yet. This could be optimized
 *    depending on the terminator. This operation makes most sense when
 *    combined with sorted streams.
 *    </li>
 *    <li>{@link java.util.stream.Stream#sorted()}: Operation that makes
 *    most sense when combined with sorting.
 *    </li>
 *    <li>{@link java.util.stream.Stream#forEachOrdered(Consumer)}:
 *    Sorting related.
 *    </li>
 *    <li>{@link java.util.stream.Stream#toArray()}: Creating an array
 *    requires size to be computed first, and then populate it, making it a
 *    potentially expensive operation in a distributed operation. Also,
 *    there are other ways to achieve this, such as mapping to a variable
 *    length collection and then narrowing it down to an array?
 *    </li>
 *    <li>{@link java.util.stream.Stream#collect(java.util.stream.Collector)}:
 *    This method is designed to work with {@link java.util.stream.Collectors},
 *    whose static methods are used as helpers functions, but these are not
 *    Serializable, and we'd need to investigate whether we can provide
 *    marshallable versions. Also, {@link java.util.stream.Collectors} brings
 *    in a lot of baggage. Instead, {@link #collect(Supplier, BiConsumer, BiConsumer)}
 *    should be used.
 *    </li>
 *    <li>{@link java.util.stream.Stream#findFirst()}: The only difference
 *    with {@link #findAny()} is ordering, so since we do not want to enter
 *    the ordering business, we leave it out.</li>
 * </ul>
 *
 * @param <T>
 * @since 8.0
 */
public interface Traversable<T> extends AutoCloseable {

   // TODO: In distributed environments, where the lambdas passed are executed could matter a lot:
   // For example, for filtering, the predicate would be better run directly
   // in the source of data, rather than bring all data and filter it locally.
   // If the default would to run lambdas at the data source, there's a couple
   // of things considering:
   //    1. How to marshall a predicate without forcing the predicate to be Serializable.
   //       SerializedLambda exposes how serialization works, we'd need to apply a
   //       similar thing caching as much as we can.
   //    2. Have a way to tweak lambda executions to happen locally instead of
   //       at data source. This would be useful for operations such as
   //       peek() and for each. This could easily be done with a new Param.
   //       This option is also handy for situations where the lambda captures
   //       objects that simply cannot be marshalled.

   /**
    * Closeable iterator for manually iterating over the contents of the
    * traversable. The iterator also helps with determining when the end of
    * the traversable has been reached, by checking {@link CloseableIterator#hasNext()}
    */
   CloseableIterator<T> iterator();

   /**
    * Closeable spliterator for manually iterating over the contents of the
    * traversable. The iterator also helps with determining when the end of
    * the traversable has been reached, by checking {@link CloseableIterator#hasNext()}
    */
   CloseableSpliterator<T> spliterator();

   /**
    * Return a traversable containing elements matching the given predicate.
    */
   Traversable<T> filter(Predicate<? super T> p);

   /**
    * Returns a traversable containing the results of applying the given
    * function over the elements of the traversable.
    */
   <R> Traversable<R> map(Function<? super T, ? extends R> f);

   /**
    * Returns a traversable containing the results of replacing each element of
    * this traversable with the contents of a traverable produced by applying
    * the provided function to each element.
    *
    * From a functional map perspective, this operation is particularly handy
    * when the values are collections.
    */
   <R> Traversable<R> flatMap(Function<? super T, ? extends Traversable<? extends R>> f);

   /**
    * Applies an operation to all elements of this traversable.
    */
   void forEach(Consumer<? super T> c);

   /**
    * Applies a binary folding operation to a start value and all elements of
    * this traversable.
    */
   T reduce(T z, BinaryOperator<T> folder);

   /**
    * Applies a binary folding operation to all elements of this traversable,
    * and wraps the result in an optional. If the traversable is empty, it
    * returns an empty optional.
    */
   Optional<T> reduce(BinaryOperator<T> folder);

   /**
    * Applies a binary folding operation to a start value and the result
    * of each element having a mapping function applied.
    *
    * @apiNote This is a map and reduce in one go, which could potentially be
    * done more efficiently than if a map is executed and then reduce, so leave it in.
    */
   <U> U reduce(U z, BiFunction<U, ? super T, U> mapper, BinaryOperator<U> folder);

   /**
    * Transforms the traversable into a result container, first constructed
    * with the given supplier, and then accumulating elements over it with the
    * given consumer. The combiner can be used to combine accumulated results
    * executed in paralell or coming from different nodes in a distributed
    * environment.
    *
    * In distributed environments where some keys are remote, the
    * {@link Supplier} and {@link BiConsumer} instances passed in are sent to
    * other nodes and hence they need to be marshallable. If the collect
    * operation can be defined using the helper methods in
    * {@link java.util.stream.Collectors}, it is recommended that those are
    * used, which can easily be made marshalled using the
    * org.infinispan.stream.CacheCollectors#serializableCollector method.
    */
   <R> R collect(Supplier<R> s, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner);

   /**
    * Transforms the traversable into a result container using a
    * {@code Collector}.
    *
    * In distributed environments where some keys are remote, the
    * {@link Collector} instance passed in is sent other nodes and hence it
    * needs to be marshallable. This can easily be made achieved using the
    * org.infinispan.stream.CacheCollectors#serializableCollector method.
    */
   <R, A> R collect(Collector<? super T, A, R> collector);

   /**
    * Returns an optional containing the minimum element of this traversable
    * based on the comparator passed in. If the traversable is empty,
    * it returns an empty optional.
    */
   default Optional<T> min(Comparator<? super T> comparator) {
      return reduce(BinaryOperator.minBy(comparator));
   }

   /**
    * Returns an optional containing the maximum element of this traversable
    * based on the comparator passed in. If the traversable is empty,
    * it returns an empty optional.
    */
   default Optional<T> max(Comparator<? super T> comparator) {
      return reduce(BinaryOperator.maxBy(comparator));
   }

   /**
    * Return the number of elements in the traversable.
    */
   long count();

   /**
    * Returns whether any elements of this traversable match the provided predicate.
    *
    * @apiNote An important reason to keep this method is the fact as opposed
    * to a reduction which must evaluate all elements in the traversable, this
    * method could stop as soon as it has found an element that matches.
    */
   boolean anyMatch(Predicate<? super T> p);

   /**
    * Returns whether all elements of this traversable match the provided predicate.
    *
    * @apiNote An important reason to keep this method is the fact as opposed
    * to a reduction which must evaluate all elements in the traversable, this
    * method could stop as soon as it has found an element that does not match
    * the predicate.
    */
   boolean allMatch(Predicate<? super T> p);

   /**
    * Returns whether no elements of this traversable match the provided predicate.
    *
    * @apiNote An important reason to keep this method is the fact as opposed
    * to a reduction which must evaluate all elements in the traversable, this
    * method could stop as soon as it has found an element that does matches
    * the predicate.
    */
   boolean noneMatch(Predicate<? super T> predicate);

   /**
    * Returns an optional containing an element of the traversable,
    * or an empty optional if empty.
    */
   Optional<T> findAny();

   /**
    * Close traversable and release any resources held.
    */
   @Override
   void close();

}
