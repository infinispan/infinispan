package org.infinispan.functional;

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

import org.infinispan.commons.util.Experimental;

/**
 * Unsorted traversable stream for sequential and aggregating operations.
 *
 * <p>Traversable contains two type of operations:
 * <ol>
 *    <li>Intermediate operations which transform a traversable, into another,
 *    e.g. {@link #filter(Predicate)}.
 *    </li>
 *    <li>Terminal operations which produce a side effect, e.g. {@link #forEach(Consumer)}.
 *    Once a terminal operation is completed, the resources taken by the
 *    traversable are released.
 *    </li>
 * </ol>
 *
 * <p>Traversable cannot be reused and hence is designed to be used only once
 * via its intermediate and terminal operations.
 *
 * <p>In distributed environments, unless individually specified, all lambdas
 * passed to methods are executed where data is located. For example, if
 * executing {@link #forEach(Consumer)}, the {@link Consumer} function is
 * executed wherever a particular key resides. To execute a for-each operation
 * where the side effects are executed locally, all the {@link Traversable}'s
 * data needs to be collected and iterated over manually.
 *
 * @param <T>
 * @since 8.0
 */
@Experimental
public interface Traversable<T> {

   /**
    * An intermediate operation that returns a traversable containing elements
    * matching the given predicate.
    */
   Traversable<T> filter(Predicate<? super T> p);

   /**
    * An intermediate operation that returns a traversable containing the
    * results of applying the given function over the elements of the
    * traversable.
    */
   <R> Traversable<R> map(Function<? super T, ? extends R> f);

   /**
    * An intermediate operation that returns a traversable containing the
    * results of replacing each element of this traversable with the contents
    * of a traversable produced by applying the provided function to each element.
    *
    * <p>From a functional map perspective, this operation is particularly handy
    * when the values are collections.
    */
   <R> Traversable<R> flatMap(Function<? super T, ? extends Traversable<? extends R>> f);

   /**
    * A terminal operation that applies an operation to all elements of this
    * traversable.
    */
   void forEach(Consumer<? super T> c);

   /**
    * A terminal operation that applies a binary folding operation to a start
    * value and all elements of this traversable.
    */
   T reduce(T z, BinaryOperator<T> folder);

   /**
    * A terminal operation that applies a binary folding operation to all
    * elements of this traversable, and wraps the result in an optional.
    * If the traversable is empty, it returns an empty optional.
    */
   Optional<T> reduce(BinaryOperator<T> folder);

   /**
    * A terminal operation that applies a binary folding operation to a start
    * value and the result of each element having a mapping function applied.
    *
    * <p>This is a combined map/reduce which could potentially be done more
    * efficiently than if a map is executed and then reduce.
    */
   <U> U reduce(U z, BiFunction<U, ? super T, U> mapper, BinaryOperator<U> folder);

   /**
    * A terminal operation that transforms the traversable into a result
    * container, first constructed with the given supplier, and then
    * accumulating elements over it with the given consumer.
    *
    * <p>The combiner can be used to combine accumulated results executed in
    * parallel or coming from different nodes in a distributed environment.
    *
    * <p>In distributed environments where some keys are remote, the
    * {@link Supplier} and {@link BiConsumer} instances passed in are sent to
    * other nodes and hence they need to be marshallable. If the collect
    * operation can be defined using the helper methods in
    * {@link java.util.stream.Collectors}, it is recommended that those are
    * used, which can easily be made marshalled using the
    * {@code org.infinispan.stream.CacheCollectors#serializableCollector} method.
    */
   <R> R collect(Supplier<R> s, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner);

   /**
    * A terminal operation that transforms the traversable into a result
    * container using a {@code Collector}.
    *
    * <p>In distributed environments where some keys are remote, the
    * {@link Collector} instance passed in is sent other nodes and hence it
    * needs to be marshallable. This can easily be made achieved using the
    * {@code org.infinispan.stream.CacheCollectors#serializableCollector} method.
    */
   <R, A> R collect(Collector<? super T, A, R> collector);

   /**
    * A terminal operation that returns an optional containing the minimum
    * element of this traversable based on the comparator passed in.
    * If the traversable is empty, it returns an empty optional.
    */
   default Optional<T> min(Comparator<? super T> comparator) {
      return reduce(BinaryOperator.minBy(comparator));
   }

   /**
    * A terminal operation that returns an optional containing the maximum
    * element of this traversable based on the comparator passed in.
    * If the traversable is empty, it returns an empty optional.
    */
   default Optional<T> max(Comparator<? super T> comparator) {
      return reduce(BinaryOperator.maxBy(comparator));
   }

   /**
    * A terminal operation that returns the number of elements in the traversable.
    */
   long count();

   /**
    * A terminal operation that returns whether any elements of this
    * traversable match the provided predicate.
    *
    * <p>An important reason to keep this method is the fact as opposed
    * to a reduction which must evaluate all elements in the traversable, this
    * method could stop as soon as it has found an element that matches.
    */
   boolean anyMatch(Predicate<? super T> p);

   /**
    * A terminal operation that returns whether all elements of this
    * traversable match the provided predicate.
    *
    * <p>An important reason to keep this method is the fact as opposed
    * to a reduction which must evaluate all elements in the traversable, this
    * method could stop as soon as it has found an element that does not match
    * the predicate.
    */
   boolean allMatch(Predicate<? super T> p);

   /**
    * A terminal operation that returns whether no elements of this
    * traversable match the provided predicate.
    *
    * <p>An important reason to keep this method is the fact as opposed
    * to a reduction which must evaluate all elements in the traversable, this
    * method could stop as soon as it has found an element that does matches
    * the predicate.
    */
   boolean noneMatch(Predicate<? super T> predicate);

   /**
    * A terminal operation that returns an optional containing an element of
    * the traversable, or an empty optional if empty.
    */
   Optional<T> findAny();

}
