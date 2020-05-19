package org.infinispan.persistence.spi;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

import javax.transaction.Transaction;

import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.IntSet;
import org.infinispan.persistence.support.BatchModification;
import org.infinispan.util.concurrent.CompletableFutures;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

/**
 * <p>
 * Implementations of this store must be thread safe if concurrent operations are performed on it. This should include
 * possibly invoking start or stop multiple times
 * <p>
 * Note that this method is Experimental and its methods may change slightly over time until it has matured.
 * @author William Burns
 * @since 11.0
 * @param <K> key value type
 * @param <V> value value type
 */
@Experimental
public interface NonBlockingStore<K, V> {

   enum Characteristic {
      /**
       * Whether this cache can be shared between multiple nodes. An example would be an external system, such as
       * a database.
       */
      SHAREABLE,
      /**
       * If this store only supports being read from.  Any write based operations will never be invoked on this store.
       */
      READ_ONLY,
      /**
       * If this store only supports being written to. Any read based operations will never be invoked on this store.
       */
      WRITE_ONLY,
      /**
       * If this store supports bulk read operations.
       * <p>
       * Stores that have this characteristic must override the {@link #publishKeys(IntSet, Predicate)},
       * {@link #publishEntries(IntSet, Predicate, boolean)}  and {@link #size(IntSet)} methods.
       */
      BULK_READ,
      /**
       * If this store supports being invoked in a transactional context with a prepare and commit or rollback phases.
       * <p>
       * Stores that have this characteristic must override the
       * {@link #prepareWithModifications(Transaction, BatchModification)}, {@link #commit(Transaction)} and
       * {@link #rollback(Transaction)} methods.
       */
      TRANSACTIONAL,
      /**
       * Whether this store supports being segmented. All methods in this SPI take as an argument a way to map a given
       * entry to a segment. A segment in Infinispan is an int that acts as a bucket for many keys. Many store
       * implementations may be able to store and load entries in a more performant way if they segment their data
       * accordingly.
       * <p>
       * If this store is not segmentable then invokers of this SPI are not required to calculate these segments before
       * invoking these methods and thus these methods may be invoked with any int value, null or equivalent. Please
       * see each method to see how they may be affected when this store is not segmentable.
       * <p>
       * Note that a store may also be configured at runtime to be segmented or not. If this store is configured to not
       * be segmented this store will be treated as if it does not have the SEGMENTABLE characteristic (causing possible
       * parameters to be null or invalid segment numbers). A store implementation may want to block this configuration
       * by throwing an exception in the {@link #start(InitializationContext)} method if it does not want to support this.
       * <p>
       * While it is possible that a SEGMENTABLE store can be configured as not segmented, a store that is not
       * SEGMENTABLE will never be allowed to be configured as segmented.
       * <p>
       * Stores that have this characteristic must override the {@link #addSegments(IntSet)} and
       * {@link #removeSegments(IntSet)} methods.
       */
      SEGMENTABLE,
      /**
       * If this store supports storing expiration metadata. Certain methods may or may not include expired entries.
       * <p>
       * Stores that have this characteristic must override the {@link #purgeExpired()} method.
       */
      EXPIRATION
   }

   /**
    * <p>
    * This method is guaranteed to not be invoked concurrently with other operations. This also means another method
    * will not be invoked on this store until after the returned Stage completes.
    * @param ctx initialization context used to initialize this store
    * @return a stage that when complete signals that this store has been successfully started
    */
   CompletionStage<Void> start(InitializationContext ctx);

   /**
    * <p>
    * This method is guaranteed to not be invoked concurrently with other operations. This also means another method
    * will not be invoked on this store until after the returned Stage completes.
    * @return a stage that when complete signals that this store has been stopped
    */
   CompletionStage<Void> stop();

   /**
    *
    * @return
    */
   default Set<Characteristic> characteristics() {
      return EnumSet.noneOf(Characteristic.class);
   }

   /**
    *
    * @return
    */
   default CompletionStage<Boolean> isAvailable() {
      return CompletableFutures.completedTrue();
   }

   /**
    *
    * @param segment
    * @param key
    * @return
    */
   CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key);

   /**
    *
    * @param segment
    * @param key
    * @return
    */
   default CompletionStage<Boolean> containsKey(int segment, Object key) {
      return load(segment, key)
            .thenApply(Objects::nonNull);
   }

   /**
    *
    * @param segment
    * @param entry
    * @return
    */
   CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry);

   /**
    *
    * @param segment
    * @param key
    * @return
    */
   CompletionStage<Boolean> delete(int segment, Object key);

   /**
    *
    * @param segments
    * @return
    */
   default CompletionStage<Void> addSegments(IntSet segments) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.SEGMENTABLE + ", but it does not implement addSegments");
   }

   /**
    *
    * @param segments
    * @return
    */
   default CompletionStage<Void> removeSegments(IntSet segments) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.SEGMENTABLE + ", but it does not implement removeSegments");
   }

   /**
    *
    * @return
    */
   CompletionStage<Void> clear();

   /**
    *
    * @param publisherCount
    * @param publisher
    * @return
    */
   default CompletionStage<Void> bulkWrite(int publisherCount, Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> publisher) {
      return Flowable.fromPublisher(publisher)
            .concatMapCompletable(sp ->
                        Flowable.fromPublisher(sp)
                              .concatMapCompletable(me -> Completable.fromCompletionStage(write(sp.getSegment(), me)))
                  , publisherCount)
            .toCompletionStage(null);
   }

   /**
    *
    * @param publisherCount
    * @param publisher
    * @return
    */
   default CompletionStage<Void> bulkDelete(int publisherCount, Publisher<SegmentedPublisher<Object>> publisher) {
      return Flowable.fromPublisher(publisher)
            .concatMapCompletable(sp ->
                        Flowable.fromPublisher(sp)
                              .concatMapCompletable(obj -> Completable.fromCompletionStage(delete(sp.getSegment(), obj)))
                  , publisherCount)
            .toCompletionStage(null);
   }

   /**
    *
    * @param segments
    * @return
    */
   default CompletionStage<Long> size(IntSet segments) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.BULK_READ + ", but it does not implement size");
   }

   /**
    *
    * @param segments
    * @return
    */
   default CompletionStage<Long> approximateSize(IntSet segments) {
      return size(segments);
   }

   /**
    *
    * @param segments
    * @param filter
    * @return
    */
   default Publisher<MarshallableEntry<K, V>> publishEntries(IntSet segments, Predicate<? super K> filter, boolean includeValues) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.BULK_READ + ", but it does not implement entryPublisher");
   }

   /**
    * Publishes keys from this store that are in one of the provided segments and also pass the provided filter.
    * The returned publisher must support being subscribed to any number of times. That is subsequent invocations of
    * {@link Publisher#subscribe(Subscriber)} should provide independent views of the underlying keys to the Subscribers.
    * Keys should not retrieved until a given Subscriber requests them via the
    * {@link org.reactivestreams.Subscription#request(long)} method.
    * <p>
    * Subscribing to the returned {@link Publisher} should not block the invoking thread. It is up to the store
    * implementation to ensure this occurs. If the underlying store implementation has non blocking support the
    * recommended approach is to return a Publisher that observes its values on the provided
    * {@link InitializationContext#getNonBlockingExecutor()}. If however the store must block to perform an operation it
    * is recommended to wrap your Publisher before returning with the
    * {@link org.infinispan.util.concurrent.BlockingManager#blockingPublisher(Publisher)} method and it will handle
    * subscription and observation on the blocking and non blocking executors respectively.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#BULK_READ}</td>
    *       <td valign="top">When set this store must implement this method</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">When set this store must not return expired keys</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set the provided {@code segments} parameter should be ignored</td>
    *    </tr>
    * </table>
    * <p>
    * @implSpec
    * A default implementation is provided that does the following:
    * <pre>{@code
    * return Flowable.fromPublisher(publishEntries(segments, filter))
    *     .map(MarshallableEntry::getKey);}
    * </pre>
    * @param segments A set of segments to filter keys by. This will always be non null.
    * @param filter A filter to filter they keys by. If this is null then no additional filtering should be done after segments.
    * @return a publisher that will provide the keys from the store
    */
   default Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      return Flowable.fromPublisher(publishEntries(segments, filter, false))
            .map(MarshallableEntry::getKey);
   }

   /**
    *
    * @return
    */
   default Publisher<MarshallableEntry<K, V>> purgeExpired() {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.EXPIRATION + ", but it does not implement purgeExpired");
   }

   /**
    * Write modifications to the store in the prepare phase, as this is the only way we know the FINAL values of the entries.
    * This is required to handle scenarios where an objects value is changed after the put command has been executed, but
    * before the commit is called on the Tx.
    *
    * @param transaction the current transactional context.
    * @param batchModification an object containing the write/remove operations required for this transaction.
    */
   default CompletionStage<Void> prepareWithModifications(Transaction transaction, BatchModification batchModification) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.TRANSACTIONAL + ", but it does not implement prepareWithModifications");
   }

   /**
    * Commit the provided transaction's changes to the underlying store.
    *
    * @param transaction the current transactional context.
    */
   default CompletionStage<Void> commit(Transaction transaction) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.TRANSACTIONAL + ", but it does not implement commit");
   }

   /**
    * Rollback the provided transaction's changes to the underlying store.
    *
    * @param transaction the current transactional context.
    */
   default CompletionStage<Void> rollback(Transaction transaction) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.TRANSACTIONAL + ", but it does not implement rollback");
   }

   /**
    * Some stores may not want to perform operations based on if a command has certain flags
    * @implSpec
    * The default implementation returns false
    * @param commandFlags the flags attributed to the command when performing the operation
    * @return whether the operation should occur
    */
   default boolean ignoreCommandWithFlags(long commandFlags) {
      return false;
   }

   /**
    *
    * @param <Type>
    */
   interface SegmentedPublisher<Type> extends Publisher<Type> {
      /**
       *
       * @return
       */
      int getSegment();
   }
}
