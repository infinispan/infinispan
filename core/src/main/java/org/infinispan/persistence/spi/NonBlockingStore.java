package org.infinispan.persistence.spi;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.transaction.Transaction;

import org.infinispan.Cache;
import org.infinispan.commons.util.Experimental;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.support.BatchModification;
import org.infinispan.util.concurrent.CompletableFutures;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

/**
 * The contract for defining a way for a cache to interface with external source of data, such as a database,
 * filesystem etc. As the name implies, all of the methods in this class must <b>never</b> block the invoking thread.
 * <p>
 * The first method that will be invoked on this store will be the {@link #start(InitializationContext)} to allow
 * it to initialize and startup. Once the returned stage has completed the store is assumed to be in working state
 * and is ready to handle operations. Infinispan guarantees the visibility of variables written during
 * the start method, so there is no need to synchronize these manually, unless they are mutated in the normal operations
 * of the store itself.
 * <p>
 * After the store has started, Infinispan will utilise the {@link #characteristics()} method to query the store's
 * characteristics. It is highly recommended that this method never change the values it returns once the
 * store has been started as these may or may not be cached. For more information on how the characteristics affect
 * the store operations, please see {@link Characteristic} and its various values.
 * <p>
 * By default this interface only requires half a dozen or so methods to be implemented. However, there are more
 * optional methods that may be implemented. If you implement such a method, please be sure to advertise the appropriate
 * characteristic for that method, so Infinispan knows to invoke it. If Infinispan has been told a
 * characteristic is available and the method is not overridden, an {@link UnsupportedOperationException} will be
 * thrown when trying to invoke the appropriate method. Each {@link Characteristic} defines what methods map to which
 * characteristic.
 * <p>
 * Although recommended, Segmentation support in a store implementation is not required. Segment parameters are provided
 * for all methods where segment information would be required, for example {@link #load(int, Object)} and
 * {@link #publishEntries(IntSet, Predicate, boolean). When a store does not support segmentation, these parameters can
 * simply be ignored by the implementation. As previously stated, it's recommended that segmentation is supported as an
 * Infinispan cache can perform much more efficiently when segmentation is supported when
 * performing bulk operations such as {@code Cache.size()} or {@code Cache.entrySet().stream()}. It also decreases state
 * transfer duration when {@link PersistenceConfiguration#fetchPersistentState()} is enabled, as well as the time
 * required to remove data by segments. To indicate that a store implementation supports segmentation, it's necessary
 * that the {@link Characteristic#SEGMENTABLE} characteristic is returned via the {@link #characteristics()} method. A
 * store implementation can tell if segmentation is enabled by checking the store configuration
 * {@link StoreConfiguration#segmented()} available from the {@code InitializationContext}.
 * <p>
 * A store implementation may have to interact with blocking APIs to perform their required operations, however we
 * should never block the invoking thread, therefore Infinispan provides a utility helper for these operations. This is
 * the {@link org.infinispan.util.concurrent.BlockingManager} and may be obtained by invoking
 * {@link InitializationContext#getBlockingManager()} on the provided context in the start method. This utility class
 * comes with an assortment of methods ranging from equivalent methods for more commonly used methods such as
 * {@link java.util.concurrent.CompletableFuture#supplyAsync(Supplier, Executor)} to a wrapper around a
 * {@link Publisher} that ensures it is subscribed and obversed on the proper threads. The {@code BlockingManager} is
 * special in that it guarantees the code that is blocking is ran on a blocking thread but any stages it produces are
 * continued on a non blocking thread, which is very important to not leak blocking threads to the internal Infinispan
 * system.
 * <p>
 * Implementations of this store must be thread safe if concurrent operations are performed on it. The one exception
 * is that {@link #start(InitializationContext)} and {@link #stop()} will not be invoked concurrently with other
 * operations.
 * <p>
 * Note that this interface is Experimental and its methods may change slightly over time until it has matured.
 * @author William Burns
 * @since 11.0
 * @param <K> key value type
 * @param <V> value value type
 */
@Experimental
public interface NonBlockingStore<K, V> {

   /**
    * Enumeration defining the various characteristics of the underlying store to communicate what features it may
    * or may not support.
    */
   enum Characteristic {
      /**
       * Whether this cache can be shared between multiple nodes. An example would be an external system, such as
       * a database. This characteristic is here solely for validation of the store configuration.
       */
      SHAREABLE,
      /**
       * If this store only supports being read from.  Any write based operations will never be invoked on this store.
       * No optional methods map to this characteristic. The {@link #write(int, MarshallableEntry)},
       * {@link #delete(int, Object)}, {@link #bulkWrite(int, Publisher)}, and {@link #bulkDelete(int, Publisher)} methods
       * will not be invoked on a store that has this characteristic.
       */
      READ_ONLY,
      /**
       * If this store only supports being written to. Any read based operations will never be invoked on this store.
       * No optional methods map to this characteristic. The {@link #load(int, Object)} and
       * {@link #containsKey(int, Object)} methods will not be invoked on a store that has this characteristic.
       */
      WRITE_ONLY,
      /**
       * If this store supports bulk read operations. If a store does not have this characteristic operations such
       * as {@link Cache#size()} and {@code Cache.entrySet().stream()} will not utilize this store.
       * <p>
       * Stores that have this characteristic must override the {@link #publishKeys(IntSet, Predicate)},
       * {@link #publishEntries(IntSet, Predicate, boolean)}  and {@link #size(IntSet)} methods.
       * <p>
       * This characteristic is ignored if the store also contains {@link #WRITE_ONLY}
       */
      BULK_READ,
      /**
       * If this store supports being invoked in a transactional context with a prepare and commit or rollback phases.
       * Stores of this type may take part of the actual transaction if present.
       * <p>
       * Stores that have this characteristic must override the
       * {@link #prepareWithModifications(Transaction, BatchModification)}, {@link #commit(Transaction)} and
       * {@link #rollback(Transaction)} methods.
       * <p>
       * This characteristic is ignored if the store also contains {@link #READ_ONLY}
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
       * {@link #removeSegments(IntSet)} methods. If a store is {@link #SHAREABLE} and is configured to be shared
       * via configuration these methods will not be invoked though.
       */
      SEGMENTABLE,
      /**
       * If this store supports storing expiration metadata. That is this store should never return an expired entry
       * via any methods such as {@link #load(int, Object)}, {@link #publishKeys(IntSet, Predicate)} or
       * {@link #publishEntries(IntSet, Predicate, boolean)}. It is recommended that a store use the provided
       * {@link org.infinispan.commons.time.TimeService} in the {@code InitializationContext} to determine if an
       * entry has expired.
       * <p>
       * The information about an entry and its expiration is included in the {@link org.infinispan.metadata.Metadata}
       * which is accessible from the {@link MarshallableEntry} which is provided.
       * <p>
       * Stores that have this characteristic must override the {@link #purgeExpired()} method.
       */
      EXPIRATION
   }

   /**
    * The first method that will be invoked to allow the store to be configured and for any additional steps, such as
    * connecting via a socket or opening file descriptors, to be performed.
    * <p>
    * The provided {@link InitializationContext} contains many helpful objects, including the configuration of the
    * cache and store, concurrency utilities such as {@link org.infinispan.util.concurrent.BlockingManager} or
    * an executor reserved for non blocking operations only {@link InitializationContext#getNonBlockingExecutor()}.
    * <p>
    * This method is guaranteed to not be invoked concurrently with other operations. This means another method
    * will not be invoked on this store until after the returned Stage completes.
    * <p>
    * It is expected that an implementation should be able to "restart" by invoking {@code start} a second time if
    * {@link #stop()} has been invoked and allowed for its stage to complete.
    * @param ctx initialization context used to initialize this store
    * @return a stage that when complete signals that this store has been successfully started
    */
   CompletionStage<Void> start(InitializationContext ctx);

   /**
    * This method will be invoked when the cache is being shutdown. It is expected that all resources related to the
    * store to be freed upon completion of the returned stage.
    * <p>
    * This method is guaranteed to not be invoked concurrently with other operations. This also means another method
    * will not be invoked on this store until after the returned Stage completes.
    * <p>
    * It is expected that an implementation should be able to "restart" by invoking {@link #start(InitializationContext)}
    * a second time if {@code stop} has been invoked and allowed for its stage to complete.
    * @return a stage that when complete signals that this store has been stopped
    */
   CompletionStage<Void> stop();

   /**
    * Returns a set of characteristics for this store and its elements. This method may be invoked multiple times
    * to determine which methods of the store can be used and how its data can be handled.
    * <p>
    * Please see {@link Characteristic} and its values for a description of what each characteristic declares the
    * store as supporting.
    * @implSpec
    * The default implementation returns an empty set
    * @return the set of characteristics that this store supports
    */
   default Set<Characteristic> characteristics() {
      return EnumSet.noneOf(Characteristic.class);
   }

   /**
    * Returns a stage that when complete returns a boolean indicating whether the current store can be accessed for
    * requests. This can be useful for store implementations that rely on an external source, such as a remote database,
    * that may become unreachable. This can reduce sending requests to a store that is not available, as subsequent cache
    * requests will result in a {@link StoreUnavailableException} being thrown until the store becomes available again.
    * <p>
    * Store availability is is polled periodically to update a store's status if it's availability changes. This method
    * will not be invoked concurrently with itself (ie. this method will not be invoked until after the previous stage
    * has completed), but will be invoked concurrently with other operations, excluding
    * {@link #start(InitializationContext)} and {@link #stop()}.
    * <p>
    If a store is configured to be {@link StoreConfiguration#async()} and the store becomes unavailable, then it's
    possible for the cache operations to be accepted in the interim period between the loss of availability and the
    modification-queue becoming full. This allows for this store to be unavailable for short periods of time without a
    {@link StoreUnavailableException} being thrown, however if the store does not become available before the queue
    fills, then a {@link StoreUnavailableException} is eventually thrown.
    * @implSpec
    * The default implementations returns a completed stage with the value {@code Boolean.TRUE}
    * @return stage that when complete signals if the store is available
    */
   default CompletionStage<Boolean> isAvailable() {
      return CompletableFutures.completedTrue();
   }

   /**
    * Returns a stage that will contain the value loaded from the store. If a {@link MarshallableEntry} needs to be
    * created here, {@link InitializationContext#getMarshallableEntryFactory()} ()} and {@link
    * InitializationContext#getByteBufferFactory()} should be used.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#WRITE_ONLY}</td>
    *       <td valign="top">This method will never be invoked</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">When set this method must not return expired entries</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set the provided {@code segment} parameter may be ignored</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segment the segment for the given key if segmentation is enabled otherwise 0
    * @param key key of the entry to load
    * @return a stage that when complete contains the store value or null if not present
    */
   CompletionStage<MarshallableEntry<K, V>> load(int segment, Object key);

   /**
    * Returns a stage that will contain whether the value can be found in the store.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#WRITE_ONLY}</td>
    *       <td valign="top">This method will never be invoked</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">When set this method must not return true if the entry was expired</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set the provided {@code segment} parameter may be ignored</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * <p>
    * @implSpec
    * A default implementation is provided that does the following:
    * <pre>{@code
    * return load(segment, key)
    *        .thenApply(Objects::nonNull);}
    * </pre>
    * @param segment the segment for the given key if segmentation is enabled otherwise 0
    * @param key key of the entry to check
    * @return a stage that when complete contains a boolean stating if the value is contained in the store
    */
   default CompletionStage<Boolean> containsKey(int segment, Object key) {
      return load(segment, key)
            .thenApply(Objects::nonNull);
   }

   /**
    * Writes the entry to the store for the given segment returning a stage that completes normally when it is finished.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#READ_ONLY}</td>
    *       <td valign="top">This method will never be invoked</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">When set this method must store the expiration metadata</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When set this method must ensure the segment is stored with the entry</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segment the segment for the given key if segmentation is enabled otherwise 0
    * @param entry the entry to persist to the store
    * @return a stage that when complete signals that the store has written the value
    */
   CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry);

   /**
    * Removes the entry for given key and segment from the store returning a stage that when completes normally
    * contains whether the entry was actually removed or not.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#READ_ONLY}</td>
    *       <td valign="top">This method will never be invoked</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set the provided {@code segment} parameter may be ignored</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segment the segment for the given key if segmentation is enabled otherwise 0
    * @param key key of the entry to delete from the store
    * @return a stage that when complete contains a boolean stating if the value was removed from the store
    */
   CompletionStage<Boolean> delete(int segment, Object key);

   /**
    * Invoked when a node becomes an owner of the given segments. Some store implementations may require initializing
    * additional resources when a new segment is required. For example a store could store entries in a different file
    * per segment.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SHAREABLE}</td>
    *       <td valign="top">If the store has this characteristic and is configured to be {@link StoreConfiguration#shared()},
    *          this method will never be invoked</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">This method is only invoked if the store has this characteristic</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segments the segments to add
    * @return a stage that when complete signals that the segments have been added
    */
   default CompletionStage<Void> addSegments(IntSet segments) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.SEGMENTABLE + ", but it does not implement addSegments");
   }

   /**
    * Invoked when a node loses ownership of the given segments. A store must then remove any entries that map to the
    * given segments and can remove any resources related to the given segments. For example a database store can
    * delete rows of the given segment or a file based store may delete files related to the given segments.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SHAREABLE}</td>
    *       <td valign="top">If the store has this characteristic and is configured to be {@link StoreConfiguration#shared()},
    *          this method will never be invoked</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">This method is only invoked if the store has this characteristic</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segments the segments to remove
    * @return a stage that when complete signals that the segments have been removed
    */
   default CompletionStage<Void> removeSegments(IntSet segments) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.SEGMENTABLE + ", but it does not implement removeSegments");
   }

   /**
    * Clears all entries from the store.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#READ_ONLY}</td>
    *       <td valign="top">This method will never be invoked</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @return a stage that when complete signals that the store has been cleared
    */
   CompletionStage<Void> clear();

   /**
    * Writes the entries provided by the publisher into the underlying store. The Publisher will provide a
    * {@link SegmentedPublisher} for every segment in the batch, which contains at least one entry that maps to
    * the segment for the SegmentPublisher.
    * <p>
    * The publisher may publish up to {@code publisherCount} publishers where each publisher is separated by the segment
    * each entry maps to. Failure to request at least {@code publisherCount} publishers from the Publisher may cause a
    * deadlock. Many reactive tools have methods such as {@code flatMap} that take an argument of how many concurrent
    * subscriptions it manages, which is perfectly matched with this argument.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#READ_ONLY}</td>
    *       <td valign="top">This method will never be invoked</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set the provided {@code publisherCount} parameter will be one and there
    *          will only be one {@code SegmentedPublisher} to subscribe to</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * <p>
    * @implSpec
    * The default implementation subscribes to the Publisher and simply invokes {@link #write(int, MarshallableEntry)}
    * for each entry in the publishers encountered passing the segment it maps to.
    * @param publisherCount how many SegmentedPublishers the provided Publisher may publish
    * @param publisher the publisher which will provide a {@code SegmentedPublisher} for each segment containing entries
    * @return a stage that when complete signals that the store has written the values
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
    * Removes the entries for the keys provided by the publisher into the underlying store. The Publisher will provide a
    * {@link SegmentedPublisher}, for every segment in the batch, which contains at least one key that maps to
    * the segment for the SegmentPublisher.
    * <p>
    * The publisher may publish up to {@code publisherCount} publishers where each publisher is separated by the segment
    * each entry maps to. Failure to request at least {@code publisherCount} publishers from the Publisher may cause a
    * deadlock. Many reactive tools have methods such as {@code flatMap} that take an argument of how many concurrent
    * subscriptions it manages, which is perfectly matched with this argument.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#READ_ONLY}</td>
    *       <td valign="top">This method will never be invoked</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set the provided {@code publisherCount} parameter will be one and there
    *          will only be one {@code SegmentedPublisher} to subscribe to</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * <p>
    * @implSpec
    * The default implementation subscribes to the Publisher and simply invokes {@link #delete(int, Object)}
    * for each entry in the publishers encountered passing the segment it maps to.
    * @param publisherCount how many SegmentedPublishers the provided Publisher may publish
    * @param publisher the publisher which will provide a {@code SegmentedPublisher} for each segment containing entries
    * @return a stage that when complete signals that the store has deleted the values
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
    * Returns the amount of entries that map to the given segments in the store.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#BULK_READ}</td>
    *       <td valign="top">This method is only invoked if the store has this characteristic</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set the provided {@code segments} parameter may be ignored</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segments the segments for which the entries are counted
    * @return a stage that when complete contains the count of how many entries are present for the given segments
    */
   default CompletionStage<Long> size(IntSet segments) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.BULK_READ + ", but it does not implement size");
   }

   /**
    * Returns an estimation of the amount of entries that map to the given segments in the store. This is similar to
    * {@link #size(IntSet)} except that it may take some liberties in the returned size. That is it may ignore if
    * an entry is expired or if the store has some underlying optimizations to eventually have a consistent size.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#BULK_READ}</td>
    *       <td valign="top">This method is only invoked if the store has this characteristic</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set the provided {@code segments} parameter may be ignored</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * <p>
    * @implSpec
    * A default implementation is provided that does the following:
    * <pre>{@code
    * return size(segments);}
    * </pre>
    * @param segments the segments for which the entries are counted
    * @return a stage that when complete contains the count of how many entries are present for the given segments
    */
   default CompletionStage<Long> approximateSize(IntSet segments) {
      return size(segments);
   }

   /**
    * Publishes entries from this store that are in one of the provided segments and also pass the provided filter.
    * The returned publisher must support being subscribed to any number of times. That is subsequent invocations of
    * {@link Publisher#subscribe(Subscriber)} should provide independent views of the underlying entries to the Subscribers.
    * Entries should not retrieved until a given Subscriber requests them via the
    * {@link org.reactivestreams.Subscription#request(long)} method.
    * <p>
    * Subscribing to the returned {@link Publisher} should not block the invoking thread. It is the responsibility of
    * the store implementation to ensure this occurs. If however the store must block to perform an operation it
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
    *       <td valign="top">This method is only invoked if the store has this characteristic</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">When set the returned publisher must not return expired entries</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set the provided {@code segments} parameter may be ignored</td>
    *    </tr>
    * </table>
    * @param segments A set of segments to filter entries by. This will always be non null.
    * @param filter A filter to filter they keys by. If this is null then no additional filtering should be done after segments.
    * @return a publisher that will provide the keys from the store
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
    * Subscribing to the returned {@link Publisher} should not block the invoking thread. It is the responsibility of
    * the store implementation to ensure this occurs. If however the store must block to perform an operation it
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
    *       <td valign="top">This method is only invoked if the store has this characteristic</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">When set the returned publisher must not return expired keys</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set the provided {@code segments} parameter may be ignored</td>
    *    </tr>
    * </table>
    * <p>
    * @implSpec
    * A default implementation is provided that just invokes {@link #publishEntries(IntSet, Predicate, boolean)} and
    * maps the {@link MarshallableEntry} to its key.
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
    * Returns a Publisher that when subscribed to will remove any entries that have expired from the store and publishes
    * them to returned Publisher.
    * <p>
    * When the Publisher is subscribed to it is expected to do point in time expiration and should
    * not return a Publisher that has infinite entries or never completes.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">This method is only invoked if the store has this characteristic</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @return a Publisher that publishes the entries that are expired at the time of subscription
    */
   default Publisher<MarshallableEntry<K, V>> purgeExpired() {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.EXPIRATION + ", but it does not implement purgeExpired");
   }

   /**
    * Write modifications to the store in the prepare phase, which are not yet persisted until the same transaction
    * is committed via {@link #commit(Transaction)} or they are discarded if the transaction is rolled back via
    * {@link #rollback(Transaction)}.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#TRANSACTIONAL}</td>
    *       <td valign="top">This method is only invoked if the store has this characteristic</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * <p>
    * This method will most likely change as the {@link BatchModification} does not contain the segment information
    * required for the store to map the entries to.
    * @param transaction the current transactional context.
    * @param batchModification an object containing the write/remove operations required for this transaction.
    */
   @Experimental
   default CompletionStage<Void> prepareWithModifications(Transaction transaction, BatchModification batchModification) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.TRANSACTIONAL + ", but it does not implement prepareWithModifications");
   }

   /**
    * Commit the provided transaction's changes to the underlying store.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#TRANSACTIONAL}</td>
    *       <td valign="top">This method is only invoked if the store has this characteristic</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param transaction the current transactional context.
    * @return a stage that when completed signals that the transaction was committed
    */
   default CompletionStage<Void> commit(Transaction transaction) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.TRANSACTIONAL + ", but it does not implement commit");
   }

   /**
    * Rollback the provided transaction's changes to the underlying store.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#TRANSACTIONAL}</td>
    *       <td valign="top">This method is only invoked if the store has this characteristic</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param transaction the current transactional context.
    * @return a stage that when completed signals that the transaction was rolled back
    */
   default CompletionStage<Void> rollback(Transaction transaction) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.TRANSACTIONAL + ", but it does not implement rollback");
   }

   /**
    * Some stores may not want to perform operations based on if a command has certain flags. This method is currently
    * only used for testing single write operations. This method may be removed at any time as it is experimental, it is
    * not recommended for end users to implement it.
    * @implSpec
    * The default implementation returns false
    * @param commandFlags the flags attributed to the command when performing the operation
    * @return whether the operation should occur
    */
   @Experimental
   default boolean ignoreCommandWithFlags(long commandFlags) {
      return false;
   }

   /**
    * A Publisher that in addition a stream of values also provides the segment that all of those values map to.
    * @param <Type> type of values in this Publisher
    */
   interface SegmentedPublisher<Type> extends Publisher<Type> {
      /**
       * Returns the segment for all keys in the publisher.
       * @return segment the data the publisher provides maps to
       */
      int getSegment();
   }
}
