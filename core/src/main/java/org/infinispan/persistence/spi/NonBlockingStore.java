package org.infinispan.persistence.spi;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import org.infinispan.util.concurrent.CompletableFutures;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

/**
 * The contract for defining how caches interface with external sources of data, such as databases or filesystems.
 * As the name implies, any method in this class must <b>never</b> block the invoking thread.
 * <p>
 * The first method invoked on this store is {@link #start(InitializationContext)}, which starts the store.
 * Once the returned stage has completed, the store is assumed to be in working state and ready to handle operations.
 * Infinispan guarantees the visibility of variables written during the start method, so you do not need to
 * synchronize them manually unless they are mutated in the normal operations of the store itself.
 * <p>
 * After the store starts, Infinispan uses the {@link #characteristics()} method to query the characteristics of
 * the store. It is highly recommended that this method never change the values it returns after the
 * store starts because characteristics might not be cached. For more information, see {@link Characteristic}
 * and its various values.
 * <p>
 * By default, this interface has only a few required methods. If you implement any of the optional methods,
 * ensure that you advertise the appropriate characteristic for that method so that Infinispan invokes it.
 * If Infinispan is instructed that a
 * characteristic is available and the method is not overridden, an {@link UnsupportedOperationException} will be
 * thrown when trying to invoke the appropriate method. Each {@link Characteristic} defines what methods map to which
 * characteristic.
 * <p>
 * Although recommended, segmentation support in store implementations is optional. Segment parameters are provided
 * for all methods where segment information is required, for example {@link #load(int, Object)} and
 * {@link #publishEntries(IntSet, Predicate, boolean). If your store implementation does not support segmentation,
 * you can ignore these parameters. However, you should note that segmented stores allow Infinispan caches to more
 * efficiently perform bulk operations such as {@code Cache.size()} or {@code Cache.entrySet().stream()}. Segmentation
 * also decreases the duration of state transfers when {@link PersistenceConfiguration#fetchPersistentState()} is enabled,
 * as well as the time required to remove data by segments. To indicate that a store implementation supports segmentation,
 * the {@link Characteristic#SEGMENTABLE} characteristic must be returned by the {@link #characteristics()} method. Store
 * implementations can determine if stores are configured to be segmented if {@link StoreConfiguration#segmented()} is
 * enabled, which is available from the {@code InitializationContext}.
 * <p>
 * Store implementations might need to interact with blocking APIs to perform their required operations. However the invoking
 * thread must never be blocked, so Infinispan provides a {@link org.infinispan.util.concurrent.BlockingManager} utility class
 * that handles blocking operations to ensure that they do not leak into the internal system. {@code BlockingManager} does this
 * by running any blocking operations on blocking threads, while any stages continue on non-blocking threads.
 * <p>
 * This utility class provides different methods that range from equivalents for commonly used methods, such as
 * {@link java.util.concurrent.CompletableFuture#supplyAsync(Supplier, Executor)}, to a wrapper around a {@link Publisher} that
 * ensures it is subscribed and obversed on the correct threads. To obtain a {@code BlockingManager}, invoke the
 * {@link InitializationContext#getBlockingManager()} method on the provided context in the start method.
 * <p>
 * Implementations of this store must be thread safe if concurrent operations are performed on it. The one exception
 * is that {@link #start(InitializationContext)} and {@link #stop()} are not invoked concurrently with other operations.
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
       * If this store can be shared across multiple Infinispan nodes; for example, an external system such as
       * a database. This characteristic allows validation of the store configuration.
       */
      SHAREABLE,
      /**
       * If this store supports only being read from. Write-based operations are never invoked on this store.
       * No optional methods map to this characteristic. The {@link #write(int, MarshallableEntry)},
       * {@link #delete(int, Object)}, and {@link #batch(int, Publisher, Publisher)}  methods
       * are not invoked on stores with this characteristic.
       */
      READ_ONLY,
      /**
       * If this store supports only being written to. Read-based operations are never invoked on this store.
       * No optional methods map to this characteristic. The {@link #load(int, Object)} and
       * {@link #containsKey(int, Object)} methods are not invoked on stores with this characteristic.
       */
      WRITE_ONLY,
      /**
       * If this store supports bulk read operations. If a store does not have this characteristic, operations such
       * as {@link Cache#size()} and {@code Cache.entrySet().stream()} do not use this store.
       * <p>
       * Stores that have this characteristic must override the {@link #publishKeys(IntSet, Predicate)},
       * {@link #publishEntries(IntSet, Predicate, boolean)} and {@link #size(IntSet)} methods.
       * <p>
       * This characteristic is ignored if the store also contains {@link #WRITE_ONLY}.
       */
      BULK_READ,
      /**
       * If this store supports being invoked in a transactional context with prepare and commit or rollback phases.
       * Stores of this type can participate in the actual transaction, if present.
       * <p>
       * Stores that have this characteristic must override the
       * {@link #prepareWithModifications(Transaction, int, Publisher, Publisher)} , {@link #commit(Transaction)} and
       * {@link #rollback(Transaction)} methods.
       * <p>
       * This characteristic is ignored if the store also contains {@link #READ_ONLY}.
       */
      TRANSACTIONAL,
      /**
       * If this store supports segmentation. All methods in this SPI take as an argument a way to map a given
       * entry to a segment. A segment in Infinispan is an int that acts as a bucket for many keys. Many store
       * implementations may be able to store and load entries in a more performant way if they segment their data
       * accordingly.
       * <p>
       * If this store is not segmentable then invokers of this SPI are not required to calculate these segments before
       * invoking these methods and thus these methods may be invoked with any int value, null or equivalent. Refer to
       * each method to determine their effect when this store is not segmented.
       * <p>
       * Note that you can also configure stores at runtime to be segmented or not. If the runtime configuration of this
       * store is non-segmented, it is equivalent to the store not having the SEGMENTABLE characteristic, which might cause
       * parameters to be null or invalid segment numbers. Store implementation can block this configuration
       * by throwing an exception in the {@link #start(InitializationContext)} method.
       * <p>
       * While it is possible that a SEGMENTABLE store can be configured as non-segmented, a store that is not
       * SEGMENTABLE can never then later be configured as segmented.
       * <p>
       * Stores that have this characteristic must override the {@link #addSegments(IntSet)} and
       * {@link #removeSegments(IntSet)} methods. However, if a store is {@link #SHAREABLE} and is configured to be shared
       * via configuration these methods are not invoked.
       */
      SEGMENTABLE,
      /**
       * If this store uses expiration metadata so that it never returns expired entries
       * via any methods such as {@link #load(int, Object)}, {@link #publishKeys(IntSet, Predicate)} or
       * {@link #publishEntries(IntSet, Predicate, boolean)}. Stores should use the provided
       * {@link org.infinispan.commons.time.TimeService} in the {@code InitializationContext} to determine if entries are
       * expired.
       * <p>
       * The information about an entry and its expiration is included in the {@link org.infinispan.metadata.Metadata},
       * accessible from the {@link MarshallableEntry} that is provided.
       * <p>
       * Stores that have this characteristic must override the {@link #purgeExpired()} method.
       */
      EXPIRATION
   }

   /**
    * Shortcut to return -1L when the size or approximate size is unavailable.
    */
   CompletableFuture<Long> SIZE_UNAVAILABLE_FUTURE = CompletableFuture.completedFuture(-1L);

   /**
    * The first method to invoke so that the store can be configured and additional steps, such as connecting through
    * a socket or opening file descriptors, are performed.
    * <p>
    * The provided {@link InitializationContext} contains many helpful objects, including the configuration of the
    * cache and store, concurrency utilities such as {@link org.infinispan.util.concurrent.BlockingManager} or
    * an executor reserved for non-blocking operations only {@link InitializationContext#getNonBlockingExecutor()}.
    * <p>
    * This method is guaranteed not to be invoked concurrently with other operations. This means other methods are
    * not invoked on this store until after the returned Stage completes.
    * <p>
    * It is expected that an implementation should be able to "restart" by invoking {@code start} a second time if
    * {@link #stop()} has been invoked and allowed for its stage to complete.
    * @param ctx initialization context used to initialize this store.
    * @return a stage that, when complete, indicates that this store has started successfully.
    */
   CompletionStage<Void> start(InitializationContext ctx);

   /**
    * This method is invoked when the cache is being shutdown. It is expected that all resources related to the
    * store are freed when the returned stage is complete.
    * <p>
    * This method is guaranteed not to be invoked concurrently with other operations. This means other methods are
    * not invoked on this store until after the returned Stage completes.
    * <p>
    * It is expected that an implementation should be able to "restart" by invoking {@link #start(InitializationContext)}
    * a second time if {@code stop} has been invoked and allowed for its stage to complete.
    * @return a stage that, when complete, indicates that this store has stopped.
    */
   CompletionStage<Void> stop();

   /**
    * This method is to be invoked when the store should clean up all underlying data and storage of said data. For
    * example a database store would remove the underlying table(s) that it is using and a file based store would
    * remove all of the various files or directories it may have created.
    * @implSpec
    * The default implementation invokes the {@link #stop()} method returning the stage it returned.
    * @return a stage that, when complete, indicates that this store is stopped and all data and storage for it are also
    *    cleaned up
    */
   default CompletionStage<Void> destroy() {
      return stop();
   }

   /**
    * Returns a set of characteristics for this store and its elements. This method may be invoked multiple times
    * to determine which methods of the store can be used and how the data in the store can be handled.
    * <p>
    * Refer to {@link Characteristic} and its values for descriptions of each characteristic for stores.
    * @implSpec
    * The default implementation returns an empty set.
    * @return the set of characteristics that this store supports.
    */
   default Set<Characteristic> characteristics() {
      return EnumSet.noneOf(Characteristic.class);
   }

   /**
    * Returns a stage that, when complete, returns a boolean indicating whether the current store can be accessed for
    * requests. This can be useful for store implementations that rely on an external source, such as a remote database,
    * that may become unreachable. This can reduce sending requests to a store that is not available, as subsequent cache
    * requests will result in a {@link StoreUnavailableException} being thrown until the store becomes available again.
    * <p>
    * Store availability is is polled periodically to update the status of stores if their availability changes. This method
    * is not invoked concurrently with itself. In other words, this method is not invoked until after the previous stage
    * has completed. However this method is invoked concurrently with other operations, except for
    * {@link #start(InitializationContext)} and {@link #stop()}.
    * <p>
    If a store is configured to be {@link StoreConfiguration#async()} and the store becomes unavailable, then it is
    possible for the cache operations to be accepted in the interim period between the loss of availability and the
    modification-queue becoming full. This allows for this store to be unavailable for short periods of time without a
    {@link StoreUnavailableException} being thrown; however if the store does not become available before the queue
    fills, then a {@link StoreUnavailableException} is thrown.
    * @implSpec
    * The default implementation returns a completed stage with the value {@code Boolean.TRUE}.
    * @return stage that, when complete, indicates if the store is available.
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
    *       <td valign="top">This method will never be invoked.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">When set this method must not return expired entries.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set or segmentation is disabled in the
    *       {@link StoreConfiguration#segmented() configuration},
    *       the {@code segment} parameter may be ignored.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segment the segment for the given key if segmentation is enabled, otherwise 0.
    * @param key key of the entry to load.
    * @return a stage that, when complete, contains the store value or null if not present.
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
    *       <td valign="top">This method will never be invoked.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">When set this method must not return true if the entry was expired.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set or segmentation is disabled in the
    *       {@link StoreConfiguration#segmented() configuration},
    *       the {@code segment} parameter may be ignored.</td>
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
    * @param segment the segment for the given key if segmentation is enabled, otherwise 0.
    * @param key key of the entry to check.
    * @return a stage that, when complete, contains a boolean stating if the value is contained in the store.
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
    *       <td valign="top">This method will never be invoked.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">When set, this method must store the expiration metadata.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When set and segmentation is not disabled in the
    *       {@link StoreConfiguration#segmented() configuration},
    *       this method must ensure the segment is stored with the entry.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segment the segment for the given key if segmentation is enabled, otherwise 0.
    * @param entry the entry to persist to the store.
    * @return a stage that when complete indicates that the store has written the value.
    */
   CompletionStage<Void> write(int segment, MarshallableEntry<? extends K, ? extends V> entry);

   /**
    * Removes the entry for given key and segment from the store
    * and optionally report if the entry was actually removed or not.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#READ_ONLY}</td>
    *       <td valign="top">This method will never be invoked.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set or segmentation is disabled in the
    *       {@link StoreConfiguration#segmented() configuration},
    *       the {@code segment} parameter may be ignored.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segment the segment for the given key if segmentation is enabled, otherwise 0.
    * @param key key of the entry to delete from the store.
    * @return a stage that completes with {@code TRUE} if the key existed in the store,
    * {@code FALSE} if the key did not exist in the store,
    * or {@code null} if the store does not report this information.
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
    *          this method will never be invoked.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">This method is invoked only if the store has this characteristic and is configured to be
    *          {@link StoreConfiguration#segmented() segmented}.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segments the segments to add.
    * @return a stage that, when complete, indicates that the segments have been added.
    */
   default CompletionStage<Void> addSegments(IntSet segments) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.SEGMENTABLE + ", but it does not implement addSegments");
   }

   /**
    * Invoked when a node loses ownership of the given segments. A store must then remove any entries that map to the
    * given segments and can remove any resources related to the given segments. For example, a database store can
    * delete rows of the given segment or a file-based store can delete files related to the given segments.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SHAREABLE}</td>
    *       <td valign="top">If the store has this characteristic and is configured to be
    *       {@link StoreConfiguration#shared() shared}, this method will never be invoked.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">This method is invoked only if the store has this characteristic and is configured to be
    *          {@link StoreConfiguration#segmented() segmented}.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segments the segments to remove.
    * @return a stage that, when complete, indicates that the segments have been removed.
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
    *       <td valign="top">This method will never be invoked.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @return a stage that, when complete, indicates that the store has been cleared.
    */
   CompletionStage<Void> clear();

   /**
    * Writes and removes the entries provided by the Publishers into the store. Both are provided in the same method
    * so that a batch may be performed as a single atomic operation if desired, although it is up to the store to
    * manage its batching. If needed a store may generate batches of a configured size by using the
    * {@link StoreConfiguration#maxBatchSize()} setting.
    * <p>
    * Each of the {@code Publisher}s may publish up to {@code publisherCount} publishers where each
    * publisher is separated by the segment each entry maps to. Failure to request at least {@code publisherCount} publishers from the Publisher may cause a
    * deadlock. Many reactive tools have methods such as {@code flatMap} that take an argument of how many concurrent
    * subscriptions it manages, which is perfectly matched with this argument.
    * <p>
    * WARNING: For performance reasons neither Publisher will emit any {@link SegmentedPublisher}s until both the write
    * and remove Publisher are subscribed to. These Publishers should also be only subscribed to once.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#READ_ONLY}</td>
    *       <td valign="top">This method will never be invoked.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">If not set or segmentation is disabled in the
    *       {@link StoreConfiguration#segmented() configuration},
    *       the {@code publisherCount} parameter has a value of 1,
    *       which means there is only be one {@code SegmentedPublisher} to subscribe to.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * <p>
    * @implSpec
    * The default implementation subscribes to both Publishers but requests values from the write publisher invoking
    * {@link #write(int, MarshallableEntry)} for each of the entries in a non overlapping sequential fashion. Once all
    * of the writes are complete it does the same for the remove key Publisher but invokes {@link #delete(int, Object)}
    * for each key.
    * @param publisherCount the maximum number of {@code SegmentPublisher}s either publisher will publish
    * @param removePublisher publishes what keys should be removed from the store
    * @param writePublisher publishes the entries to write to the store
    * @return a stage that when complete signals that the store has written the values
    */
   default CompletionStage<Void> batch(int publisherCount, Publisher<SegmentedPublisher<Object>> removePublisher,
         Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      Flowable<Void> entriesWritten = Flowable.fromPublisher(writePublisher)
            .concatMapEager(sp ->
                        Flowable.fromPublisher(sp)
                              .concatMapCompletable(me -> Completable.fromCompletionStage(write(sp.getSegment(), me)))
                              .toFlowable()
                  , publisherCount, publisherCount);
      Flowable<Void> removedKeys = Flowable.fromPublisher(removePublisher)
            .concatMapEager(sp ->
                        Flowable.fromPublisher(sp)
                              .concatMapCompletable(key -> Completable.fromCompletionStage(delete(sp.getSegment(), key)))
                              .toFlowable()
                  , publisherCount, publisherCount);
      // Note that removed is done after write has completed, but is subscribed eagerly. This makes sure there is only
      // one pending write or remove.
      return Flowable.concatArrayEager(entriesWritten, removedKeys)
            .lastStage(null);
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
    *       <td valign="top">This method is only invoked if the store has this characteristic.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set or segmentation is disabled in the
    *       {@link StoreConfiguration#segmented() configuration},
    *       the {@code segments} parameter may be ignored.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param segments the segments for which the entries are counted.
    * @return a stage that, when complete, contains the count of how many entries are present for the given segments.
    */
   default CompletionStage<Long> size(IntSet segments) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.BULK_READ + ", but it does not implement size");
   }

   /**
    * Returns an estimation of the amount of entries that map to the given segments in the store. This is similar to
    * {@link #size(IntSet)} except that it is not strict about the returned size. For instance, this method might ignore
    * if an entry is expired or if the store has some underlying optimizations to eventually have a consistent size.
    * <p>
    * The implementations should be O(1).
    * If a size approximation cannot be returned without iterating over all the entries in the store,
    * the implementation should return {@code -1L}.
    * </p>
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#BULK_READ}</td>
    *       <td valign="top">This method is only invoked if the store has this characteristic.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When the store does not have this characteristic or segmentation is disabled in the
    *       {@link StoreConfiguration#segmented() configuration},
    *       the {@code segment} parameter is always {@code IntSets.immutableRangeSet(numSegments)}.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * <p>
    * @implSpec
    * The default implementation always returns {@code -1}.
    * @param segments the segments for which the entries are counted.
    * @return a stage that, when complete, contains the approximate count of the entries in the given segments,
    * or {@code -1L} if an approximate count cannot be provided.
    */
   default CompletionStage<Long> approximateSize(IntSet segments) {
      return SIZE_UNAVAILABLE_FUTURE;
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
    * subscription and observation on the blocking and non-blocking executors respectively.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#BULK_READ}</td>
    *       <td valign="top">This method is only invoked if the store has this characteristic.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">When set the returned publisher must not return expired entries.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set or segmentation is disabled in the
    *       {@link StoreConfiguration#segmented() configuration},
    *       the {@code segment} parameter may be ignored.</td>
    *    </tr>
    * </table>
    * @param segments a set of segments to filter entries by. This will always be non null.
    * @param filter a filter to filter they keys by. If this is null then no additional filtering should be done after segments.
    * @return a publisher that provides the keys from the store.
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
    *       <td valign="top">This method is only invoked if the store has this characteristic.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">When set the returned publisher must not return expired keys.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#SEGMENTABLE}</td>
    *       <td valign="top">When this is not set or segmentation is disabled in the
    *       {@link StoreConfiguration#segmented() configuration},
    *       the {@code segment} parameter may be ignored.</td>
    *    </tr>
    * </table>
    * <p>
    * @implSpec
    * A default implementation is provided that invokes {@link #publishEntries(IntSet, Predicate, boolean)} and
    * maps the {@link MarshallableEntry} to its key.
    * </pre>
    * @param segments a set of segments to filter keys by. This will always be non null.
    * @param filter a filter to filter they keys by. If this is null then no additional filtering should be done after segments.
    * @return a publisher that provides the keys from the store.
    */
   default Publisher<K> publishKeys(IntSet segments, Predicate<? super K> filter) {
      return Flowable.fromPublisher(publishEntries(segments, filter, false))
            .map(MarshallableEntry::getKey);
   }

   /**
    * Returns a Publisher that, after it is subscribed to, removes any expired entries from the store and publishes
    * them to the returned Publisher.
    * <p>
    * When the Publisher is subscribed to, it is expected to do point-in-time expiration and should
    * not return a Publisher that has infinite entries or never completes.
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
    *       <td valign="top">{@link Characteristic#EXPIRATION}</td>
    *       <td valign="top">This method is only invoked if the store has this characteristic.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @return a Publisher that publishes the entries that are expired at the time of subscription.
    */
   default Publisher<MarshallableEntry<K, V>> purgeExpired() {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.EXPIRATION + ", but it does not implement purgeExpired");
   }

   /**
    * Write remove and put modifications to the store in the prepare phase, which should not yet persisted until the
    * same transaction is committed via {@link #commit(Transaction)} or they are discarded if the transaction is rolled back via
    * {@link #rollback(Transaction)}.
    * <p>
    * Each of the {@code Publisher}s may publish up to {@code publisherCount} publishers where each
    * publisher is separated by the segment each entry maps to. Failure to request at least {@code publisherCount} publishers from the Publisher may cause a
    * deadlock. Many reactive tools have methods such as {@code flatMap} that take an argument of how many concurrent
    * subscriptions it manages, which is perfectly matched with this argument.
    * <p>
    * WARNING: For performance reasons neither Publisher will emit any {@link SegmentedPublisher}s until both the write
    * and remove Publisher are subscribed to. These Publishers should also be only subscribed to once.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#TRANSACTIONAL}</td>
    *       <td valign="top">This method is invoked only if the store has this characteristic.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param transaction the current transactional context.
    * @param publisherCount the maximum number of {@code SegmentPublisher}s either publisher will publish
    * @param removePublisher publishes what keys should be removed from the store
    * @param writePublisher publishes the entries to write to the store
    * @return a stage that when complete signals that the store has written the values
    */
   default CompletionStage<Void> prepareWithModifications(Transaction transaction, int publisherCount,
         Publisher<SegmentedPublisher<Object>> removePublisher,
         Publisher<SegmentedPublisher<MarshallableEntry<K, V>>> writePublisher) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.TRANSACTIONAL + ", but it does not implement prepareWithModifications");
   }

   /**
    * Commit changes in the provided transaction to the underlying store.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#TRANSACTIONAL}</td>
    *       <td valign="top">This method is invoked only if the store has this characteristic.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param transaction the current transactional context.
    * @return a stage that, when completed, indicates that the transaction was committed.
    */
   default CompletionStage<Void> commit(Transaction transaction) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.TRANSACTIONAL + ", but it does not implement commit");
   }

   /**
    * Roll back the changes from the provided transaction to the underlying store.
    * <p>
    * <h4>Summary of Characteristics Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Characteristics Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Characteristic</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link Characteristic#TRANSACTIONAL}</td>
    *       <td valign="top">This method is invoked only if the store has this characteristic.</td>
    *    </tr>
    * </table>
    * <p>
    * If a problem is encountered, it is recommended to wrap any created/caught Throwable in a
    * {@link PersistenceException} and the stage be completed exceptionally.
    * @param transaction the current transactional context.
    * @return a stage that, when completed, indicates that the transaction was rolled back.
    */
   default CompletionStage<Void> rollback(Transaction transaction) {
      throw new UnsupportedOperationException("Store characteristic included " + Characteristic.TRANSACTIONAL + ", but it does not implement rollback");
   }

   /**
    * Some stores may not want to perform operations based on if a command has certain flags. This method is currently
    * only used for testing single write operations. This method may be removed at any time as it is experimental, it is
    * not recommended for end users to implement it.
    * @implSpec
    * The default implementation returns false.
    * @param commandFlags the flags attributed to the command when performing the operation.
    * @return whether the operation should occur.
    */
   @Experimental
   default boolean ignoreCommandWithFlags(long commandFlags) {
      return false;
   }

   /**
    * A Publisher that provides a stream of values and the segments to which those values map.
    * @param <Type> type of values in this Publisher.
    */
   interface SegmentedPublisher<Type> extends Publisher<Type> {
      /**
       * Returns the segment for all keys in the publisher.
       * @return segment the data the publisher provides maps to.
       */
      int getSegment();
   }
}
