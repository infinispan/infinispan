package org.infinispan.commons.api.functional;

import java.util.concurrent.CompletableFuture;

import org.infinispan.commons.util.Experimental;

/**
 * An easily extensible parameter that allows functional map operations to be
 * tweaked. Apart from {@link org.infinispan.commons.api.functional.Param.FutureMode}, examples
 * would include local-only parameter, skip-cache-store parameter and others.
 *
 * <p>What makes {@link Param} different from {@link MetaParam} is that {@link Param}
 * values are never stored in the functional map. They merely act as ways to
 * tweak how operations are executed.
 *
 * <p>Since {@link Param} instances control how the internals work, only
 * {@link Param} implementations by Infinispan will be supported.
 *
 * <p>This interface is equivalent to Infinispan's Flag, but it's more
 * powerful because it allows to pass a flag along with a value. Infinispan's
 * Flag are enum based which means no values can be passed along with value.
 *
 * <p>Since each param is an independent entity, it's easy to create
 * public versus private parameter distinction. When parameters are stored in
 * enums, it's more difficult to make such distinction.
 *
 * @param <P> type of parameter
 * @since 8.0
 */
@Experimental
public interface Param<P> {

   /**
    * A parameter's identifier. Each parameter must have a different id.
    *
    * <p>A numeric id makes it flexible enough to be stored in collections that
    * take up low resources, such as arrays.
    */
   int id();

   /**
    * Parameter's value.
    */
   P get();

   /**
    * When a method defines {@link CompletableFuture} as a return type, it
    * implies the method called will be called asynchronously and that the
    * {@link CompletableFuture} returned will be completed once the method's
    * work is complete.
    *
    * <p>So, calling a method that returns {@link CompletableFuture} normally
    * implies that the method will allocate a thread to do that job. However,
    * there are situations when the user calls a method that returns
    * {@link CompletableFuture} and immediately calls {@link CompletableFuture#get()}
    * or similar methods to get the result. Calling such methods result in the
    * caller thread blocking in which case, having such method spawn another
    * thread is a waste of resources. So, for such situations, the caller can
    * pass in the {@link #COMPLETED} param so that the internal logic avoids
    * creating a separate thread, since the caller thread will block to get
    * the result immediately.
    *
    * <p>By default, all methods returning {@link CompletableFuture} are
    * asynchronous, hence using the {@link #ASYNC} future mode.
    *
    * @since 8.0
    */
   @Experimental
   enum FutureMode implements Param<FutureMode> {
      ASYNC, COMPLETED;

      public static final int ID = ParamIds.FUTURE_MODE_ID;

      @Override
      public int id() {
         return ID;
      }

      @Override
      public FutureMode get() {
         return this;
      }

      /**
       * Provides default future mode.
       */
      public static FutureMode defaultValue() {
         return ASYNC;
      }
   }

   /**
    * When a persistence store is attached to a cache, by default all write
    * operations, regardless of whether they are inserts, updates or removes,
    * are persisted to the store. Using {@link #SKIP}, the write operations
    * can skip the persistence store modification, applying the effects of
    * the write operation only in the in-memory contents of the caches in
    * the cluster.
    *
    * @apiNote Amongst the old flags, there's one that allows cache store
    * to be skipped for loading or reading. There's no need for such
    * per-invocation parameter here, because to avoid loading or reading from
    * the store, {@link org.infinispan.commons.api.functional.FunctionalMap.WriteOnlyMap}
    * operations can be called which do not read previous values from the
    * persistence store.
    *
    * @since 8.0
    */
   @Experimental
   enum PersistenceMode implements Param<PersistenceMode> {
      PERSIST, SKIP;

      public static final int ID = ParamIds.PERSISTENCE_MODE_ID;
      private static final PersistenceMode[] CACHED_VALUES = values();

      @Override
      public int id() {
         return ID;
      }

      @Override
      public PersistenceMode get() {
         return this;
      }

      /**
       * Provides default persistence mode.
       */
      public static PersistenceMode defaultValue() {
         return PERSIST;
      }

      public static PersistenceMode valueOf(int ordinal) {
         return CACHED_VALUES[ordinal];
      }
   }

}
