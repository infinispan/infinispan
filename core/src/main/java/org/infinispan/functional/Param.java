package org.infinispan.functional;

import org.infinispan.commons.util.Experimental;

/**
 * An easily extensible parameter that allows functional map operations to be
 * tweaked. Examples would include local-only parameter, skip-cache-store parameter and others.
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
    * the store, Write-only operations can be called which do not read previous
    * values from the persistence store.
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

   /**
    * Normally the cache has to acquire locks during any write operation to guarantee
    * its correctness. If the application is sure that no concurrent operation occurs,
    * it is possible to increase performance by setting this param to {@link #SKIP}.
    * The result of any operation without locking is undefined under presence of concurrent
    * writes.
    */
   @Experimental
   enum LockingMode implements Param<LockingMode> {
      LOCK, SKIP;

      public static final int ID = ParamIds.LOCKING_MODE_ID;
      private static final LockingMode[] CACHED_VALUES = values();


      @Override
      public int id() {
         return ID;
      }

      @Override
      public LockingMode get() {
         return this;
      }

      public static LockingMode defaultValue() {
         return LOCK;
      }

      public static LockingMode valueOf(int ordinal) {
         return CACHED_VALUES[ordinal];
      }
   }

   /**
    * Defines where is the command executed.
    */
   @Experimental
   enum ExecutionMode implements Param<ExecutionMode> {
      /**
       * Command is executed on its owners, in transactional mode in the context, too, but there it is not persisted.
       */
      CLUSTER,
      /**
       * Command is executed only locally, it is not sent to remote nodes. If the command is a write and this node
       * is not an owner of given entry, the entry is not stored in the cache. If the command reads a value and
       * the entry is not available locally, null entry is provided instead.
       */
      LOCAL;
      // Other options: context-only write, write without remote read (SKIP_REMOTE_LOOKUP)...

      public static final int ID = ParamIds.EXECUTION_MODE_ID;
      private static final ExecutionMode[] CACHED_VALUES = values();

      @Override
      public int id() {
         return ID;
      }

      @Override
      public ExecutionMode get() {
         return this;
      }

      public static ExecutionMode defaultValue() {
         return CLUSTER;
      }

      public static ExecutionMode valueOf(int ordinal) {
         return CACHED_VALUES[ordinal];
      }
   }
}
