package org.infinispan.commons.api.functional;

import java.util.concurrent.CompletableFuture;

/**
 * An easily extensible parameter that allows functional map operations to be
 * tweaked. Apart from {@link org.infinispan.commons.api.functional.Param.FutureMode}, examples
 * would include local-only parameter, skip-cache-store parameter and others.
 *
 * What makes {@link Param} different from {@link MetaParam} is that {@link Param}
 * values are never stored in the functional map. They merely act as ways to
 * tweak how operations are executed.
 *
 * Since {@link Param} instances control how the internals work, only
 * {@link Param} implementations by Infinispan will be supported.
 *
 * @apiNote This interface is equivalent to Infinispan's Flag, but it's more
 * powerful because it allows to pass a flag along with a value. Infinispan's
 * Flag are enum based which means no values can be passed along with value.
 *
 * @apiNote Since each param is an independent entity, it's easy to create
 * public versus private parameter distinction. When parameters are stored in
 * enums, it's more difficult to make such distinction.
 *
 * @param <P> type of parameter
 */
public interface Param<P> {

   /**
    * A parameter's identifier. Each parameter must have a different id.
    *
    * @implNote Why does a Param need an id? The most efficient way to store
    * multiple parameters is to keep them in an array. An integer-based id
    * means it can act as index of the array.
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
    * So, calling a method that returns {@link CompletableFuture} normally
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
    * By default, all methods returning {@link CompletableFuture} are
    * asynchronous, hence using the {@link #ASYNC} future mode.
    */
   enum FutureMode implements Param<FutureMode> {
      ASYNC {
         @Override
         public FutureMode get() {
            return ASYNC;
         }
      },
      COMPLETED {
         @Override
         public FutureMode get() {
            return COMPLETED;
         }
      };

      public static final int ID = ParamIds.FUTURE_MODE_ID;

      @Override
      public int id() {
         return ID;
      }

      /**
       * Provides default future mode.
       */
      public static FutureMode defaultValue() {
         return ASYNC;
      }
   }

}
