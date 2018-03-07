package org.infinispan.stream.impl;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Handler for storing iterators between invocations so that remote node can request data in chunks.
 * @author wburns
 * @since 9.2
 */
@Listener(observation = Listener.Observation.POST)
public class IteratorHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final Map<Object, CloseableIterator<?>> currentRequests = new ConcurrentHashMap<>();
   private final Map<Address, Set<Object>> ownerRequests = new ConcurrentHashMap<>();

   @Inject private EmbeddedCacheManager manager;

   /**
    * A {@link CloseableIterator} that also allows callers to attach {@link Runnable} instances to it, so that when
    * this iterator is closed, those <b>Runnable</b>s are also invoked.
    * @param <E>
    */
   public interface OnCloseIterator<E> extends CloseableIterator<E> {
      /**
       * Register a runnable to be invoked when this iterator is closed
       * @param runnable the runnable to run
       * @return this iterator again
       */
      OnCloseIterator<E> onClose(Runnable runnable);
   }

   @ViewChanged
   public void viewChange(ViewChangedEvent event) {
      List<Address> newMembers = event.getNewMembers();
      Iterator<Map.Entry<Address, Set<Object>>> iter = ownerRequests.entrySet().iterator();
      while (iter.hasNext()) {
         Map.Entry<Address, Set<Object>> entry = iter.next();
         Address owner = entry.getKey();
         // If an originating node is no longer here then we have to close their iterators
         if (!newMembers.contains(owner)) {
            Set<Object> ids = entry.getValue();
            if (!ids.isEmpty()) {
               log.tracef("View changed and no longer contains %s, closing %s iterators", owner, ids);
               ids.forEach(this::closeIterator);
            }
            iter.remove();
         }
      }
   }

   @Start
   public void start() {
      manager.addListener(this);
   }

   @Stop
   public void stop() {
      // If our cache is stopped we should remove our listener, since this doesn't mean the cache manager is stopped
      manager.removeListener(this);
   }

   /**
    * Starts an iteration process from the given stream that converts the stream to a subsequent stream using the given
    * intermediate operations and then creates a managed iterator for the caller to subsequently retrieve.
    * @param streamSupplier the supplied stream
    * @param intOps the intermediate operations to perform on the stream
    * @param <Original> original stream type
    * @param <E> resulting type
    * @return the id of the managed iterator
    */
   public <Original, E> OnCloseIterator<E> start(Address origin, Supplier<Stream<Original>> streamSupplier,
         Iterable<IntermediateOperation> intOps, Object requestId) {
      if (trace) {
         log.tracef("Iterator requested from %s using requestId %s", origin, requestId);
      }
      BaseStream stream = streamSupplier.get();
      for (IntermediateOperation intOp : intOps) {
         stream = intOp.perform(stream);
      }

      OnCloseIterator<E> iter = new IteratorCloser<>((CloseableIterator<E>) Closeables.iterator(stream));
      // When the iterator is closed make sure to clean up
      iter.onClose(() -> closeIterator(origin, requestId));
      currentRequests.put(requestId, iter);
      // This will be null if there have been no iterators for this node.
      // If the originating node died before we start this iterator this could be null as well. In this case the
      // iterator will be closed on the next view change.
      Set<Object> ids = ownerRequests.computeIfAbsent(origin, k -> new ConcurrentHashSet<>());
      ids.add(requestId);
      return iter;
   }

   public <E> CloseableIterator<E> getIterator(Object requestId) {
      CloseableIterator<E> closeableIterator = (CloseableIterator<E>) currentRequests.get(requestId);
      if (closeableIterator == null) {
         throw new IllegalStateException("Iterator for requestId " + requestId + " doesn't exist!");
      }
      if (trace) {
         log.tracef("Iterator retrieved using requestId %s", requestId);
      }
      return closeableIterator;
   }

   /**
    * Returns how many iterators are currently open
    * @return how many iterators are currently open
    */
   public int openIterators() {
      return currentRequests.size();
   }

   /**
    * Invoked to have handler forget about given iterator they requested. This should be invoked when an
    * {@link Iterator} has been found to have completed {@link Iterator#hasNext()} is false or if the caller
    * wishes to stop a given iteration early. If an iterator is fully iterated upon (ie. {@link Iterator#hasNext()}
    * returns false, that invocation will also close resources related to the iterator.
    * @param requestId the id of the iterator
    */
   public void closeIterator(Address origin, Object requestId) {
      Set<Object> ids = ownerRequests.get(origin);
      if (ids != null) {
         ids.remove(requestId);
      }
      closeIterator(requestId);
   }

   private void closeIterator(Object requestId) {
      CloseableIterator<?> closeableIterator = currentRequests.remove(requestId);
      if (closeableIterator != null) {
         if (trace) {
            log.tracef("Closing iterator using requestId %s", requestId);
         }
         closeableIterator.close();
      }
   }

   private class IteratorCloser<E> implements OnCloseIterator<E> {
      private final CloseableIterator<E> closeableIterator;
      private volatile Runnable closeRunnable;

      IteratorCloser(CloseableIterator<E> closeableIterator) {
         this.closeableIterator = closeableIterator;
      }

      @Override
      public boolean hasNext() {
         boolean hasNext = closeableIterator.hasNext();
         if (!hasNext) {
            close();
         }
         return hasNext;
      }

      @Override
      public E next() {
         // Make sure to double check hasNext in case if user didn't call it before next.
         hasNext();
         return closeableIterator.next();
      }

      @Override
      public void forEachRemaining(Consumer<? super E> action) {
         closeableIterator.forEachRemaining(action);
         close();
      }

      @Override
      public void close() {
         closeableIterator.close();
         // We only want to run the runnable once, in case if we end up closing twice
         Runnable onClose = closeRunnable;
         if (onClose != null) {
            closeRunnable = null;
            onClose.run();
         }
      }

      @Override
      public IteratorCloser<E> onClose(Runnable closeHandler) {
         if (this.closeRunnable == null) {
            this.closeRunnable = closeHandler;
         } else {
            this.closeRunnable = Util.composeWithExceptions(this.closeRunnable, closeHandler);
         }
         return this;
      }
   }
}
