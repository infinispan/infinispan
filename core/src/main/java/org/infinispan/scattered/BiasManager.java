package org.infinispan.scattered;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.infinispan.remoting.transport.Address;

/**
 * This component tracks if this node can read the data stored locally despite not being an owner
 * and which nodes will read the local data that is primary-owned by this node.
 *
 * Since tracking these remote nodes has a memory overhead this component can deliberately
 * revoke the bias sending the {@link org.infinispan.commands.remote.RevokeBiasCommand}.
 */
public interface BiasManager {
   /**
    * Allow reading local data if the topology is still actual.
    * @param key
    * @param topologyId
    */
   void addLocalBias(Object key, int topologyId);

   /**
    * Stop reading local data.
    * @param key
    */
   void revokeLocalBias(Object key);

   /**
    * Stop reading local data from this segment.
    * @param segments
    */
   void revokeLocalBiasForSegments(Set<Integer> segments);

   /**
    * Check if we can read local data and update last-read timestamp for this key.
    * @param key
    * @return
    */
   boolean hasLocalBias(Object key);

   List<Address> getRemoteBias(Object key); // testing only

   /**
    * Check if there are any nodes that have local bias, and starting replacing them
    * with the provided address. The caller can find out the currently biased nodes
    * from {@link Revocation#biased()} and is expected to send
    * {@link org.infinispan.commands.remote.RevokeBiasCommand} to the holders and when
    * this completes call {@link Revocation#complete()} or {@link Revocation#fail()}.
    *
    * This method returns <code>null</code> when there is no need to revoke any bias
    * on remote nodes. When {@link Revocation#shouldRevoke()} returns false, the caller
    * should set up a handler through {@link Revocation#handleCompose(Supplier)} and
    * retry calling this method in the handler.
    *
    * @param key
    * @param newBiased
    * @return
    */
   Revocation startRevokingRemoteBias(Object key, Address newBiased);

   /**
    * Notify the component that the node is reading the biased entry and the bias
    * should not be revoked unless necessary.
    *
    * @param key
    * @param origin
    */
   void renewRemoteBias(Object key, Address origin);

   /**
    * The cache has been cleared and therefore all biases are forgotten.
    */
   void clear();

   interface Revocation extends BiConsumer<Object, Throwable> {
      boolean shouldRevoke();
      List<Address> biased();
      void complete();
      void fail();

      CompletionStage<?> toCompletionStage();

      /**
       * Similar to {@link CompletableFuture#thenCompose(Function)}, returns future provided by the supplier
       * after the current revocation has been finished
       * @param supplier
       * @return
       */
      <T> CompletableFuture<T> handleCompose(Supplier<CompletionStage<T>> supplier);

      @Override
      default void accept(Object nil, Throwable throwable) {
         if (throwable == null) {
            complete();
         } else {
            fail();
         }
      }
   }
}
