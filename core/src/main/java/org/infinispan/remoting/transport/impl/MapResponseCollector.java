package org.infinispan.remoting.transport.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.util.Experimental;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.concurrent.CompletableFutures;

/**
 * Response collector supporting {@link JGroupsTransport#invokeRemotelyAsync(Collection, ReplicableCommand, ResponseMode, long, ResponseFilter, DeliverOrder, boolean)}.
 *
 * @author Dan Berindei
 * @since 9.2
 */
@Experimental
public abstract class MapResponseCollector extends ValidResponseCollector<Map<Address, Response>> {
   private static final int DEFAULT_EXPECTED_SIZE = 4;

   protected final HashMap<Address, Response> map;
   private Exception exception;

   public static MapResponseCollector validOnly(int expectedSize) {
      return new ValidOnly(expectedSize);
   }

   public static MapResponseCollector validOnly() {
      return new ValidOnly(DEFAULT_EXPECTED_SIZE);
   }

   public static MapResponseCollector ignoreLeavers(int expectedSize) {
      return new IgnoreLeavers(expectedSize);
   }

   public static MapResponseCollector ignoreLeavers() {
      return new IgnoreLeavers(DEFAULT_EXPECTED_SIZE);
   }

   public static MapResponseCollector ignoreLeavers(boolean ignoreLeavers, int expectedSize) {
      return ignoreLeavers ? ignoreLeavers(expectedSize) : validOnly(expectedSize);
   }

   public static MapResponseCollector ignoreLeavers(boolean ignoreLeavers) {
      return ignoreLeavers ? ignoreLeavers() : validOnly();
   }

   private MapResponseCollector(int expectedSize) {
      this.map = new HashMap<>(expectedSize);
   }

   @Override
   protected Map<Address, Response> addException(Address sender, Exception exception) {
      recordException(ResponseCollectors.wrapRemoteException(sender, exception));
      return null;
   }

   void recordException(Exception e) {
      if (this.exception == null) {
         this.exception = e;
      } else {
         this.exception.addSuppressed(e);
      }
   }

   @Override
   protected Map<Address, Response> addValidResponse(Address sender, ValidResponse response) {
      map.put(sender, response);
      return null;
   }

   @Override
   public Map<Address, Response> finish() {
      if (exception != null) {
         throw CompletableFutures.asCompletionException(exception);
      }
      return map;
   }

   private static class ValidOnly extends MapResponseCollector {
      ValidOnly(int expectedSize) {
         super(expectedSize);
      }

      @Override
      protected Map<Address, Response> addTargetNotFound(Address sender) {
         recordException(ResponseCollectors.remoteNodeSuspected(sender));
         return null;
      }
   }

   private static class IgnoreLeavers extends MapResponseCollector {
      IgnoreLeavers(int expectedSize) {
         super(expectedSize);
      }

      @Override
      protected Map<Address, Response> addTargetNotFound(Address sender) {
         map.put(sender, CacheNotFoundResponse.INSTANCE);
         return null;
      }
   }
}
