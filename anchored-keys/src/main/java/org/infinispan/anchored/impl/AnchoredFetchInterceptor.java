package org.infinispan.anchored.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.impl.BaseRpcInterceptor;
import org.infinispan.notifications.Listener;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidSingleResponseCollector;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Fetch the real value from the anchor owner in an anchored cache.
 *
 * @author Dan Berindei
 * @since 11
 */
@Listener
@Scope(Scopes.NAMED_CACHE)
public class AnchoredFetchInterceptor extends BaseRpcInterceptor {

   private static Log log = LogFactory.getLog(AnchoredFetchInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject CommandsFactory cf;
   @Inject EntryFactory entryFactory;
   @Inject AnchorManager anchorManager;

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      boolean isWrite = false;
      CompletionStage<Void> fetchStage = fetchContextValues(ctx, command, isWrite);
      return asyncInvokeNext(ctx, command, fetchStage);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      Map<Object, Address> existingLocations = new HashMap<>();
      ctx.forEachValue((key, cacheEntry) -> {
         if (cacheEntry.getValue() instanceof Address) {
            existingLocations.put(key, ((Address) cacheEntry.getValue()));
         }
      });
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, throwable) -> {
         // Replace the value in the context entry if the local node is not an owner
         rCtx.forEachValue((key, cacheEntry) -> {
            if (cacheEntry.isChanged()) {
               if (cacheEntry.getValue() instanceof Address)
                  return;

               Address location = existingLocations.get(key);
               if (location == null) {
                  location = anchorManager.getCurrentWriter();
               }
               if (rpcManager.getAddress().equals(location))
                  return;

               cacheEntry.setValue(location);
            }
         });
      });
   }

   private CompletionStage<Void> fetchContextValues(InvocationContext ctx, GetKeyValueCommand command,
                                                    boolean isWrite) {
      AggregateCompletionStage<Void> fetchStage = CompletionStages.aggregateCompletionStage();
      List<CompletionStage<CacheEntry<?, ?>>> stages = new ArrayList<>(ctx.lookedUpEntriesCount());
      ctx.forEachEntry((key, ctxEntry) -> {
         if (!(ctxEntry.getValue() instanceof Address))
            return;

         ClusteredGetCommand getCommand =
               cf.buildClusteredGetCommand(key, command.getSegment(), command.getFlagsBitSet());
         getCommand.setTopologyId(0);
         getCommand.setWrite(isWrite);

         Address realOwner = (Address) ctxEntry.getValue();
         FetchResponseCollector collector = new FetchResponseCollector(key);
         CompletionStage<CacheEntry<?, ?>> stage =
               rpcManager.invokeCommand(realOwner, getCommand, collector, rpcManager.getSyncRpcOptions());
         stages.add(stage);
         fetchStage.dependsOn(stage);
      });

      return fetchStage.freeze().thenAccept(__ -> {
         // The stages in the list are all done now
         for (CompletionStage<CacheEntry<?, ?>> stage : stages) {
            CacheEntry<?, ?> ownerEntry = CompletionStages.join(stage);
            entryFactory.wrapExternalEntry(ctx, ownerEntry.getKey(), ownerEntry, true, isWrite);
         }
      });
   }

   private static class FetchResponseCollector extends ValidSingleResponseCollector<CacheEntry<?, ?>> {
      private final Object key;

      public FetchResponseCollector(Object key) {
         this.key = key;
      }

      @Override
      protected CacheEntry<?, ?> withValidResponse(Address sender, ValidResponse response) {
         Object responseValue = response.getResponseValue();
         CacheEntry<Object, Object> ice;
         if (responseValue == null) {
            return NullCacheEntry.getInstance();
         } else {
            return ((InternalCacheValue) responseValue).toInternalCacheEntry(key);
         }
      }

      @Override
      protected CacheEntry<?, ?> targetNotFound(Address sender) {
         return null;
      }
   }
}
