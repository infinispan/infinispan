package org.infinispan.interceptors.impl;

import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.group.GroupFilter;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDSequentialInterceptor;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * An interceptor that keeps track of the keys
 * added/removed during the processing of a {@link GetKeysInGroupCommand}
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class GroupingInterceptor extends DDSequentialInterceptor {

   private CacheNotifier<?, ?> cacheNotifier;
   private GroupManager groupManager;
   private InternalEntryFactory factory;
   private boolean isPassivationEnabled;

   @Inject
   public void injectDependencies(CacheNotifier<?, ?> cacheNotifier, GroupManager groupManager,
                                  InternalEntryFactory factory, Configuration configuration) {
      this.cacheNotifier = cacheNotifier;
      this.groupManager = groupManager;
      this.factory = factory;
      this.isPassivationEnabled = configuration.persistence().passivation();
   }

   @Override
   public CompletableFuture<Void> visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      final String groupName = command.getGroupName();
      command.setGroupOwner(isGroupOwner(groupName));
      if (!command.isGroupOwner() || !isPassivationEnabled) {
         Object result = ctx.forkInvocationSync(command);
         if (result instanceof List) {
            //noinspection unchecked
            filter((List<CacheEntry>) result);
         }
         return ctx.shortCircuit(result);
      }
      KeyListener listener = new KeyListener(groupName, groupManager, factory);
      //this is just to try to make the snapshot the most recent possible by picking some modification on the fly.
      cacheNotifier.addListener(listener);
      try {
         Object result = ctx.forkInvocationSync(command);
         if (result instanceof List) {
            //noinspection unchecked
            ((List) result).addAll(listener.activatedKeys);
            //noinspection unchecked
            filter((List<CacheEntry>) result);
         } else if (result instanceof Map) {
            for (CacheEntry entry : listener.activatedKeys) {
               //noinspection unchecked
               ((Map) result).put(entry.getKey(), entry.getValue());
            }
         }
         return ctx.shortCircuit(result);
      } finally {
         cacheNotifier.removeListener(listener);
      }
   }

   private void filter(List<CacheEntry> list) {
      for (int i = 0; i < list.size(); ++i) {
         CacheEntry entry = list.get(i);
         if (entry instanceof MVCCEntry) {
            list.set(i, factory.create(entry));
         }
      }
   }

   private boolean isGroupOwner(String groupName) {
      //no need to contact the primary owner if we are a backup owner.
      return groupManager.isOwner(groupName);
   }

   @Listener
   public static class KeyListener {

      private final ConcurrentLinkedQueue<CacheEntry> activatedKeys;
      private final GroupFilter<Object> filter;
      private final InternalEntryFactory factory;

      public KeyListener(String groupName, GroupManager groupManager, InternalEntryFactory factory) {
         this.factory = factory;
         filter = new GroupFilter<>(groupName, groupManager);
         activatedKeys = new ConcurrentLinkedQueue<>();
      }

      @CacheEntryActivated
      public void handleRemove(CacheEntryActivatedEvent<?, ?> event) {
         final Object key = event.getKey();
         if (filter.accept(key)) {
            activatedKeys.add(factory.create(key, event.getValue(), event.getMetadata()));
         }
      }
   }
}
