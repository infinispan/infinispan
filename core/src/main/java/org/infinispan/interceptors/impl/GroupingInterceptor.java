package org.infinispan.interceptors.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.group.impl.GroupFilter;
import org.infinispan.distribution.group.impl.GroupManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;

/**
 * An interceptor that keeps track of the keys
 * added/removed during the processing of a {@link GetKeysInGroupCommand}
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class GroupingInterceptor extends DDAsyncInterceptor {

   private CacheNotifier<?, ?> cacheNotifier;
   private GroupManager groupManager;
   private InternalEntryFactory factory;
   private boolean isPassivationEnabled;
   private DistributionManager distributionManager;

   @Inject
   public void injectDependencies(CacheNotifier<?, ?> cacheNotifier, GroupManager groupManager,
                                  InternalEntryFactory factory, Configuration configuration,
                                  DistributionManager distributionManager) {
      this.cacheNotifier = cacheNotifier;
      this.groupManager = groupManager;
      this.factory = factory;
      this.isPassivationEnabled = configuration.persistence().passivation();
      this.distributionManager = distributionManager;
   }

   @Override
   public Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      final String groupName = command.getGroupName();
      //no need to contact the primary owner if we are a backup owner.
      command.setGroupOwner(distributionManager.getCacheTopology().isWriteOwner(groupName));
      if (!command.isGroupOwner() || !isPassivationEnabled) {
         return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
            if (rv instanceof List) {
               //noinspection unchecked
               filter((List<CacheEntry>) rv);
            }
         });
      }

      KeyListener listener = new KeyListener(groupName, groupManager, factory);
      //this is just to try to make the snapshot the most recent possible by picking some modification on the fly.
      cacheNotifier.addListener(listener);
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> {
         cacheNotifier.removeListener(listener);

         if (rv instanceof List) {
            //noinspection unchecked
            ((List) rv).addAll(listener.activatedKeys);
            //noinspection unchecked
            filter((List<CacheEntry>) rv);
         } else if (rv instanceof Map) {
            for (CacheEntry entry : listener.activatedKeys) {
               //noinspection unchecked
               ((Map) rv).put(entry.getKey(), entry.getValue());
            }
         }
      });
   }

   private void filter(List<CacheEntry> list) {
      for (int i = 0; i < list.size(); ++i) {
         CacheEntry entry = list.get(i);
         if (entry instanceof MVCCEntry) {
            list.set(i, factory.create(entry));
         }
      }
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
