package org.infinispan.distribution.group.impl;

import static org.infinispan.commons.util.ReflectionUtil.invokeMethod;
import static org.infinispan.transaction.impl.WriteSkewHelper.addVersionRead;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.infinispan.CacheStream;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.group.Group;
import org.infinispan.distribution.group.Grouper;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@Scope(Scopes.NAMED_CACHE)
public class GroupManagerImpl implements GroupManager {

   private static final Log log = LogFactory.getLog(GroupManagerImpl.class);

   @Inject
   ComponentRegistry componentRegistry;
   @Inject
   ComponentRef<EntryFactory> entryFactory;
   @Inject
   ComponentRef<VersionGenerator> versionGenerator;

   private final ConcurrentMap<Class<?>, GroupMetadata> groupMetadataCache;
   private final List<Grouper<?>> groupers;

   public GroupManagerImpl(Configuration configuration) {
      groupMetadataCache = new ConcurrentHashMap<>();
      if (configuration.clustering().hash().groups().groupers() != null) {
         groupers = configuration.clustering().hash().groups().groupers();
      } else {
         groupers = Collections.emptyList();
      }
   }

   @Override
   public Object getGroup(Object key) {
      GroupMetadata metadata = getMetadata(key);
      if (metadata != null) {
         return applyGroupers(metadata.getGroup(key), key);
      } else
         return applyGroupers(null, key);
   }

   @Override
   public <K, V> Map<K, V> collect(CacheStream<? extends CacheEntry<K, V>> stream, InvocationContext ctx, String groupName) {
      CacheEntryGroupPredicate<K> predicate = new CacheEntryGroupPredicate<>(groupName);
      predicate.inject(componentRegistry);
      List<CacheEntry<K, V>> list = stream.filterKeySegments(IntSets.immutableSet(groupSegment(groupName)))
            .filter(predicate)
            .collect(Collectors::toList);
      return ctx.isInTxScope() ? handleTxGetGroup((TxInvocationContext<?>) ctx, list, groupName) : handleNoTxGetGroup(list, groupName);
   }

   private <V, K> Map<K, V> handleNoTxGetGroup(List<? extends CacheEntry<K, V>> entries, String groupName) {
      boolean trace = log.isTraceEnabled();
      Map<K, V> group = new HashMap<>();
      entries.forEach(e -> {
         if (trace) {
            log.tracef("Found entry belonging to group %s: %s", groupName, e);
         }
         group.put(e.getKey(), e.getValue());
      });
      return group;
   }

   private <V, K> Map<K, V> handleTxGetGroup(TxInvocationContext<?> ctx, List<? extends CacheEntry<K, V>> entries, String groupName) {
      boolean trace = log.isTraceEnabled();
      synchronized (ctx) {
         Map<K, V> group = new HashMap<>();
         entries.forEach(e -> {
            if (ctx.lookupEntry(e.getKey()) == null) {
               entryFactory.running().wrapExternalEntry(ctx, e.getKey(), e, true, false);
               addVersionRead(ctx, e, e.getKey(), versionGenerator.running(), log);
            }
            if (trace) {
               log.tracef("Found entry belonging to group %s: %s", groupName, e);
            }
            group.put(e.getKey(), e.getValue());
         });
         return group;
      }
   }

   private int groupSegment(String groupName) {
      KeyPartitioner keyPartitioner = componentRegistry.getComponent(KeyPartitioner.class);
      if (keyPartitioner instanceof GroupingPartitioner) {
         return ((GroupingPartitioner) keyPartitioner).unwrap().getSegment(groupName);
      } else {
         return keyPartitioner.getSegment(groupName);
      }
   }


   @FunctionalInterface
   private interface GroupMetadata {
      GroupMetadata NONE = instance -> null;

      Object getGroup(Object instance);
   }

   private static class GroupMetadataImpl implements GroupMetadata {
      private final Method method;

      GroupMetadataImpl(Method method) {
         if (method.getParameterCount() > 0)
            throw new IllegalArgumentException(Util.formatString("@Group method %s must have zero arguments", method));
         this.method = method;
      }

      @Override
      public Object getGroup(Object instance) {
         method.setAccessible(true);
         return invokeMethod(instance, method, Util.EMPTY_OBJECT_ARRAY);
      }
   }

   private static GroupMetadata createGroupMetadata(Class<?> clazz) {
      Collection<Method> possibleMethods = ReflectionUtil.getAllMethods(clazz, Group.class);
      if (possibleMethods.isEmpty())
         return GroupMetadata.NONE;
      else if (possibleMethods.size() == 1)
         return new GroupMetadataImpl(possibleMethods.iterator().next());
      else
         throw new IllegalStateException(Util.formatString("Cannot define more that one @Group method for class hierarchy rooted at %s", clazz.getName()));
   }

   private Object applyGroupers(Object group, Object key) {
      for (Grouper<?> grouper : groupers) {
         if (grouper.getKeyType().isAssignableFrom(key.getClass()))
            //noinspection unchecked
            group = ((Grouper<Object>) grouper).computeGroup(key, group);
      }
      return group;
   }

   private GroupMetadata getMetadata(Object key) {
      Class<?> keyClass = key.getClass();
      GroupMetadata groupMetadata = groupMetadataCache.get(keyClass);
      if (groupMetadata == null) {
         //this is not ideal as it is possible for the group metadata to be redundantly calculated several times.
         //however profiling showed that using the Map<Class,Future> cache-approach is significantly slower on
         // the long run
         groupMetadata = createGroupMetadata(keyClass);
         GroupMetadata previous = groupMetadataCache.putIfAbsent(keyClass, groupMetadata);
         if (previous != null) {
            // in case another thread added a metadata already, discard what we created and reuse the existing.
            return previous;
         }
      }
      return groupMetadata;
   }
}
