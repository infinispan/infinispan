package org.infinispan.multimap.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LockingMode;

public abstract class BaseDistributedMultimapTest<T, V> extends BaseDistFunctionalTest<String, V> {

   protected Map<Address, T> cluster = new HashMap<>();
   protected boolean fromOwner;

   protected abstract T create(EmbeddedMultimapCacheManager<String, V> manager);

   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();

      for (EmbeddedCacheManager cacheManager : cacheManagers) {
         EmbeddedMultimapCacheManager<String, V> multimapCacheManager = (EmbeddedMultimapCacheManager<String, V>) EmbeddedMultimapCacheManagerFactory.from(cacheManager);
         cluster.put(cacheManager.getAddress(), create(multimapCacheManager));
      }
   }

   protected final Object[] factory(Supplier<BaseDistributedMultimapTest<T, V>> constructor) {
      return Stream.of(Boolean.TRUE, Boolean.FALSE)
            .flatMap(tx -> {
               if (tx) {
                  return Stream.of(transactionalFactory(constructor));
               }

               return Stream.of(
                     constructor.get().fromOwner(false).numOwners(1).cacheMode(CacheMode.DIST_SYNC).transactional(false),
                     constructor.get().fromOwner(true).numOwners(1).cacheMode(CacheMode.DIST_SYNC).transactional(false)
               );
            })
            .toArray();
   }

   protected final Object[] transactionalFactory(Supplier<BaseDistributedMultimapTest<T, V>> constructor) {
      return Arrays.stream(LockingMode.values())
            .flatMap(mode -> Stream.of(
                  constructor.get().fromOwner(false).numOwners(1).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(mode),
                  constructor.get().fromOwner(true).numOwners(1).cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(mode)
            ))
            .toArray();
   }

   protected final <O extends BaseDistributedMultimapTest<T, V>> O self() {
      return (O) this;
   }

   protected <O extends BaseDistributedMultimapTest<T, V>> O fromOwner(boolean value) {
      this.fromOwner = value;
      return self();
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "fromOwner");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), fromOwner);
   }

   @Override
   protected SerializationContextInitializer getSerializationContext() {
      return MultimapSCI.INSTANCE;
   }

   protected final String getEntryKey() {
      return fromOwner
            ? getStringKeyForCache(cache(0, cacheName))
            : getStringKeyForCache(cache(cluster.size() - 1, cacheName));
   }

   protected final T getMultimapMember() {
      return getMultimapMember(0);
   }

   protected final T getMultimapMember(int index) {
      return Objects.requireNonNull(cluster.get(manager(index).getAddress()));
   }
}
