package org.infinispan.server.security.realm;

import static org.wildfly.common.Assert.checkMinimumParameter;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jspecify.annotations.Nullable;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.cache.RealmIdentityCache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

public final class CaffeineRealmIdentityCache implements RealmIdentityCache {

   private final Cache<Principal, RealmIdentity> identityCache;
   private final Map<Principal, Set<Principal>> domainPrincipalMap;
   private final ReadWriteLock lock = new ReentrantReadWriteLock();

   public CaffeineRealmIdentityCache(int maxEntries, long maxAge) {
      checkMinimumParameter("maxEntries", 1, maxEntries);
      checkMinimumParameter("maxAge", -1, maxAge);
      Caffeine<Object, Object> builder = Caffeine.newBuilder();
      builder.maximumSize(maxEntries).evictionListener(new RemovalListener<Principal, RealmIdentity>() {
         @Override
         public void onRemoval(@Nullable Principal key, @Nullable RealmIdentity value, RemovalCause cause) {
            if (cause.wasEvicted()) {
               Set<Principal> removed = domainPrincipalMap.remove(value.getRealmIdentityPrincipal());
               if (removed != null) {
                  removed.forEach(identityCache.asMap()::remove);
               }
            }
         }
      });
      if (maxAge > 0) {
         builder.expireAfterWrite(maxAge, TimeUnit.MILLISECONDS);
      }
      identityCache = builder.build();
      domainPrincipalMap = new HashMap<>();
   }

   @Override
   public void put(Principal key, RealmIdentity identity) {
      lock.writeLock().lock();
      try {
         RealmIdentity cached = identityCache.asMap().computeIfAbsent(key, principal -> {
            domainPrincipalMap.computeIfAbsent(identity.getRealmIdentityPrincipal(), principal1 -> {
               Set<Principal> principals = ConcurrentHashMap.newKeySet();
               principals.add(key);
               return principals;
            });
            return identity;
         });
         domainPrincipalMap.get(cached.getRealmIdentityPrincipal()).add(key);
      } finally {
         lock.writeLock().unlock();
      }
   }

   @Override
   public RealmIdentity get(Principal key) {
      lock.readLock().lock();
      try {
         RealmIdentity cached = identityCache.getIfPresent(key);
         if (cached != null) {
            return cached;
         }
         Set<Principal> domainPrincipal = domainPrincipalMap.get(key);
         if (domainPrincipal != null) {
            return identityCache.getIfPresent(domainPrincipal.iterator().next());
         }
         return null;
      } finally {
         lock.readLock().unlock();
      }
   }

   @Override
   public void remove(Principal key) {
      lock.writeLock().lock();
      try {
         RealmIdentity identity = identityCache.asMap().remove(key);
         if (identity != null) {
            Set<Principal> removed = domainPrincipalMap.remove(identity.getRealmIdentityPrincipal());
            if (removed != null) {
               removed.forEach(identityCache.asMap()::remove);
            }
         } else {
            Set<Principal> principals = domainPrincipalMap.remove(key);
            if (principals != null) {
               principals.forEach(identityCache.asMap()::remove);
            }
         }
      } finally {
         lock.writeLock().unlock();
      }
   }

   @Override
   public void clear() {
      lock.writeLock().lock();
      try {
         identityCache.invalidateAll();
         domainPrincipalMap.clear();
      } finally {
         lock.writeLock().unlock();
      }
   }

   Cache<Principal, RealmIdentity> identityCache() {
      return identityCache;
   }
}
