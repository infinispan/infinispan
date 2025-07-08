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

import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.cache.RealmIdentityCache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

public final class CaffeineRealmIdentityCache implements RealmIdentityCache {

   private final Cache<Principal, RealmIdentity> identityCache;
   private final Map<Principal, Set<Principal>> domainPrincipalMap;
   private final ReadWriteLock lock = new ReentrantReadWriteLock();

   public CaffeineRealmIdentityCache(int maxEntries, long maxAge) {
      checkMinimumParameter("maxEntries", 1, maxEntries);
      checkMinimumParameter("maxAge", -1, maxAge);
      identityCache = Caffeine.newBuilder().maximumSize(maxEntries).expireAfterWrite(maxAge, TimeUnit.MILLISECONDS).build();
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
            domainPrincipalMap.remove(identity.getRealmIdentityPrincipal()).forEach(identityCache.asMap()::remove);
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
}
