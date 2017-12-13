package org.infinispan.manager;

import java.util.EnumSet;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationHelper;

/**
 * The default implementation of {@link EmbeddedCacheManagerAdmin}
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class DefaultCacheManagerAdmin implements EmbeddedCacheManagerAdmin {
   private final EmbeddedCacheManager cacheManager;
   private final GlobalConfigurationManager clusterConfigurationManager;
   private final AuthorizationHelper authzHelper;
   private final EnumSet<AdminFlag> flags;

   DefaultCacheManagerAdmin(EmbeddedCacheManager cm, AuthorizationHelper authzHelper, EnumSet<AdminFlag> flags) {
      this.cacheManager = cm;
      this.authzHelper = authzHelper;
      this.clusterConfigurationManager = cm.getGlobalComponentRegistry().getComponent(GlobalConfigurationManager.class);
      this.flags = flags;
   }

   @Override
   public <K, V> Cache<K, V> createCache(String cacheName, Configuration configuration) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      clusterConfigurationManager.createCache(cacheName, configuration, flags);
      return cacheManager.getCache(cacheName);
   }

   @Override
   public <K, V> Cache<K, V> getOrCreateCache(String cacheName, Configuration configuration) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      clusterConfigurationManager.getOrCreateCache(cacheName, configuration, flags);
      return cacheManager.getCache(cacheName);
   }

   @Override
   public <K, V> Cache<K, V> createCache(String cacheName, String template) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      clusterConfigurationManager.createCache(cacheName, template, flags);
      return cacheManager.getCache(cacheName);
   }

   @Override
   public <K, V> Cache<K, V> getOrCreateCache(String cacheName, String template) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      clusterConfigurationManager.getOrCreateCache(cacheName, template, flags);
      return cacheManager.getCache(cacheName);
   }

   @Override
   public void removeCache(String cacheName) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      clusterConfigurationManager.removeCache(cacheName, flags);
   }

   @Override
   public EmbeddedCacheManagerAdmin withFlags(AdminFlag... flags) {
      EnumSet<AdminFlag> newFlags = EnumSet.copyOf(this.flags);
      for(AdminFlag flag : flags) newFlags.add((flag));
      return new DefaultCacheManagerAdmin(cacheManager, authzHelper, newFlags);
   }

   @Override
   public EmbeddedCacheManagerAdmin withFlags(EnumSet<AdminFlag> flags) {
      EnumSet<AdminFlag> newFlags = EnumSet.copyOf(this.flags);
      newFlags.addAll(flags);
      return new DefaultCacheManagerAdmin(cacheManager, authzHelper, newFlags);
   }
}
