package org.infinispan.manager;

import static org.infinispan.commons.util.concurrent.CompletionStages.join;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.security.impl.Authorizer;

/**
 * The default implementation of {@link EmbeddedCacheManagerAdmin}
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public class DefaultCacheManagerAdmin implements EmbeddedCacheManagerAdmin {
   private final EmbeddedCacheManager cacheManager;
   private final GlobalConfigurationManager clusterConfigurationManager;
   private final Authorizer authorizer;
   private final EnumSet<AdminFlag> flags;
   private final Subject subject;

   DefaultCacheManagerAdmin(EmbeddedCacheManager cm, Authorizer authorizer, EnumSet<AdminFlag> flags,
                            Subject subject, GlobalConfigurationManager clusterConfigurationManager) {
      this.cacheManager = cm;
      this.authorizer = authorizer;
      this.clusterConfigurationManager = clusterConfigurationManager;
      this.flags = flags;
      this.subject = subject;
   }

   @Override
   public <K, V> Cache<K, V> createCache(String cacheName, Configuration configuration) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      join(clusterConfigurationManager.createCache(cacheName, configuration, flags));
      return cacheManager.getCache(cacheName);
   }

   @Override
   public <K, V> Cache<K, V> getOrCreateCache(String cacheName, Configuration configuration) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      join(clusterConfigurationManager.getOrCreateCache(cacheName, configuration, flags));
      return cacheManager.getCache(cacheName);
   }

   @Override
   public <K, V> Cache<K, V> createCache(String cacheName, String template) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      join(clusterConfigurationManager.createCache(cacheName, template, flags));
      return cacheManager.getCache(cacheName);
   }

   @Override
   public <K, V> Cache<K, V> getOrCreateCache(String cacheName, String template) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      join(clusterConfigurationManager.getOrCreateCache(cacheName, template, flags));
      return cacheManager.getCache(cacheName);
   }

   @Override
   public void createTemplate(String name, Configuration configuration) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      join(clusterConfigurationManager.createTemplate(name, configuration, flags));
   }

   @Override
   public Configuration getOrCreateTemplate(String name, Configuration configuration) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      join(clusterConfigurationManager.getOrCreateTemplate(name, configuration, flags));
      return cacheManager.getCacheConfiguration(name);
   }

   @Override
   public void removeTemplate(String name) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      join(clusterConfigurationManager.removeTemplate(name, flags));
   }

   @Override
   public void removeCache(String cacheName) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      join(clusterConfigurationManager.removeCache(cacheName, flags));
   }

   @Override
   public EmbeddedCacheManagerAdmin withFlags(AdminFlag... flags) {
      EnumSet<AdminFlag> newFlags = EnumSet.copyOf(this.flags);
      Collections.addAll(newFlags, flags);
      return new DefaultCacheManagerAdmin(cacheManager, authorizer, newFlags, subject, clusterConfigurationManager);
   }

   @Override
   public EmbeddedCacheManagerAdmin withFlags(EnumSet<AdminFlag> flags) {
      EnumSet<AdminFlag> newFlags = EnumSet.copyOf(this.flags);
      newFlags.addAll(flags);
      return new DefaultCacheManagerAdmin(cacheManager, authorizer, newFlags, subject, clusterConfigurationManager);
   }

   @Override
   public EmbeddedCacheManagerAdmin withSubject(Subject subject) {
      return new DefaultCacheManagerAdmin(cacheManager, authorizer, flags, subject, clusterConfigurationManager);
   }

   @Override
   public void updateConfigurationAttribute(String cacheName, String attributeName, String attributeValue) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      Cache<Object, Object> cache = cacheManager.getCache(cacheName);
      Configuration config = SecurityActions.getCacheConfiguration(cache.getAdvancedCache());
      Attribute<?> attribute = config.findAttribute(attributeName);
      attribute.fromString(attributeValue);
      EnumSet<AdminFlag> newFlags = flags.clone();
      newFlags.add(CacheContainerAdmin.AdminFlag.UPDATE);
      join(clusterConfigurationManager.getOrCreateCache(cacheName, config, newFlags));
   }

   @Override
   public void assignAlias(String aliasName, String cacheName) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      Cache<Object, Object> cache = cacheManager.getCache(cacheName);
      Cache<?, ?> oldAliased = cacheManager.getCache(aliasName);
      Configuration config = cache.getCacheConfiguration();
      Attribute<Set<String>> attribute = config.attributes().attribute(Configuration.ALIASES);
      Set<String> aliases = new HashSet<>(attribute.get());
      aliases.add(aliasName);
      attribute.set(aliases);
      EnumSet<AdminFlag> newFlags = flags.clone();
      newFlags.add(CacheContainerAdmin.AdminFlag.UPDATE);
      join(clusterConfigurationManager.getOrCreateCache(cacheName, config, newFlags)
            .thenCompose(ignore -> removeOldAlias(oldAliased)));
   }

   private CompletionStage<Void> removeOldAlias(Cache<?, ?> assigned) {
      if (assigned == null)
         return CompletableFutures.completedNull();

      Configuration configuration = assigned.getCacheConfiguration();
      EnumSet<AdminFlag> newFlags = flags.clone();
      newFlags.add(AdminFlag.UPDATE);
      return clusterConfigurationManager.getOrCreateCache(assigned.getName(), configuration, newFlags)
            .thenApply(CompletableFutures.toNullFunction());
   }
}
