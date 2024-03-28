package org.infinispan.manager;

import static org.infinispan.util.concurrent.CompletionStages.join;

import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.security.AuthorizationPermission;
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
   public <K, V> CompletionStage<Cache<K, V>> createCacheAsync(String name, Configuration configuration) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      return clusterConfigurationManager.createCache(name, configuration, flags).thenCompose(__ -> cacheManager.getCacheAsync(name));
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
   public <K, V> CompletionStage<Cache<K, V>> createCacheAsync(String name, String template) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      return clusterConfigurationManager.createCache(name, template, flags).thenCompose(__ -> cacheManager.getCacheAsync(name));
   }

   @Override
   public <K, V> Cache<K, V> getOrCreateCache(String cacheName, String template) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      join(clusterConfigurationManager.getOrCreateCache(cacheName, template, flags));
      return cacheManager.getCache(cacheName);
   }

   @Override
   public void createTemplate(String name, Configuration configuration) {
      join(createTemplateAsync(name, configuration));
   }

   @Override
   public CompletionStage<Void> createTemplateAsync(String name, Configuration configuration) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      return clusterConfigurationManager.createTemplate(name, configuration, flags);
   }

   @Override
   public Configuration getOrCreateTemplate(String name, Configuration configuration) {
      return join(getOrCreateTemplateAsync(name, configuration));
   }

   @Override
   public CompletionStage<Configuration> getOrCreateTemplateAsync(String name, Configuration configuration) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      return clusterConfigurationManager.getOrCreateTemplate(name, configuration, flags).thenApply(__ -> cacheManager.getCacheConfiguration(name));
   }

   @Override
   public void removeTemplate(String name) {
      join(removeTemplateAsync(name));
   }

   @Override
   public CompletionStage<Void> removeTemplateAsync(String name) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      return clusterConfigurationManager.removeTemplate(name, flags);
   }

   @Override
   public void removeCache(String name) {
      join(removeCacheAsync(name));
   }

   @Override
   public CompletionStage<Void> removeCacheAsync(String name) {
      authorizer.checkPermission(subject, AuthorizationPermission.CREATE);
      return clusterConfigurationManager.removeCache(name, flags);
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
}
