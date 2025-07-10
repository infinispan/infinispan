package org.infinispan.manager.impl;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.health.Health;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stats.CacheContainerStats;

/**
 * This is a convenient base class for implementing a cache manager delegate.
 *
 * The only constructor takes a {@link org.infinispan.manager.EmbeddedCacheManager}
 * argument, to which each method call is delegated. One can extend this class and only override the method sub-set
 * it is interested in.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @see org.infinispan.cache.impl.AbstractDelegatingCache
 * @see org.infinispan.cache.impl.AbstractDelegatingAdvancedCache
 */
@SurvivesRestarts
public class AbstractDelegatingEmbeddedCacheManager extends InternalCacheManager {

   protected EmbeddedCacheManager cm;

   public AbstractDelegatingEmbeddedCacheManager(EmbeddedCacheManager cm) {
      this.cm = cm;
   }

   @Override
   public org.infinispan.configuration.cache.Configuration defineConfiguration(String cacheName,
                                                                               org.infinispan.configuration.cache.Configuration configuration) {
      return cm.defineConfiguration(cacheName, configuration);
   }

   @Override
   public Configuration defineConfiguration(String cacheName, String templateCacheName, Configuration configurationOverride) {
      return cm.defineConfiguration(cacheName, templateCacheName, configurationOverride);
   }

   @Override
   public void undefineConfiguration(String configurationName) {
      cm.undefineConfiguration(configurationName);
   }

   @Override
   public String getClusterName() {
      return cm.getClusterName();
   }

   @Override
   public List<Address> getMembers() {
      return cm.getMembers();
   }

   @Override
   public Address getAddress() {
      return cm.getAddress();
   }

   @Override
   public Address getCoordinator() {
      return cm.getCoordinator();
   }

   @Override
   public boolean isCoordinator() {
      return cm.isCoordinator();
   }

   @Override
   public ComponentStatus getStatus() {
      return cm.getStatus();
   }

   @Override
   public org.infinispan.configuration.cache.Configuration getDefaultCacheConfiguration() {
      return cm.getDefaultCacheConfiguration();
   }

   @Override
   public org.infinispan.configuration.global.GlobalConfiguration getCacheManagerConfiguration() {
      return cm.getCacheManagerConfiguration();
   }

   @Override
   public org.infinispan.configuration.cache.Configuration getCacheConfiguration(String name) {
      return cm.getCacheConfiguration(name);
   }

   @Override
   public Set<String> getCacheNames() {
      return cm.getCacheNames();
   }

   @Override
   public Set<String> getAccessibleCacheNames() {
      return cm.getAccessibleCacheNames();
   }

   @Override
   public Set<String> getCacheConfigurationNames() {
      return cm.getCacheConfigurationNames();
   }

   @Override
   public ClusterExecutor executor() {
      return cm.executor();
   }

   @Override
   public Health getHealth() {
      return cm.getHealth();
   }

   @Override
   public CacheManagerInfo getCacheManagerInfo() {
      return cm.getCacheManagerInfo();
   }

   @Override
   public boolean isRunning(String cacheName) {
      return cm.isRunning(cacheName);
   }

   @Override
   public boolean isDefaultRunning() {
      return cm.isDefaultRunning();
   }

   @Override
   public boolean cacheExists(String cacheName) {
      return cm.cacheExists(cacheName);
   }

   @Override
   public boolean cacheConfigurationExists(String name) {
      return cm.cacheConfigurationExists(name);
   }

   @Override
   public EmbeddedCacheManagerAdmin administration() {
      return cm.administration();
   }

   @Override
   public ClassAllowList getClassAllowList() {
      return cm.getClassAllowList();
   }

   @Override
   public <K, V> Cache<K, V> createCache(String name, Configuration configuration) {
      return cm.createCache(name, configuration);
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, boolean createIfAbsent) {
      return cm.getCache(cacheName, createIfAbsent);
   }

   @Override
   public void stopCache(String cacheName) {
      cm.stopCache(cacheName);
   }

   @Override
   public EmbeddedCacheManager startCaches(String... cacheNames) {
      return cm.startCaches(cacheNames);
   }

   @Override
   public <K, V> Cache<K, V> getCache() {
      return cm.getCache();
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      return cm.getCache(cacheName);
   }

   @Override
   public void start() {
      cm.start();
   }

   @Override
   public void stop() {
      cm.stop();
   }

   @Override
   public void addCacheDependency(String from, String to) {
      cm.addCacheDependency(from, to);
   }

   @Override
   public void addListener(Object listener) {
      cm.addListener(listener);
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      return cm.addListenerAsync(listener);
   }

   @Override
   public void removeListener(Object listener) {
      cm.removeListener(listener);
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      return cm.removeListenerAsync(listener);
   }

   @Override
   public CacheContainerStats getStats() {
      return cm.getStats();
   }

   @Override
   public void close() throws IOException {
      cm.close();
   }

   @Override
   public EmbeddedCacheManager withSubject(Subject subject) {
      return cm.withSubject(subject);
   }

   @Override
   public Subject getSubject() {
      return cm.getSubject();
   }

   @Override
   protected GlobalComponentRegistry globalComponentRegistry() {
      return InternalCacheManager.of(cm);
   }
}
