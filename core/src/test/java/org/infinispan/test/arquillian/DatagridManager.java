/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.test.arquillian;

import java.util.List;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.ReplListener;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * An adapter class that enables users to call all important methods from 
 * {@link MultipleCacheManagersTest}, {@link AbstractCacheTest} and 
 * {@link AbstractInfinispanTest}, changing their visibility to public. 
 * Usage of this class is in infinispan-arquillian-container project which 
 * enables injecting of this class into a test case and forming a cluster 
 * of cache managers/caches.
 * 
 * A few methods from super-classes changed their names, mostly because 
 * they cannot be overridden. All such methods have comments on them which
 * say "name change". 
 * 
 * 
 * @author <a href="mailto:mgencur@redhat.com">Martin Gencur</a>
 * 
 */
public class DatagridManager extends MultipleCacheManagersTest
{

   public void destroy() {
      TestingUtil.killCacheManagers(cacheManagers);
      cacheManagers.clear();
      listeners.clear();
      killSpawnedThreads();
   }
   
   @Override
   protected void createCacheManagers() throws Throwable {
      //empty implementation
   }
   
   /* ========================= AbstractInfinispanTest methods ================== */
   
   //name change
   public void waitForCondition(Condition ec, long timeout) {
      eventually(ec, timeout);
   }
   
   //name change
   public Thread forkThread(Runnable r, boolean sync) {
      return fork(r, sync);
   }
   
   //name change
   public void waitForCondition(Condition ec) {
      eventually(ec);
   }
   
   /* =========================== AbstractCacheTest methods ====================== */
   
   //name change
   public boolean xorOp(boolean b1, boolean b2) {
      return xor(b1, b2);
   }
   
   //name change
   public void assertKeyNotLocked(Cache cache, Object key) {
      assertNotLocked(cache, key);
   }
   
   //name change
   public void assertKeyLocked(Cache cache, Object key) {
      assertLocked(cache, key);
   }
   
   /* ===================== MultipleCacheManagersTest methods ==================== */
   
   public void assertSupportedConfig() {
      super.assertSupportedConfig();
   }

   //name change
   public void registerCacheManagers(CacheContainer... cacheContainers) {
      registerCacheManager(cacheContainers);
   }
   
   public EmbeddedCacheManager addClusterEnabledCacheManager() {
      return super.addClusterEnabledCacheManager();
   }
   
   public EmbeddedCacheManager addClusterEnabledCacheManager(TransportFlags flags) {
      return super.addClusterEnabledCacheManager(flags);
   }
   
   public EmbeddedCacheManager addClusterEnabledCacheManager(Configuration defaultConfig) {
      return super.addClusterEnabledCacheManager(defaultConfig);
   }
   
   public EmbeddedCacheManager addClusterEnabledCacheManager(Configuration defaultConfig, boolean transactional) {
      return super.addClusterEnabledCacheManager(defaultConfig, transactional);
   }
   
   public EmbeddedCacheManager addClusterEnabledCacheManager(Configuration defaultConfig, TransportFlags flags) {
      return super.addClusterEnabledCacheManager(defaultConfig, flags);
   }
   
   public EmbeddedCacheManager addClusterEnabledCacheManager(Configuration defaultConfig, boolean transactional, TransportFlags flags) {
      return super.addClusterEnabledCacheManager(defaultConfig, transactional, flags);
   }
   
   public EmbeddedCacheManager addClusterEnabledCacheManager(Configuration.CacheMode mode, boolean transactional) {
      return super.addClusterEnabledCacheManager(mode, transactional);
   }
   
   public EmbeddedCacheManager addClusterEnabledCacheManager(Configuration.CacheMode mode, boolean transactional, TransportFlags flags) {
      return super.addClusterEnabledCacheManager(mode, transactional, flags);
   }

   public void createCluster(Configuration.CacheMode mode, boolean transactional, int count) {
      super.createCluster(mode, transactional, count);
   }
   
   public void createCluster(Configuration config, int count) {
      super.createCluster(config, count);
   }
   
   public void createCluster(Configuration config, boolean transactional, int count) {
      super.createCluster(config, transactional, count);
   }
   
   public void createCluster(Configuration.CacheMode mode, int count) {
      super.createCluster(mode, count);
   }
   
   public void defineConfigurationOnAllManagers(String cacheName, Configuration c) {
      super.defineConfigurationOnAllManagers(cacheName, c);
   }
   
   public void waitForClusterToForm(String cacheName) {
      super.waitForClusterToForm(cacheName);
   }
   
   public void waitForClusterToForm() {
      super.waitForClusterToForm();
   }
   
   public TransactionManager tm(Cache<?, ?> c) {
      return super.tm(c);
   }
   
   public TransactionManager tm(int i, String cacheName) {
      return super.tm(i, cacheName);
   }
   
   public TransactionManager tm(int i) {
      return super.tm(i);
   }
      
   public Transaction tx(int i) {
      return super.tx(i);
   }
   
   public <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, Configuration c) {
      return super.createClusteredCaches(numMembersInCluster, cacheName, c);
   }
   
   public <K, V> List<Cache<K, V>> createClusteredCaches(
         int numMembersInCluster, String cacheName, Configuration c, TransportFlags flags) {
      return super.createClusteredCaches(numMembersInCluster, cacheName, c, flags);
   }
   
   public <K, V> List<Cache<K, V>> createClusteredCaches(int numMembersInCluster, Configuration defaultConfig) {
      return super.createClusteredCaches(numMembersInCluster, defaultConfig);
   }
   
   public ReplListener replListener(Cache cache) {
      return super.replListener(cache);
   }
   
   public EmbeddedCacheManager manager(int i) {
      return super.manager(i);
   }
   
   public Cache cache(int managerIndex, String cacheName) {
      return super.cache(managerIndex, cacheName);
   }
   
   public void assertClusterSize(String message, int size) {
      super.assertClusterSize(message, size);
   }
   
   public void removeCacheFromCluster(String cacheName) {
      super.removeCacheFromCluster(cacheName);
   }
   
   public <A, B> Cache<A, B> cache(int index) {
      return super.cache(index);
   }
   
   public Address address(int cacheIndex) {
      return super.address(cacheIndex);
   }
   
   public AdvancedCache advancedCache(int i) {
      return super.advancedCache(i);
   }
   
   public AdvancedCache advancedCache(int i, String cacheName) {
      return super.advancedCache(i, cacheName);
   }
   
   public <K, V> List<Cache<K, V>> caches(String name) {
      return super.caches(name);
   }
   
   public <K, V> List<Cache<K, V>> caches() {
      return super.caches();
   }
   
   public Address address(Cache c) {
      return super.address(c);
   }
   
   public LockManager lockManager(int i) {
      return super.lockManager(i);
   }
   
   public LockManager lockManager(int i, String cacheName) {
      return super.lockManager(i, cacheName);
   }
   
   public List<EmbeddedCacheManager> getCacheManagers() {
      return super.getCacheManagers();
   }
   
   public void killMember(int cacheIndex) {
      super.killMember(cacheIndex);
   }
   
   public Object getKeyForNode(int nodeIndex) {
      return super.getKeyForNode(nodeIndex);
   }
   
   /* ========== methods simulating those from SingleCacheManagerTest ========== */
   
   public EmbeddedCacheManager manager() {
      return super.manager(0);
   }
   
   public <A, B> Cache<A, B> cache() {
      return super.cache(0);
   }
   
   public Configuration getDefaultStandaloneConfig(boolean transactional) {
      return TestCacheManagerFactory.getDefaultConfiguration(transactional);
   }
   
   public TransactionManager tm() {
      return super.cache(0).getAdvancedCache().getTransactionManager();
   }
   
   public Transaction tx() {
      try {
         return super.cache(0).getAdvancedCache().getTransactionManager().getTransaction();
      } catch (SystemException e) {
         throw new RuntimeException(e);
      }
   }
}
