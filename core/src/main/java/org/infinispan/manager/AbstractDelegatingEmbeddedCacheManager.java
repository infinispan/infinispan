/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;

import java.util.List;
import java.util.Set;

/**
 * This is a convenient base class for implementing a cache manager delegate.
 *
 * The only constructor takes a {@link org.infinispan.manager.EmbeddedCacheManager}
 * argument, to which each method call is delegated. One can extend this class and only override the method sub-set
 * it is interested in.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @see org.infinispan.AbstractDelegatingCache
 * @see org.infinispan.AbstractDelegatingAdvancedCache
 */
public class AbstractDelegatingEmbeddedCacheManager implements EmbeddedCacheManager {

   protected EmbeddedCacheManager cm;

   public AbstractDelegatingEmbeddedCacheManager(EmbeddedCacheManager cm) {
      this.cm = cm;
   }

   @Override
   public Configuration defineConfiguration(String cacheName, Configuration configurationOverride) {
      return cm.defineConfiguration(cacheName, configurationOverride);
   }

   @Override
   public Configuration defineConfiguration(String cacheName, String templateCacheName, Configuration configurationOverride) {
      return cm.defineConfiguration(cacheName, templateCacheName, configurationOverride);
   }
   
   @Override
   public org.infinispan.configuration.cache.Configuration defineConfiguration(String cacheName,
         org.infinispan.configuration.cache.Configuration configuration) {
      return cm.defineConfiguration(cacheName, configuration);
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
   public GlobalConfiguration getGlobalConfiguration() {
      return cm.getGlobalConfiguration();
   }

   @Override
   public Configuration getDefaultConfiguration() {
      return cm.getDefaultConfiguration();
   }

   @Override
   public Set<String> getCacheNames() {
      return cm.getCacheNames();
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
   public <K, V> Cache<K, V> getCache(String cacheName, boolean createIfAbsent) {
      return cm.getCache(cacheName, createIfAbsent);
   }

   @Override
   public EmbeddedCacheManager startCaches(String... cacheNames) {
      return cm.startCaches(cacheNames);
   }

   @Override
   public void removeCache(String cacheName) {
      cm.removeCache(cacheName);
   }

   @Override
   public Transport getTransport() {
      return cm.getTransport();
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
   public void addListener(Object listener) {
      cm.addListener(listener);
   }

   @Override
   public void removeListener(Object listener) {
      cm.removeListener(listener);
   }

   @Override
   public Set<Object> getListeners() {
      return cm.getListeners();
   }
}
