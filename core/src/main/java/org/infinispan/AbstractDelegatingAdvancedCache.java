/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan;

import org.infinispan.batch.BatchContainer;
import org.infinispan.container.DataContainer;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.rpc.RpcManager;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.Collection;
import java.util.List;

/**
 * Similar to {@link org.infinispan.AbstractDelegatingCache}, but for {@link AdvancedCache}.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.AbstractDelegatingCache
 */
public abstract class AbstractDelegatingAdvancedCache<K, V> extends AbstractDelegatingCache<K, V> implements AdvancedCache<K, V> {

   private AdvancedCache<K, V> cache;

   public AbstractDelegatingAdvancedCache(AdvancedCache<K, V> cache) {
      super(cache);
      this.cache = cache;
   }

   public void addInterceptor(CommandInterceptor i, int position) {
      cache.addInterceptor(i, position);
   }

   public void addInterceptorAfter(CommandInterceptor i, Class<? extends CommandInterceptor> afterInterceptor) {
      cache.addInterceptorAfter(i, afterInterceptor);
   }

   public void addInterceptorBefore(CommandInterceptor i, Class<? extends CommandInterceptor> beforeInterceptor) {
      cache.addInterceptorBefore(i, beforeInterceptor);
   }

   public void removeInterceptor(int position) {
      cache.removeInterceptor(position);
   }

   public void removeInterceptor(Class<? extends CommandInterceptor> interceptorType) {
      cache.removeInterceptor(interceptorType);
   }

   public List<CommandInterceptor> getInterceptorChain() {
      return cache.getInterceptorChain();
   }

   public EvictionManager getEvictionManager() {
      return cache.getEvictionManager();
   }

   public ComponentRegistry getComponentRegistry() {
      return cache.getComponentRegistry();
   }

   public DistributionManager getDistributionManager() {
      return cache.getDistributionManager();
   }

   public RpcManager getRpcManager() {
      return cache.getRpcManager();
   }

   public BatchContainer getBatchContainer() {
      return cache.getBatchContainer();
   }

   public InvocationContextContainer getInvocationContextContainer() {
      return cache.getInvocationContextContainer();
   }

   public DataContainer getDataContainer() {
      return cache.getDataContainer();
   }

   public TransactionManager getTransactionManager() {
      return cache.getTransactionManager();
   }

   @Override
   public XAResource getXAResource() {
      return cache.getXAResource();
   }

   public AdvancedCache<K, V> withFlags(Flag... flags) {
      cache.withFlags(flags);
      return this;
   }

   public boolean lock(K... key) {
      return cache.lock(key);
   }

   public boolean lock(Collection<? extends K> keys) {
      return cache.lock(keys);
   }
}
