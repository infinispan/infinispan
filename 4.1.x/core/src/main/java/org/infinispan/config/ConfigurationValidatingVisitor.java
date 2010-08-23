/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.infinispan.config;

import org.infinispan.config.GlobalConfiguration.TransportType;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.decorators.SingletonStoreConfig;

/**
 * ConfigurationValidatingVisitor checks semantic validity of InfinispanConfiguration instance.
 * 
 * 
 * @author Vladimir Blagojevic
 * @since 4.0
 */
public class ConfigurationValidatingVisitor extends AbstractConfigurationBeanVisitor {
   private TransportType tt = null;
   private CacheLoaderManagerConfig clmc = null;
   private Configuration.EvictionType eviction = null;

   @Override
   public void visitSingletonStoreConfig(SingletonStoreConfig ssc) {
      if (tt == null && ssc.isSingletonStoreEnabled()) throw new ConfigurationException("Singleton store configured without transport being configured");
   }

   @Override
   public void visitTransportType(TransportType tt) {
      this.tt = tt;
   }

   @Override
   public void visitConfiguration(Configuration bean) {
      checkEagerLockingAndDld(bean);
   }

   private void checkEagerLockingAndDld(Configuration bean) {
      boolean isEagerLocking = bean.isUseEagerLocking();
      checkEagerLockingAndDld(bean, isEagerLocking);
   }

   public static void checkEagerLockingAndDld(Configuration bean, boolean eagerLocking) {
      boolean isDealLockDetection = bean.isEnableDeadlockDetection();
      if (isDealLockDetection && eagerLocking) {
         throw new ConfigurationException("Deadlock detection cannot be used with eager locking until ISPN-596 is fixed. See https://jira.jboss.org/browse/ISPN-596");
      }
   }
}
