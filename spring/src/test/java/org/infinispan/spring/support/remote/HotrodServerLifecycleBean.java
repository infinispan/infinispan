/**
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
 *   ~
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

package org.infinispan.spring.support.remote;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * HotrodServerLifecycleBean.
 * 
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @since 5.1
 */
public class HotrodServerLifecycleBean implements InitializingBean, DisposableBean {

   private EmbeddedCacheManager cacheManager;

   private RemoteCacheManager remoteCacheManager;

   private HotRodServer hotrodServer;

   private String remoteCacheName;

   public void setRemoteCacheName(String remoteCacheName) {
      this.remoteCacheName = remoteCacheName;
   }

   @Override
   public void afterPropertiesSet() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager(false);
      cacheManager.getCache(remoteCacheName);

      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      remoteCacheManager = new RemoteCacheManager("localhost", hotrodServer.getPort());
   }

   @Override
   public void destroy() throws Exception {
      remoteCacheManager.stop();
      hotrodServer.stop();
      cacheManager.stop();
   }
}
