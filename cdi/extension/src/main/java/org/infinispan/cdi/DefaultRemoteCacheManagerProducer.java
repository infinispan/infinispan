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
package org.infinispan.cdi;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.jboss.solder.bean.defaultbean.DefaultBean;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

/**
 * <p>The default {@link RemoteCacheManager} producer.</p>
 *
 * <p>The remote cache manager used by default can be overridden by creating a producer which produces the new default
 * remote cache manager. The remote cache manager produced must have the scope {@link ApplicationScoped} and the
 * {@linkplain javax.enterprise.inject.Default Default} qualifier.</p>
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class DefaultRemoteCacheManagerProducer {
   /**
    * Produces the default remote cache manager with the default settings.
    *
    * @return the default remote cache manager.
    * @see org.infinispan.client.hotrod.RemoteCacheManager#RemoteCacheManager()
    */
   @Produces
   @ApplicationScoped
   @DefaultBean(RemoteCacheManager.class)
   public RemoteCacheManager getDefaultRemoteCacheManager() {
      return new RemoteCacheManager();
   }
}
