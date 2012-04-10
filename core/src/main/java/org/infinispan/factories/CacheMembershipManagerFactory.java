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
package org.infinispan.factories;

import org.infinispan.cacheviews.CacheViewsManager;
import org.infinispan.cacheviews.CacheViewsManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Transport;

/**
 * Constructs {@link org.infinispan.cacheviews.CacheViewsManager} instances.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @since 5.1
 */
@DefaultFactoryFor(classes = CacheViewsManager.class)
public class CacheMembershipManagerFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   private Transport transport;

   @Inject
   private void injectGlobalDependencies(Transport transport) {
      this.transport = transport;
   }

   @Override
   public <T> T construct(Class<T> componentType) {
      // the CacheViewsManager doesn't make sense for local caches
      if (transport == null)
         return null;

      return componentType.cast(new CacheViewsManagerImpl());
   }
}
