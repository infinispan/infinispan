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

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.api.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.SurvivesRestarts;

/**
 * Factory for setting up bootstrap components
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = {Cache.class, AdvancedCache.class, Configuration.class, ComponentRegistry.class})
@SurvivesRestarts
public class BootstrapFactory extends AbstractNamedCacheComponentFactory {
   AdvancedCache advancedCache;

   public BootstrapFactory(AdvancedCache advancedCache, Configuration configuration, ComponentRegistry componentRegistry) {
      this.componentRegistry = componentRegistry;
      this.configuration = configuration;
      this.advancedCache = advancedCache;
   }

   @Override
   public <T> T construct(Class<T> componentType) {
      Object comp = null;
      if (componentType.isAssignableFrom(AdvancedCache.class)) {
         comp = advancedCache;
      } else if (componentType.isAssignableFrom(Configuration.class)) {
         comp = configuration;
      } else if (componentType.isAssignableFrom(ComponentRegistry.class)) {
         comp = componentRegistry;
      }
      if (comp == null) throw new CacheException("Don't know how to handle type " + componentType);

      try {
         return componentType.cast(comp);
      } catch (Exception e) {
         throw new CacheException("Problems casting bootstrap component " + comp.getClass() + " to type " + componentType, e);
      }
   }
}
