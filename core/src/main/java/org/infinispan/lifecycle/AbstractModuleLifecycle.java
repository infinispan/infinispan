/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.lifecycle;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;


/**
 * A default, no-op implementation of the {@link org.infinispan.lifecycle.ModuleLifecycle} interface, designed for easy
 * extension.
 *
 * @author Manik Surtani
 * @version 4.0
 */
public class AbstractModuleLifecycle implements ModuleLifecycle {
   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      // a no-op
   }

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      // a no-op
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      // a no-op
   }

   @Override
   public void cacheManagerStopped(GlobalComponentRegistry gcr) {
      // a no-op
   }

   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
      // a no-op
   }

   @Override
   public void cacheStarted(ComponentRegistry cr, String cacheName) {
      // a no-op
   }

   @Override
   public void cacheStopping(ComponentRegistry cr, String cacheName) {
      // a no-op
   }

   @Override
   public void cacheStopped(ComponentRegistry cr, String cacheName) {
      // a no-op
   }
}
