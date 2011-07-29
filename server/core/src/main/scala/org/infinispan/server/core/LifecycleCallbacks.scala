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

package org.infinispan.server.core

import org.infinispan.lifecycle.AbstractModuleLifecycle
import org.infinispan.server.core.ExternalizerIds._
import org.infinispan.factories.{ComponentRegistry, GlobalComponentRegistry}
import org.infinispan.config.{Configuration, GlobalConfiguration}

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.marshall.AdvancedExternalizer} implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
class LifecycleCallbacks extends AbstractModuleLifecycle {

   override def cacheManagerStarting(gcr: GlobalComponentRegistry, globalCfg: GlobalConfiguration) =
      addExternalizer(globalCfg)

   override def cacheStarting(cr: ComponentRegistry, cfg: Configuration, cacheName: String) =
      cfg.fluent.storeAsBinary.disable

   private[core] def addExternalizer(globalCfg : GlobalConfiguration) =
      globalCfg.fluent.serialization
         .addAdvancedExternalizer(SERVER_CACHE_VALUE, new CacheValue.Externalizer)
}