/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.interceptors.base;

import org.infinispan.Cache;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Anyone using the {@link org.infinispan.AdvancedCache#addInterceptor(CommandInterceptor, int)} method (or any of its
 * overloaded forms) or registering custom interceptors via XML should extend this base class when creating their own 
 * custom interceptors.
 * <p />
 * As of Infinispan 5.1, annotations on custom interceptors, including {@link Inject}, {@link Start} and {@link Stop} 
 * will not be respected and callbacks will not be made.
 * <p />
 * Instead, custom interceptor authors should extend this base class to gain access to {@link Cache} and {@link EmbeddedCacheManager},
 * from which other components may be accessed.  Further, lifecycle should be implemented by overriding {@link #start()}
 * and {@link #stop()} as defined in this class.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class BaseCustomInterceptor extends CommandInterceptor {
   protected Cache<?, ?> cache;
   protected EmbeddedCacheManager embeddedCacheManager;

   @Inject
   private void setup(Cache cache, EmbeddedCacheManager embeddedCacheManager) {
      this.cache = cache;
      this.embeddedCacheManager = embeddedCacheManager;
   }

   @Start
   protected void start() {
      // Meant to be overridden
   }

   @Stop
   protected void stop() {
      // Meant to be overridden
   }
}
