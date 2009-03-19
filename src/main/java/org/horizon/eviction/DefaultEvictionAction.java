/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
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
package org.horizon.eviction;

import org.horizon.Cache;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

/**
 * Default eviction action policy that calls {@link Cache#evict(Object)} to evict an entry.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class DefaultEvictionAction implements EvictionAction {
   private static final Log log = LogFactory.getLog(DefaultEvictionAction.class);
   private static boolean trace = log.isTraceEnabled();
   private Cache cache;

   public void setCache(Cache<?, ?> cache) {
      this.cache = cache;
   }

   @SuppressWarnings("unchecked")
   public boolean evict(Object key) {
      if (trace) log.trace("Attempting to evict cached entry with key of {0}", key);

      try {
         cache.evict(key);
         log.trace("Eviction of cached entry with key of {0} successful", key);
         return true;
      }
      catch (Exception e) {
         if (log.isDebugEnabled()) log.debug("Unable to evict {0}", e, key);
         return false;
      }
   }
}
