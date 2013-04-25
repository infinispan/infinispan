/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.container.versioning.EntryVersion;

import java.util.concurrent.TimeUnit;

/**
 * This interface encapsulates metadata information that can be stored
 * alongside values in the cache.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public interface Metadata {

   /**
    * Returns the lifespan of the cache entry with which this metadata object
    * is associated.  Negative values are interpreted as unlimited lifespan.
    *
    * @return lifespan of the entry
    */
   long lifespan();

   /**
    * Returns the lifespan time unit of the cache entry with which this
    * metadata object is associated.
    *
    * @return lifespan time unit
    */
   TimeUnit lifespanUnit();

   /**
    * Returns the the maximum amount of time that the cache entry associated
    * with this metadata object is allowed to be idle for before it is
    * considered as expired.
    *
    * @return maximum idle time of the entry
    */
   long maxIdle();

   /**
    * Returns the maximum idle time unit of the cache entry with which this
    * metadata object is associated.
    *
    * @return maximum idle time unit
    */
   TimeUnit maxIdleUnit();

   /**
    * Returns the version of the cache entry with which this metadata object
    * is associated.
    *
    * @return version of the entry
    */
   EntryVersion version();

   /**
    * Metadata builder
    */
   public interface Builder {

      Builder lifespan(long time, TimeUnit unit);

      Builder maxIdle(long time, TimeUnit unit);

      Builder version(EntryVersion version);

      Metadata build();

   }

}
