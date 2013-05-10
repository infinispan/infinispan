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

   // Concrete, immutable class chosen instead of interface in order to enable
   // injection of internally generated versions in the absence of user
   // provided version.

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
    * Returns an instance of {@link Builder} which can be used to build
    * new instances of {@link Metadata} instance which are full copies of
    * this {@link Metadata}.
    *
    * @return instance of {@link Builder}
    */
   Builder builder();

   /**
    * Metadata builder
    */
   public interface Builder {

      /**
       * Set lifespan.
       *
       * @param time of lifespan
       * @param unit of lifespan time
       * @return a builder instance
       */
      Builder lifespan(long time, TimeUnit unit);

      /**
       * Set max idle time.
       *
       * @param time of max idle
       * @param unit of max idle time
       * @return a builder instance
       */
      Builder maxIdle(long time, TimeUnit unit);

      /**
       * Set version.
       *
       * @param version of the metadata
       * @return a builder instance
       */
      Builder version(EntryVersion version);

      /**
       * Reads the metadata and apply its data.
       *
       * @param template metadata
       * @return a builder instance
       */
      Builder read(Metadata template);

      /**
       * Build a metadata instance.
       *
       * @return an instance of metadata
       */
      Metadata build();

   }

}
