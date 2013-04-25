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
 * Metadata class for embedded caches.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public final class EmbeddedMetadata implements Metadata {

   private final long lifespan;
   private final TimeUnit lifespanUnit;
   private final long maxIdle;
   private final TimeUnit maxIdleUnit;
   private final EntryVersion version;

   private EmbeddedMetadata(
         long lifespan, TimeUnit lifespanUnit,
         long maxIdle, TimeUnit maxIdleUnit, EntryVersion version) {
      this.lifespan = lifespan;
      this.lifespanUnit = lifespanUnit;
      this.maxIdle = maxIdle;
      this.maxIdleUnit = maxIdleUnit;
      this.version = version;
   }

   @Override
   public long lifespan() {
      return lifespan;
   }

   @Override
   public TimeUnit lifespanUnit() {
      return lifespanUnit;
   }

   @Override
   public long maxIdle() {
      return maxIdle;
   }

   @Override
   public TimeUnit maxIdleUnit() {
      return maxIdleUnit;
   }

   @Override
   public EntryVersion version() {
      return version;
   }

   public static final class Builder implements Metadata.Builder {

      private long lifespan;
      private TimeUnit lifespanUnit;
      private long maxIdle;
      private TimeUnit maxIdleUnit;
      private EntryVersion version;

      @Override
      public Metadata.Builder lifespan(long time, TimeUnit unit) {
         lifespan = time;
         lifespanUnit = unit;
         return this;
      }

      @Override
      public Metadata.Builder maxIdle(long time, TimeUnit unit) {
         maxIdle = time;
         maxIdleUnit = unit;
         return this;
      }

      @Override
      public Metadata.Builder version(EntryVersion version) {
         this.version = version;
         return this;
      }

      @Override
      public Metadata build() {
         return new EmbeddedMetadata(
               lifespan, lifespanUnit, maxIdle, maxIdleUnit, version);
      }

   }

}
