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
package org.infinispan.container.entries;

/**
 * Provide utility methods for dealing with expiration of cache entries.
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @since 4.0
 */
public class ExpiryHelper {

   public static boolean isExpiredMortal(long lifespan, long created, long now) {
      return lifespan > -1 && created > -1 && now > created + lifespan;
   }

   public static boolean isExpiredTransient(long maxIdle, long lastUsed, long now) {
      return maxIdle > -1 && lastUsed > -1 && now > maxIdle + lastUsed;
   }

   public static boolean isExpiredTransientMortal(long maxIdle, long lastUsed, long lifespan, long created, long now) {
      return isExpiredTransient(maxIdle, lastUsed, now) || isExpiredMortal(lifespan, created, now);
   }

   /**
    * Make sure this is not invoked in a loop, if so use {@link #isExpiredMortal(long, long, long)} instead
    * and reuse the result of {@link System#currentTimeMillis()} multiple times
    */
   @Deprecated
   public static boolean isExpiredMortal(long lifespan, long created) {
      return lifespan > -1 && created > -1 && System.currentTimeMillis() > created + lifespan;
   }

   /**
    * Make sure this is not invoked in a loop, if so use {@link #isExpiredTransient(long, long, long)} instead
    * and reuse the result of {@link System#currentTimeMillis()} multiple times
    */
   @Deprecated
   public static boolean isExpiredTransient(long maxIdle, long lastUsed) {
      return maxIdle > -1 && lastUsed > -1 && System.currentTimeMillis() > maxIdle + lastUsed;
   }

   /**
    * Make sure this is not invoked in a loop, if so use {@link #isExpiredTransientMortal(long, long, long, long, long)} instead
    * and reuse the result of {@link System#currentTimeMillis()} multiple times
    */
   @Deprecated
   public static boolean isExpiredTransientMortal(long maxIdle, long lastUsed, long lifespan, long created) {
      return isExpiredTransient(maxIdle, lastUsed) || isExpiredMortal(lifespan, created);
   }
}
