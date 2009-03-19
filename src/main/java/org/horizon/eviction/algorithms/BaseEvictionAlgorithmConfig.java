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
package org.horizon.eviction.algorithms;

import org.horizon.config.AbstractNamedCacheConfigurationBean;
import org.horizon.config.ConfigurationException;
import org.horizon.config.Dynamic;
import org.horizon.eviction.EvictionAlgorithmConfig;

import java.util.concurrent.TimeUnit;

/**
 * A base class used for configuring eviction algorithms.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public abstract class BaseEvictionAlgorithmConfig extends AbstractNamedCacheConfigurationBean implements EvictionAlgorithmConfig {
   private static final long serialVersionUID = 4591691674370188932L;

   protected String evictionAlgorithmClassName;
   @Dynamic
   protected int maxEntries = -1;
   @Dynamic
   protected int minEntries = -1;
   @Dynamic
   protected long minTimeToLive = -1;

   /**
    * Can only be instantiated by a subclass.
    */
   protected BaseEvictionAlgorithmConfig() {
   }

   public String getEvictionAlgorithmClassName() {
      return evictionAlgorithmClassName;
   }

   public int getMaxEntries() {
      return maxEntries;
   }

   /**
    * @param maxEntries max entries to hold in the cache. 0 denotes immediate expiry and -1 denotes unlimited entries.
    *                   -1 is the default
    */
   public void setMaxEntries(int maxEntries) {
      testImmutability("maxEntries");
      this.maxEntries = maxEntries;
   }

   public int getMinEntries() {
      return minEntries;
   }

   /**
    * This specifies the minimum entries to prune down to when maxExtries has been hit.  -1 is the default value, which
    * means this feature is effectively unset, and eviction algorithms would be expected to evict until the cache
    * contains no more than maxEntries.  Any other value means that if a pruning process starts, it will not stop until
    * minEntries has been reached.  So, for example, minEntries of 0 would mean that the cache is emptied the moment
    * maxEntries is exceeded.
    *
    * @param minEntries minEntries value
    */
   public void setMinEntries(int minEntries) {
      testImmutability("minEntries");
      this.minEntries = minEntries;
   }

   /**
    * @return The minimum time to live, in milliseconds.
    */
   public long getMinTimeToLive() {
      return minTimeToLive;
   }

   /**
    * @param minTimeToLive time to live, in milliseconds.  This defaults to -1, meaning that it is excluded from
    *                      calculations.
    */
   public void setMinTimeToLive(long minTimeToLive) {
      testImmutability("minTimeToLive");
      this.minTimeToLive = minTimeToLive;
   }

   public void setMinTimeToLive(long time, TimeUnit timeUnit) {
      testImmutability("minTimeToLive");
      minTimeToLive = timeUnit.toMillis(time);
   }

   public void validate() throws ConfigurationException {
      if (evictionAlgorithmClassName == null)
         throw new ConfigurationException("Eviction algorithm class name cannot be null!");
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BaseEvictionAlgorithmConfig that = (BaseEvictionAlgorithmConfig) o;

      if (maxEntries != that.maxEntries) return false;
      if (minEntries != that.minEntries) return false;
      if (minTimeToLive != that.minTimeToLive) return false;
      if (evictionAlgorithmClassName != null ? !evictionAlgorithmClassName.equals(that.evictionAlgorithmClassName) : that.evictionAlgorithmClassName != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = evictionAlgorithmClassName != null ? evictionAlgorithmClassName.hashCode() : 0;
      result = 31 * result + maxEntries;
      result = 31 * result + minEntries;
      result = 31 * result + (int) (minTimeToLive ^ (minTimeToLive >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + "{" +
            "evictionAlgorithmClassName='" + evictionAlgorithmClassName + '\'' +
            ", maxEntries=" + maxEntries +
            ", minEntries=" + minEntries +
            ", minTimeToLive=" + minTimeToLive +
            '}';
   }

   public void reset() {
      maxEntries = -1;
      minEntries = -1;
      minTimeToLive = -1;
   }

   public BaseEvictionAlgorithmConfig clone() {
      try {
         return (BaseEvictionAlgorithmConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen", e);
      }
   }
}
