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
package org.horizon.config;

import org.horizon.eviction.DefaultEvictionAction;
import org.horizon.eviction.EvictionAlgorithmConfig;

import java.util.concurrent.TimeUnit;

public class EvictionConfig extends AbstractNamedCacheConfigurationBean {
   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -7979639000026975201L;

   /**
    * Wake up interval, in milliseconds
    */
   private long wakeUpInterval = 5000; // 5 second default
   private EvictionAlgorithmConfig algorithmConfig;
   private int eventQueueSize = 200000; // 200,000 default
   private String actionPolicyClass = DefaultEvictionAction.class.getName();

   public EvictionConfig() {
   }

   public long getWakeUpInterval() {
      return wakeUpInterval;
   }

   public void setWakeUpInterval(long wakeUpInterval) {
      testImmutability("wakeUpInterval");
      this.wakeUpInterval = wakeUpInterval;
   }

   public void setWakeupInterval(long time, TimeUnit timeUnit) {
      setWakeUpInterval(timeUnit.toMillis(time));
   }

   public EvictionAlgorithmConfig getAlgorithmConfig() {
      return algorithmConfig;
   }

   public void setAlgorithmConfig(EvictionAlgorithmConfig algorithmConfig) {
      this.algorithmConfig = algorithmConfig;
   }

   public int getEventQueueSize() {
      return eventQueueSize;
   }

   public void setEventQueueSize(int eventQueueSize) {
      testImmutability("eventQueueSize");
      this.eventQueueSize = eventQueueSize;
   }

   public String getActionPolicyClass() {
      return actionPolicyClass;
   }

   public void setActionPolicyClass(String actionPolicyClass) {
      testImmutability("actionPolicyClass");
      this.actionPolicyClass = actionPolicyClass;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EvictionConfig that = (EvictionConfig) o;

      if (eventQueueSize != that.eventQueueSize) return false;
      if (wakeUpInterval != that.wakeUpInterval) return false;
      if (actionPolicyClass != null ? !actionPolicyClass.equals(that.actionPolicyClass) : that.actionPolicyClass != null)
         return false;
      if (algorithmConfig != null ? !algorithmConfig.equals(that.algorithmConfig) : that.algorithmConfig != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (int) (wakeUpInterval ^ (wakeUpInterval >>> 32));
      result = 31 * result + (algorithmConfig != null ? algorithmConfig.hashCode() : 0);
      result = 31 * result + eventQueueSize;
      result = 31 * result + (actionPolicyClass != null ? actionPolicyClass.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "EvictionConfig{" +
            "wakeUpInterval=" + wakeUpInterval +
            ", algorithmConfig=" + algorithmConfig +
            ", eventQueueSize=" + eventQueueSize +
            ", actionPolicyClass='" + actionPolicyClass + '\'' +
            '}';
   }

   public EvictionConfig clone() {
      try {
         EvictionConfig clone = (EvictionConfig) super.clone();
         clone.actionPolicyClass = actionPolicyClass;
         if (algorithmConfig != null) clone.algorithmConfig = algorithmConfig.clone();
         clone.eventQueueSize = eventQueueSize;
         clone.wakeUpInterval = wakeUpInterval;
         return clone;
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen", e);
      }
   }
}
