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
package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

/**
 * Configures deadlock detection.
 */
public class DeadlockDetectionConfigurationBuilder extends AbstractConfigurationChildBuilder<DeadlockDetectionConfiguration> {

   private boolean enabled = false;
   private long spinDuration = TimeUnit.MILLISECONDS.toMillis(100);
   
   DeadlockDetectionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }
   
   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    */
   public DeadlockDetectionConfigurationBuilder spinDuration(long l) {
      this.spinDuration = l;
      return this;
   }
   
   /**
    * Enable deadlock detection
    */
   public DeadlockDetectionConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   /**
    * Disable deadlock detection
    */
   public DeadlockDetectionConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }
   
   /**
    * Enable or disable deadlock detection
    */
   public DeadlockDetectionConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   DeadlockDetectionConfiguration create() {
      return new DeadlockDetectionConfiguration(enabled, spinDuration);
   }
   
   @Override
   public DeadlockDetectionConfigurationBuilder read(DeadlockDetectionConfiguration template) {
      this.enabled = template.enabled();
      this.spinDuration = template.spinDuration();
      
      return this;
   }
   
}
