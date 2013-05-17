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

package org.infinispan.configuration.cache;

import org.infinispan.configuration.Builder;
import org.infinispan.marshall.Marshaller;

/**
 * Compatibility mode configuration builder
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class CompatibilityModeConfigurationBuilder
      extends AbstractConfigurationChildBuilder implements Builder<CompatibilityModeConfiguration> {

   private boolean enabled;
   private Marshaller marshaller;

   CompatibilityModeConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Enables compatibility mode between embedded and different remote
    * endpoints (Hot Rod, Memcached, REST...etc).
    */
   public CompatibilityModeConfigurationBuilder enable() {
      enabled = true;
      return this;
   }

   /**
    * Disables compatibility mode between embedded.
    */
   public CompatibilityModeConfigurationBuilder disable() {
      enabled = false;
      return this;
   }

   /**
    * Sets whether compatibility mode is enabled or disabled.
    *
    * @param enabled if true, compatibility mode is enabled.  If false, it is disabled.
    */
   public CompatibilityModeConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * Sets the marshaller instance to be used by the interoperability layer.
    */
   public CompatibilityModeConfigurationBuilder marshaller(Marshaller marshaller) {
      this.marshaller = marshaller;
      return this;
   }

   @Override
   public void validate() {
      // No-op
   }

   @Override
   public CompatibilityModeConfiguration create() {
      return new CompatibilityModeConfiguration(enabled, marshaller);
   }

   @Override
   public Builder<?> read(CompatibilityModeConfiguration template) {
      this.enabled = template.enabled();
      this.marshaller = template.marshaller();
      return this;
   }

}
