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

public class VersioningConfigurationBuilder extends AbstractConfigurationChildBuilder<VersioningConfiguration> {

   boolean enabled = false;
   VersioningScheme scheme = VersioningScheme.NONE;

   protected VersioningConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   public VersioningConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }

   public VersioningConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public VersioningConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   public VersioningConfigurationBuilder scheme(VersioningScheme scheme) {
      this.scheme = scheme;
      return this;
   }

   @Override
   void validate() {
   }

   @Override
   VersioningConfiguration create() {
      return new VersioningConfiguration(enabled, scheme);
   }
   
   @Override
   public VersioningConfigurationBuilder read(VersioningConfiguration template) {
      this.enabled = template.enabled();
      this.scheme = template.scheme();
      
      return this;
   }
}
