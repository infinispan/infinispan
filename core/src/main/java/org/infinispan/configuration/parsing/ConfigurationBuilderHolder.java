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
package org.infinispan.configuration.parsing;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;

public class ConfigurationBuilderHolder {

   private final GlobalConfigurationBuilder globalConfigurationBuilder;
   private final ConfigurationBuilder defaultConfigurationBuilder;
   private final Map<String, ConfigurationBuilder> namedConfigurationBuilders;
   
   public ConfigurationBuilderHolder() {
      this.globalConfigurationBuilder = new GlobalConfigurationBuilder();
      this.defaultConfigurationBuilder = new ConfigurationBuilder();
      this.namedConfigurationBuilders = new HashMap<String, ConfigurationBuilder>();
   }
   
   public GlobalConfigurationBuilder getGlobalConfigurationBuilder() {
      return globalConfigurationBuilder;
   }
   
   public ConfigurationBuilder newConfigurationBuilder(String name) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.read(getDefaultConfigurationBuilder().build());
      namedConfigurationBuilders.put(name, builder);
      return builder;
   }
   
   public ConfigurationBuilder getDefaultConfigurationBuilder() {
      return defaultConfigurationBuilder;
   }
   
   public Map<String, ConfigurationBuilder> getNamedConfigurationBuilders() {
      return namedConfigurationBuilders;
   }
   
}
