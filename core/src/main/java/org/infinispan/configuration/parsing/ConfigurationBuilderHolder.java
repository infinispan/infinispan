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
   private ConfigurationBuilder currentConfigurationBuilder;
   private final Map<Class<? extends ConfigurationParser<?>>, ParserContext> parserContexts;
   private final ClassLoader classLoader;

   public ConfigurationBuilderHolder(ClassLoader classLoader) {
      this.globalConfigurationBuilder = new GlobalConfigurationBuilder();
      this.defaultConfigurationBuilder = new ConfigurationBuilder();
      this.namedConfigurationBuilders = new HashMap<String, ConfigurationBuilder>();
      this.currentConfigurationBuilder = defaultConfigurationBuilder;
      this.parserContexts = new HashMap<Class<? extends ConfigurationParser<?>>, ParserContext>();
      this.classLoader = classLoader;
   }

   public GlobalConfigurationBuilder getGlobalConfigurationBuilder() {
      return globalConfigurationBuilder;
   }

   public ConfigurationBuilder newConfigurationBuilder(String name) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      //no need to validate default config again
      //https://issues.jboss.org/browse/ISPN-1938
      builder.read(getDefaultConfigurationBuilder().build(false));
      namedConfigurationBuilders.put(name, builder);
      currentConfigurationBuilder = builder;
      return builder;
   }

   public ConfigurationBuilder getDefaultConfigurationBuilder() {
      return defaultConfigurationBuilder;
   }

   public Map<String, ConfigurationBuilder> getNamedConfigurationBuilders() {
      return namedConfigurationBuilders;
   }

   public ConfigurationBuilder getCurrentConfigurationBuilder() {
      return currentConfigurationBuilder;
   }

   @SuppressWarnings("unchecked")
   public <T extends ParserContext> T getParserContext(Class<? extends ConfigurationParser<?>> parserClass) {
      return (T) parserContexts.get(parserClass);
   }

   public void setParserContext(Class<? extends ConfigurationParser<?>> parserClass, ParserContext context) {
      parserContexts.put(parserClass, context);
   }

   public ClassLoader getClassLoader() {
      return classLoader;
   }

   Map<Class<? extends ConfigurationParser<?>>, ParserContext> getParserContexts() {
      return parserContexts;
   }

}
