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

import java.util.Properties;

import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.Builder;
import org.infinispan.configuration.cache.InterceptorConfiguration.Position;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This builder defines details of a specific custom interceptor.
 */
public class InterceptorConfigurationBuilder extends AbstractCustomInterceptorsConfigurationChildBuilder implements Builder<InterceptorConfiguration> {
   private static final Log log = LogFactory.getLog(InterceptorConfigurationBuilder.class);

   private Class<? extends CommandInterceptor> after;
   private Class<? extends CommandInterceptor> before;
   private CommandInterceptor interceptor;
   private int index = -1;
   private Position position = Position.OTHER_THAN_FIRST_OR_LAST;
   private Properties properties = new Properties();

   InterceptorConfigurationBuilder(CustomInterceptorsConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Dictates that the custom interceptor appears immediately <i>after</i> the specified interceptor. If the specified
    * interceptor is not found in the interceptor chain, a {@link ConfigurationException} will be thrown when the
    * cache starts.
    *
    * @param after the class of the interceptor to look for
    */
   public InterceptorConfigurationBuilder after(Class<? extends CommandInterceptor> after) {
      this.after = after;
      return this;
   }

   /**
    * Dictates that the custom interceptor appears immediately <i>before</i> the specified interceptor. If the specified
    * interceptor is not found in the interceptor chain, a {@link ConfigurationException} will be thrown when the
    * cache starts.
    *
    * @param before the class of the interceptor to look for
    */
   public InterceptorConfigurationBuilder before(Class<? extends CommandInterceptor> before) {
      this.before = before;
      return this;
   }

   /**
    * An instance of the new custom interceptor to add to the configuration.
    * @param interceptor an instance of {@link CommandInterceptor}
    */
   public InterceptorConfigurationBuilder interceptor(CommandInterceptor interceptor) {
      this.interceptor = interceptor;
      return this;
   }

   /**
    * Specifies a position in the interceptor chain to place the new interceptor.  The index starts at 0 and goes up to
    * the number of interceptors in a given configuration.  A {@link ConfigurationException} is thrown if the index is
    * less than 0 or greater than the maximum number of interceptors in the chain.
    *
    * @param i positional index in the interceptor chain to place the new interceptor.
    */
   public InterceptorConfigurationBuilder index(int i) {
      if (i < 0) throw new IllegalArgumentException("Index cannot be negative");
      this.index = i;
      return this;
   }

   /**
    * Specifies a position, denoted by the {@link Position} enumeration, where to place the new interceptor.
    *
    * @param p position to place the new interceptor
    */
   public InterceptorConfigurationBuilder position(Position p) {
      this.position = p;
      return this;
   }

   /**
    * Sets interceptor properties
    *
    * @param properties
    * @return this InterceptorConfigurationBuilder
    */
   public InterceptorConfigurationBuilder withProperties(Properties properties) {
      this.properties = properties;
      return this;
   }

   /**
    * Clears the interceptor properties
    *
    * @return this InterceptorConfigurationBuilder
    */
   public InterceptorConfigurationBuilder clearProperties() {
      this.properties = new Properties();
      return this;
   }

   public InterceptorConfigurationBuilder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
   }

   public InterceptorConfigurationBuilder removeProperty(String key) {
      this.properties.remove(key);
      return this;
   }

   @Override
   public void validate() {
      // Make sure more than one 'position' isn't picked.
      int positions = 0;

      if (before != null) positions++;
      if (after != null) positions++;
      if (index > -1) positions++;
      if (!position.equals(Position.OTHER_THAN_FIRST_OR_LAST)) positions++;

      switch (positions) {
         case 0:
            throw log.missingCustomInterceptorPosition(interceptor.getClass().getName());
         case 1:
            break;
         default:
            throw log.multipleCustomInterceptorPositions(interceptor.getClass().getName());
      }
      if (interceptor == null) {
         throw log.customInterceptorMissingClass();
      }
      if (!(interceptor instanceof BaseCustomInterceptor)) {
         log.suggestCustomInterceptorInheritance(interceptor.getClass().getName());
      }
   }

   @Override
   public InterceptorConfiguration create() {
      return new InterceptorConfiguration(after, before, interceptor, index, position, TypedProperties.toTypedProperties(properties));
   }

   @Override
   public InterceptorConfigurationBuilder read(InterceptorConfiguration template) {
      this.after = template.after();
      this.before = template.before();
      this.index = template.index();
      this.interceptor = template.interceptor();
      this.position = template.position();
      this.properties = new Properties();
      this.properties.putAll(template.properties());

      return this;
   }

   @Override
   public String toString() {
      return "InterceptorConfigurationBuilder [after=" + after + ", before=" + before + ", interceptor=" + interceptor + ", index=" + index + ", position=" + position
            + ", properties=" + properties + "]";
   }

}
