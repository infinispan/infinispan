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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.configuration.Builder;

/**
 * Configures custom interceptors to be added to the cache.
 *
 * @author pmuir
 *
 */
public class CustomInterceptorsConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<CustomInterceptorsConfiguration> {

   private List<InterceptorConfigurationBuilder> interceptorBuilders = new LinkedList<InterceptorConfigurationBuilder>();

   CustomInterceptorsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Adds a new custom interceptor definition, to be added to the cache when the cache is started.
    */
   public InterceptorConfigurationBuilder addInterceptor() {
      InterceptorConfigurationBuilder builder = new InterceptorConfigurationBuilder(this);
      this.interceptorBuilders.add(builder);
      return builder;
   }

   @Override
   public void validate() {
      for (InterceptorConfigurationBuilder builder : interceptorBuilders) builder.validate();
   }

   @Override
   public CustomInterceptorsConfiguration create() {
      if (interceptorBuilders.isEmpty()) {
         return new CustomInterceptorsConfiguration();
      } else {
         List<InterceptorConfiguration> interceptors = new ArrayList<InterceptorConfiguration>(interceptorBuilders.size());
         for (InterceptorConfigurationBuilder builder : interceptorBuilders) interceptors.add(builder.create());
         return new CustomInterceptorsConfiguration(interceptors);
      }
   }

   @Override
   public CustomInterceptorsConfigurationBuilder read(CustomInterceptorsConfiguration template) {
      this.interceptorBuilders = new LinkedList<InterceptorConfigurationBuilder>();
      for (InterceptorConfiguration c : template.interceptors()) {
         this.interceptorBuilders.add(new InterceptorConfigurationBuilder(this).read(c));
      }
      return this;
   }

   @Override
   public String toString() {
      return "CustomInterceptorsConfigurationBuilder{" +
            "interceptors=" + interceptorBuilders +
            '}';
   }

}
