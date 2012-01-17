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

import java.util.Collections;
import java.util.List;

/**
 * Configures custom interceptors to be added to the cache.
 * 
 * @author pmuir
 */
public class CustomInterceptorsConfiguration {

   private final List<InterceptorConfiguration> interceptors;

   CustomInterceptorsConfiguration(List<InterceptorConfiguration> interceptors) {
      this.interceptors = interceptors;
   }

   public CustomInterceptorsConfiguration() {
      this.interceptors = Collections.emptyList();
   }

   /**
    * This specifies a list of {@link InterceptorConfiguration} instances to be referenced when building the interceptor
    * chain.
    * @return A list of {@link InterceptorConfiguration}s. May be an empty list, will never be null.
    */
   public List<InterceptorConfiguration> interceptors() {
      return interceptors;
   }
}
