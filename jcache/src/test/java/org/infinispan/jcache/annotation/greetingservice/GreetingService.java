/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.jcache.annotation.greetingservice;

import java.util.HashSet;
import java.util.Set;

import javax.cache.annotation.CacheResult;

public class GreetingService {

   private final Set<String> greetMethodInvokedKeySet = new HashSet<String>();

   @CacheResult
   public String greet(final String name) {
      if (greetMethodInvokedKeySet.contains(name)) {
         throw new IllegalStateException(String.format("Method already invoked with param %s!", name));
      } else {
         greetMethodInvokedKeySet.add(name);
      }

      return hello(name);
   }

   @CacheResult
   public String greetWithDefaultCacheConfig(final String name) {
      return hello(name);
   }

   @CacheResult
   public String greetTwoWithDefaultCacheConfig(final String name) {
      return hello(name);
   }

   private String hello(final String name) {
      return "Hello " + name + " :)";
   }
}