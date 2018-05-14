/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.infinispan.commons.logging.log4j;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.core.AbstractLifeCycle;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.routing.PurgePolicy;
import org.apache.logging.log4j.core.appender.routing.RoutingAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

/**
 * Policy is purging appenders that were not in use specified time, or sooner if there are too many active appenders
 */
@Plugin(name = "BoundedPurgePolicy", category = Core.CATEGORY_NAME, printObject = true)
public class BoundedPurgePolicy extends AbstractLifeCycle implements PurgePolicy {
   public static final String VALUE = "";
   private final int maxSize;
   private final Map<String, String> appendersUsage;
   private String excludePrefix;
   private RoutingAppender routingAppender;

   public BoundedPurgePolicy(final int maxSize, String excludePrefix) {
      this.maxSize = maxSize;
      this.appendersUsage = new LinkedHashMap<>((int) (maxSize * 0.75f), 0.75f, true);
      this.excludePrefix = excludePrefix;
   }

   @Override
   public void initialize(@SuppressWarnings("hiding") final RoutingAppender routingAppender) {
      this.routingAppender = routingAppender;
   }

   @Override
   public boolean stop(final long timeout, final TimeUnit timeUnit) {
      setStopped();
      return true;
   }

   /**
    * Delete the oldest appenders (sorted by their last access time) until there are maxSize appenders or less.
    */
   @Override
   public void purge() {
      synchronized (this) {
         Iterator<String> iterator = appendersUsage.keySet().iterator();
         while (appendersUsage.size() > maxSize) {
            String key = iterator.next();
            LOGGER.debug("Removing appender " + key);
            iterator.remove();
            routingAppender.getAppenders().get(key).getAppender().stop();
            routingAppender.deleteAppender(key);
         }
      }
   }

   @Override
   public void update(final String key, final LogEvent event) {
      if (key != null && key.startsWith(excludePrefix)) {
         return;
      }
      synchronized (this) {
         String previous = appendersUsage.putIfAbsent(key, VALUE);
         if (previous == null) {
            purge();
         }
      }
   }

   /**
    * Create the PurgePolicy
    *
    * @param size the maximum number of appenders to keep active at any moment
    * @param excludePrefix a prefix to exclude from eviction, defaults to "${" for missing key.
    * @return The Routes container.
    */
   @PluginFactory
   public static PurgePolicy createPurgePolicy(
      @PluginAttribute("size") final int size,
      @PluginAttribute(value = "excludePrefix", defaultString = "${") final String excludePrefix) {

      return new BoundedPurgePolicy(size, excludePrefix);
   }

   @Override
   public String toString() {
      return "size=" + maxSize;
   }

}
