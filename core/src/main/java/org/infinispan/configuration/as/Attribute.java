/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package org.infinispan.configuration.as;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used in Infinispan
 *
 * @author Tristan Tarrant
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),

   ACQUIRE_TIMEOUT("acquire-timeout"),
   ALIASES("aliases"),
   ASYNC_MARSHALLING("async-marshalling"),
   BATCHING("batching"),
   CACHE("cache"),
   CHUNK_SIZE("chunk-size"),
   CLASS("class"),
   CLUSTER("cluster"),
   CONCURRENCY_LEVEL("concurrency-level"),
   DEFAULT_CACHE("default-cache"),
   DEFAULT_CACHE_CONTAINER("default-cache-container"),
   DEFAULT_EXECUTOR("default-executor"),
   DEFAULT_INTERFACE("default-interface"),
   DIAGNOSTICS_SOCKET_BINDING("diagnostics-socket-binding"),
   ENABLED("enabled"),
   EVICTION_EXECUTOR("eviction-executor"),
   EXECUTOR("executor"),
   FETCH_STATE("fetch-state"),
   FLUSH_LOCK_TIMEOUT("flush-lock-timeout"),
   HOST("host"),
   INDEXING("indexing"),
   INTERFACE("interface"),
   INTERVAL("interval"),
   ISOLATION("isolation"),
   JNDI_NAME("jndi-name"),
   L1_LIFESPAN("l1-lifespan"),
   LIFESPAN("lifespan"),
   LISTENER_EXECUTOR("listener-executor"),
   LOCK_TIMEOUT("lock-timeout"),
   LOCKING("locking"),
   MACHINE("machine"),
   MAX_ENTRIES("max-entries"),
   MAX_IDLE("max-idle"),
   MODE("mode"),
   MODIFICATION_QUEUE_SIZE("modification-queue-size"),
   MODULE("module"),
   NAME("name"),
   OOB_EXECUTOR("oob-executor"),
   OWNERS("owners"),
   PASSIVATION("passivation"),
   PATH("path"),
   PORT("port"),
   PORT_OFFSET("port-offset"),
   PRELOAD("preload"),
   PURGE("purge"),
   QUEUE_FLUSH_INTERVAL("queue-flush-interval"),
   QUEUE_SIZE("queue-size"),
   RACK("rack"),
   RELATIVE_TO("relative-to"),
   REMOTE_TIMEOUT("remote-timeout"),
   REPLICATION_QUEUE_EXECUTOR("replication-queue-executor"),
   SHARED("shared"),
   SHUTDOWN_TIMEOUT("shutdown-timeout"),
   SINGLETON("singleton"),
   SITE("site"),
   SOCKET_BINDING("socket-binding"),
   SOCKET_TIMEOUT("socket-timeout"),
   STACK("stack"),
   START("start"),
   STOP_TIMEOUT("stop-timeout"),
   STRATEGY("strategy"),
   STRIPING("striping"),
   TCP_NO_DELAY("tcp-no-delay"),
   THREAD_FACTORY("thread-factory"),
   THREAD_POOL_SIZE("thread-pool-size"),
   TIMEOUT("timeout"),
   TIMER_EXECUTOR("timer-executor"),
   TYPE("type"),
   VALUE("value"),
   VIRTUAL_NODES("virtual-nodes"), ;

   private static final Map<String, Attribute> attributes;

   static {
      final Map<String, Attribute> map = new HashMap<String, Attribute>(64);
      for (Attribute attribute : values()) {
         final String name = attribute.getLocalName();
         if (name != null)
            map.put(name, attribute);
      }
      attributes = map;
   }

   public static Attribute forName(String localName) {
      final Attribute attribute = attributes.get(localName);
      return attribute == null ? UNKNOWN : attribute;
   }

   private final String name;

   private Attribute(final String name) {
      this.name = name;
   }

   /**
    * Get the local name of this element.
    *
    * @return the local name
    */
   public String getLocalName() {
      return name;
   }
}
