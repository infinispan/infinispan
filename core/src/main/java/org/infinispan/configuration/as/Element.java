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
 * An enumeration of all the recognized XML element local names, by name.
 *
 * @author Tristan Tarrant
 */
public enum Element {
   // must be first
   UNKNOWN(null),

   CACHE_CONTAINER("cache-container"),
   DISTRIBUTED_CACHE("distributed-cache"),
   EVICTION("eviction"),
   EXPIRATION("expiration"),
   FILE_STORE("file-store"),
   INET_ADDRESS("inet-address"),
   INTERFACE("interface"),
   INTERFACES("interfaces"),
   INVALIDATION_CACHE("invalidation-cache"),
   LOCAL_CACHE("local-cache"),
   LOCKING("locking"),
   OUTBOUND_SOCKET_BINDING("outbound-socket-binding"),
   PROFILE("profile"),
   PROPERTY("property"),
   PROTOCOL("protocol"),
   REMOTE_DESTINATION("remote-destination"),
   REPLICATED_CACHE("replicated-cache"),
   ROOT("server"),
   SOCKET_BINDING("socket-binding"),
   SOCKET_BINDING_GROUP("socket-binding-group"),
   STACK("stack"),
   STATE_TRANSFER("state-transfer"),
   STORE("store"),
   SUBSYSTEM("subsystem"),
   TRANSACTION("transaction"),
   TRANSPORT("transport"),
   WRITE_BEHIND("write-behind"),
   THREAD_FACTORY("thread-factory"),
   UNBOUNDED_QUEUE_THREAD_POOL("unbounded-queue-thread-pool"),
   BOUNDED_QUEUE_THREAD_POOL("bounded-queue-thread-pool"),
   BLOCKING_BOUNDED_QUEUE_THREAD_POOL("blocking-bounded-queue-thread-pool"),
   QUEUELESS_THREAD_POOL("queueless-thread-pool"),
   SCHEDULED_THREAD_POOL("scheduled-thread-pool"), ;

   private static final Map<String, Element> MAP;

   static {
      final Map<String, Element> map = new HashMap<String, Element>(8);
      for (Element element : values()) {
         final String name = element.getLocalName();
         if (name != null)
            map.put(name, element);
      }
      MAP = map;
   }

   public static Element forName(String localName) {
      final Element element = MAP.get(localName);
      return element == null ? UNKNOWN : element;
   }

   private final String name;

   Element(final String name) {
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
