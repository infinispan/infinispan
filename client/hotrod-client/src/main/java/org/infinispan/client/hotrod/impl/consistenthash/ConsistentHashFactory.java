/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.client.hotrod.impl.consistenthash;


import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.infinispan.client.hotrod.impl.ConfigurationProperties.HASH_FUNCTION_PREFIX;

/**
 * Factory for {@link org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash} function. It will try to look
 * into the configuration for consistent hash definitions as follows:
 * consistent-hash.[version]=[fully qualified class implementing ConsistentHash]
 * e.g.
 * infinispan.client.hotrod.hash_function_impl.1=org.infinispan.client.hotrod.impl.consistenthash.ConsitentHashV1
 * <p/>
 * If no CH function is defined for a certain version, then it will be defaulted to "org.infinispan.client.hotrod.impl.ConsistentHashV[version]".
 * E.g. if the server indicates that in use CH is version 1, and it is not defined within the configuration, it will be defaulted to
 * org.infinispan.client.hotrod.impl.ConsistentHashV1.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ConsistentHashFactory {

   private static final Log log = LogFactory.getLog(ConsistentHashFactory.class);

   private final Map<Integer, String> version2ConsistentHash = new HashMap<Integer, String>();

   private ClassLoader classLoader;
   
   public void init(ConfigurationProperties config, ClassLoader classLoader) {
	  this.classLoader = classLoader;
      for (String propName : config.getProperties().stringPropertyNames()) {
         if (propName.startsWith(HASH_FUNCTION_PREFIX)) {
            if (log.isTraceEnabled()) log.tracef("Processing consistent hash: %s", propName);
            String versionString = propName.substring((HASH_FUNCTION_PREFIX + ".").length());
            int version = Integer.parseInt(versionString);
            String hashFunction = config.getProperties().getProperty(propName);
            version2ConsistentHash.put(version, hashFunction);
            if (log.isTraceEnabled()) {
               log.tracef("Added consistent hash version %d: %s", version, hashFunction);
            }
         }
      }
   }

   public ConsistentHash newConsistentHash(int version) {
      String hashFunctionClass = version2ConsistentHash.get(version);
      if (hashFunctionClass == null) {
         if (log.isTraceEnabled()) log.tracef("No hash function configured for version %d", version);
         hashFunctionClass = ConsistentHashFactory.class.getPackage().getName() + ".ConsistentHashV" + version;
         if (log.isTraceEnabled()) log.tracef("Trying to use default value: %s", hashFunctionClass);
         version2ConsistentHash.put(version, hashFunctionClass);
      }
      // TODO: Why create a brand new instance via reflection everytime a new hash topology is received? Caching???
      return (ConsistentHash) Util.getInstance(hashFunctionClass, classLoader);
   }

   public Map<Integer, String> getVersion2ConsistentHash() {
      return Collections.unmodifiableMap(version2ConsistentHash);
   }
}
