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


import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.commons.util.Util;

/**
 * Factory for {@link org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash} function. It will try to look
 * into the configuration for consistent hash definitions as follows:
 * consistent-hash.[version]=[fully qualified class implementing ConsistentHash]
 * e.g.
 * <code>infinispan.client.hotrod.hash_function_impl.1=org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1</code>
 * or if using the {@link Configuration} API,
 * <code>configuration.consistentHashImpl(1, org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashV1.class);</code>
 * <p/>
 * If no CH function is defined for a certain version, then it will be defaulted to "org.infinispan.client.hotrod.impl.ConsistentHashV[version]".
 * E.g. if the server indicates that in use CH is version 1, and it is not defined within the configuration, it will be defaulted to
 * org.infinispan.client.hotrod.impl.ConsistentHashV1.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ConsistentHashFactory {
   private Class<? extends ConsistentHash>[] version2ConsistentHash;

   public void init(Configuration configuration) {
      this.version2ConsistentHash = configuration.consistentHashImpl();
   }

   public ConsistentHash newConsistentHash(int version) {
      Class<? extends ConsistentHash> hashFunctionClass = version2ConsistentHash[version-1];
      // TODO: Why create a brand new instance via reflection everytime a new hash topology is received? Caching???
      return Util.getInstance(hashFunctionClass);
   }
}
