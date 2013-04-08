/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.memcached.configuration;

import org.infinispan.configuration.Builder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;

/**
 * MemcachedServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class MemcachedServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<MemcachedServerConfiguration, MemcachedServerConfigurationBuilder> implements
      Builder<MemcachedServerConfiguration> {

   public MemcachedServerConfigurationBuilder() {
      super(11211);
   }

   @Override
   public MemcachedServerConfigurationBuilder self() {
      return this;
   }

   @Override
   public MemcachedServerConfiguration create() {
      return new MemcachedServerConfiguration(host, port, idleTimeout, recvBufSize, sendBufSize, tcpNoDelay, workerThreads);
   }

   public MemcachedServerConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public MemcachedServerConfiguration build() {
      return build(true);
   }

}
