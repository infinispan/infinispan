/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
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
package org.infinispan.loaders.remote.configuration;

import org.infinispan.configuration.Builder;

public class RemoteServerConfigurationBuilder extends AbstractRemoteCacheStoreConfigurationChildBuilder<RemoteCacheStoreConfigurationBuilder> implements
      Builder<RemoteServerConfiguration> {
   private String host;
   private int port = 11222;

   RemoteServerConfigurationBuilder(RemoteCacheStoreConfigurationBuilder builder) {
      super(builder);
   }

   public RemoteServerConfigurationBuilder host(String host) {
      this.host = host;
      return this;
   }

   public RemoteServerConfigurationBuilder port(int port) {
      this.port = port;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public RemoteServerConfiguration create() {
      return new RemoteServerConfiguration(host, port);
   }

   @Override
   public RemoteServerConfigurationBuilder read(RemoteServerConfiguration template) {
      this.host = template.host();
      this.port = template.port();

      return this;
   }

}
