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
package org.infinispan.loaders.cassandra.configuration;

import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.Builder;

/**
 *
 * CassandraServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class CassandraServerConfigurationBuilder extends AbstractCassandraCacheStoreConfigurationChildBuilder<CassandraCacheStoreConfigurationBuilder> implements Builder<CassandraServerConfiguration> {

   protected CassandraServerConfigurationBuilder(CassandraCacheStoreConfigurationBuilder builder) {
      super(builder);
   }

   private String host;
   private int port = 9160;

   public CassandraServerConfigurationBuilder host(String host) {
      this.host = host;
      return this;
   }

   public CassandraServerConfigurationBuilder port(int port) {
      this.port = port;
      return this;
   }

   @Override
   public void validate() {
      if (host==null) {
         throw new ConfigurationException("Missing host");
      }
   }

   @Override
   public CassandraServerConfiguration create() {
      return new CassandraServerConfiguration(host, port);
   }

   @Override
   public CassandraServerConfigurationBuilder read(CassandraServerConfiguration template) {
      this.host = template.host();
      this.port = template.port();

      return this;
   }

}
