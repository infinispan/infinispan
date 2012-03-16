/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.jdbc.connectionfactory;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.util.Util;

import java.sql.Connection;

/**
 * Defines the functionality a connection factory should implement.
 *
 * @author Mircea.Markus@jboss.com
 */
public abstract class ConnectionFactory {

   /**
    * Constructs a {@link org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory} based on the supplied class
    * name.
    */
   public static ConnectionFactory getConnectionFactory(String connectionFactoryClass, ClassLoader classLoader) throws CacheLoaderException {
      return (ConnectionFactory) Util.getInstance(connectionFactoryClass, classLoader);
   }

   /**
    * Starts the connection factory. A pooled factory might be create connections here.
    */
   public abstract void start(ConnectionFactoryConfig config, ClassLoader classLoader) throws CacheLoaderException;

   /**
    * Closes the connection factory, including all allocated connections etc.
    */
   public abstract void stop();

   /**
    * Fetches a connection from the factory.
    */
   public abstract Connection getConnection() throws CacheLoaderException;

   /**
    * Destroys a connection. Important: null might be passed in, as an valid argument.
    */
   public abstract void releaseConnection(Connection conn);
}
