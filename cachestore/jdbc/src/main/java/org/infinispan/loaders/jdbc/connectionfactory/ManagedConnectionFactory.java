/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
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
package org.infinispan.loaders.jdbc.connectionfactory;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.sql.DataSource;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Connection factory that can be used when on managed environments, like application servers. It knows how to look into
 * the JNDI tree at a certain location (configurable) and delegate connection management to the DataSource. In order to
 * enable it one should set the following two properties in any Jdbc cache store:
 * <pre>
 *    <property name="connectionFactoryClass" value="org.infinispan.loaders.jdbc.connectionfactory.ManagedConnectionFactory"/>
 *    <property name="datasourceJndiLocation" value="java:/ManagedConnectionFactoryTest/DS"/>
 * </pre>
 *
 * @author Mircea.Markus@jboss.com
 */
public class ManagedConnectionFactory extends ConnectionFactory {

   private static final Log log = LogFactory.getLog(ManagedConnectionFactory.class);
   private static final boolean trace = log.isTraceEnabled();

   private DataSource dataSource;

   public void start(ConnectionFactoryConfig config) throws CacheLoaderException {
      InitialContext ctx = null;
      String datasourceName = config.getDatasourceJndiLocation();
      try {
         ctx = new InitialContext();
         dataSource = (DataSource) ctx.lookup(datasourceName);
         if (trace) {
            log.trace("Datasource lookup for " + datasourceName + " succeeded: " + dataSource);
         }
         if (dataSource == null) {
            String msg = "Could not find a connection in jndi under the name '" + datasourceName + "'";
            log.error(msg);
            throw new CacheLoaderException(msg);
         }
      }
      catch (NamingException e) {
         log.error("Could not lookup connection with datasource " + datasourceName, e);
         throw new CacheLoaderException(e);
      }
      finally {
         if (ctx != null) {
            try {
               ctx.close();
            }
            catch (NamingException e) {
               log.warn("Failed to close naming context.", e);
            }
         }
      }
   }

   public void stop() {
   }

   public Connection getConnection() throws CacheLoaderException {
      Connection connection;
      try {
         connection = dataSource.getConnection();
      } catch (SQLException e) {
         log.error(e);
         throw new CacheLoaderException(e);
      }
      if (trace) {
         log.trace("Connection checked out: " + connection);
      }
      return connection;

   }

   public void releaseConnection(Connection conn) {
      try {
         conn.close();
      } catch (SQLException e) {
         log.warn("Issues while closing connection " + conn, e);
      }
   }
}
