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

/**
 * Contains configuration elements for a {@link org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class ConnectionFactoryConfig implements Cloneable {

   private String connectionFactoryClass;
   private String driverClass;
   private String connectionUrl;
   private String userName;
   private String password;
   private String datasourceJndiLocation;

   public ConnectionFactoryConfig(String connectionFactoryClass, String driverClass, String connectionUrl,
                                  String userName, String password) {
      this.connectionFactoryClass = connectionFactoryClass;
      this.driverClass = driverClass;
      this.connectionUrl = connectionUrl;
      this.userName = userName;
      this.password = password;
   }

   public ConnectionFactoryConfig() {
   }

   public String getDriverClass() {
      return driverClass;
   }

   public String getConnectionUrl() {
      return connectionUrl;
   }

   public String getUserName() {
      return userName;
   }

   public String getPassword() {
      return password;
   }

   public void setDriverClass(String driverClass) {
      this.driverClass = driverClass;
   }

   public void setConnectionUrl(String connectionUrl) {
      this.connectionUrl = connectionUrl;
   }

   public void setUserName(String userName) {
      this.userName = userName;
   }

   public void setPassword(String password) {
      this.password = password;
   }

   public void setConnectionFactoryClass(String connectionFactoryClass) {
      this.connectionFactoryClass = connectionFactoryClass;
   }

   public String getConnectionFactoryClass() {
      return connectionFactoryClass;
   }

   public void setDatasourceJndiLocation(String location) {
      datasourceJndiLocation = location;
   }

   public String getDatasourceJndiLocation() {
      return datasourceJndiLocation;
   }

   @Override
   public ConnectionFactoryConfig clone() {
      try {
         return (ConnectionFactoryConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public String toString() {
      return "ConnectionFactoryConfig{" +
            "connectionFactoryClass='" + connectionFactoryClass + '\'' +
            ", driverClass='" + driverClass + '\'' +
            ", connectionUrl='" + connectionUrl + '\'' +
            ", userName='" + userName + '\'' +
            '}';
   }
}
