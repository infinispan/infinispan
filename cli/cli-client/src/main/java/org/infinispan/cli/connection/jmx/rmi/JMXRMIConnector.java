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
package org.infinispan.cli.connection.jmx.rmi;

import org.infinispan.cli.connection.Connection;
import org.infinispan.cli.connection.Connector;
import org.infinispan.cli.connection.jmx.JMXConnection;

public class JMXRMIConnector implements Connector {

   public JMXRMIConnector() {
   }

   @Override
   public Connection getConnection(final String connectionString) {
      try {
         return new JMXConnection(new JMXRMIUrl(connectionString));
      } catch (Exception e) {
         return null;
      }
   }


}
