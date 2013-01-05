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
package org.infinispan.cli.connection.jmx;

import java.util.HashMap;
import java.util.Map;

import javax.management.remote.JMXConnector;

public abstract class AbstractJMXUrl implements JMXUrl {
   protected String hostname;
   protected int port;
   protected String username;
   protected String password;
   protected String container;
   protected String cache;

   @Override
   public String getContainer() {
      return container;
   }

   @Override
   public String getCache() {
      return cache;
   }

   @Override
   public boolean needsCredentials() {
      return username != null && password == null;
   }

   @Override
   public Map<String, Object> getConnectionEnvironment(String credentials) {
      Map<String, Object> env = new HashMap<String, Object>();
      if (username != null) {
         env.put(JMXConnector.CREDENTIALS, new String[] { username, credentials != null ? credentials : password });
      }
      return env;
   }

}
