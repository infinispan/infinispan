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
package org.infinispan.cli.connection.jmx.remoting;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.remote.JMXConnector;

import org.infinispan.cli.connection.jmx.JMXUrl;

public class JMXRemotingUrl implements JMXUrl {
   private static final Pattern JMX_URL = Pattern.compile("^(?:(?![^:@]+:[^:@/]*@)(remoting):)?(?://)?((?:(([^:@]*):?([^:@]*))?@)?([^:/?#]*)(?::(\\d*))?)(?:/([^/]*)(?:/(.*))?)?");
   private static final int DEFAULT_REMOTING_PORT = 9999;
   protected final String hostname;
   protected final int port;
   protected final String username;
   protected final String password;
   protected final String container;
   protected final String cache;

   public JMXRemotingUrl(String connectionString) {
      Matcher matcher = JMX_URL.matcher(connectionString);
      if (!matcher.matches()) {
         throw new IllegalArgumentException(connectionString);
      }
      username = matcher.group(4);
      password = matcher.group(5);
      hostname = matcher.group(6);
      if (matcher.group(7) != null) {
         port = Integer.parseInt(matcher.group(7));
      } else {
         port = DEFAULT_REMOTING_PORT;
      }
      container = matcher.group(8);
      cache = matcher.group(9);
   }

   @Override
   public String getJMXServiceURL() {
      return "service:jmx:remoting-jmx://" + hostname + ":" + port;
   }

   @Override
   public String getContainer() {
      return container;
   }

   @Override
   public String getCache() {
      return cache;
   }

   @Override
   public Map<String, Object> getConnectionEnvironment() {
      Map<String, Object> env = new HashMap<String, Object>();
      if (username != null || password != null) {
         env.put(JMXConnector.CREDENTIALS, new String[] { username, password });
      }
      return env;
   }

   @Override
   public String toString() {
      return "remoting://" + (username == null ? "" : username + "@") + hostname + ":" + port;
   }
}
