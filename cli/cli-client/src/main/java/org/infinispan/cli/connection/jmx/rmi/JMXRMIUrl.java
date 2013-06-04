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

import static org.infinispan.cli.util.Utils.nullIfEmpty;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.infinispan.cli.connection.jmx.AbstractJMXUrl;

public class JMXRMIUrl extends AbstractJMXUrl {
   private static final Pattern JMX_URL = Pattern.compile("^(?:(?![^:@]+:[^:@/]*@)(jmx):)?(?://)?((?:(([^:@]*):?([^:@]*))?@)?(\\[[0-9A-Fa-f:]+\\]|[^:/?#]*)(?::(\\d*))?)(?:/([^/]*)(?:/(.*))?)?");

   public JMXRMIUrl(String connectionString) {
      Matcher matcher = JMX_URL.matcher(connectionString);
      if (!matcher.matches()) {
         throw new IllegalArgumentException(connectionString);
      }
      username = nullIfEmpty(matcher.group(4));
      password = nullIfEmpty(matcher.group(5));
      hostname = nullIfEmpty(matcher.group(6));
      port = Integer.parseInt(matcher.group(7));
      container = nullIfEmpty(matcher.group(8));
      cache = nullIfEmpty(matcher.group(9));
   }

   @Override
   public String getJMXServiceURL() {
      return "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";
   }

   @Override
   public String toString() {
      return "jmx://" + (username == null ? "" : username + "@") + hostname + ":" + port;
   }
}
