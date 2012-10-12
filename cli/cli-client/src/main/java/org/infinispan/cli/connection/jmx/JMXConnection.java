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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.infinispan.cli.Context;
import org.infinispan.cli.connection.Connection;

public class JMXConnection implements Connection {
   private static final QueryExp INTERPRETER_QUERY = createObjectName("*:type=CacheManager,component=Interpreter,name=*");
   private JMXConnector jmxConnector;
   private Map<String, ObjectInstance> cacheManagers;
   private Map<String, String> sessions;
   private String activeCacheManager;
   private final JMXUrl serviceUrl;
   private MBeanServerConnection mbsc;

   public JMXConnection(final JMXUrl serviceUrl) {
      this.serviceUrl = serviceUrl;
   }

   @Override
   public void connect(Context context) throws Exception {
      JMXServiceURL url = new JMXServiceURL(serviceUrl.getJMXServiceURL());
      jmxConnector = JMXConnectorFactory.connect(url, serviceUrl.getConnectionEnvironment());
      mbsc = jmxConnector.getMBeanServerConnection();
      cacheManagers = new TreeMap<String, ObjectInstance>();
      for (ObjectInstance mbean : mbsc.queryMBeans(null, INTERPRETER_QUERY)) {
         if ("org.infinispan.cli.interpreter.Interpreter".equals(mbean.getClassName())) {
            cacheManagers.put(unquote(mbean.getObjectName().getKeyProperty("name")), mbean);
         }
      }
      cacheManagers = Collections.unmodifiableMap(cacheManagers);
      activeCacheManager = cacheManagers.keySet().iterator().next();
      sessions = new HashMap<String, String>();
   }

   @Override
   public boolean isConnected() {
      return jmxConnector != null;
   }

   @Override
   public void close() throws IOException {
      if (jmxConnector != null) {
         try {
            jmxConnector.close();
         } catch (IOException e) {
            // Ignore
         } finally {
            mbsc = null;
            jmxConnector = null;
         }
      }
   }

   @Override
   public String toString() {
      return serviceUrl.toString();
   }

   @Override
   public Collection<String> getAvailableContainers() {
      return cacheManagers.keySet();
   }

   @Override
   public String getActiveContainer() {
      return activeCacheManager;
   }

   @Override
   public void setActiveContainer(String name) {
      if (cacheManagers.containsKey(name)) {
         activeCacheManager = name;
      } else {
         throw new IllegalArgumentException(name);
      }
   }

   @Override
   public Collection<String> getAvailableCaches() {
      ObjectInstance manager = cacheManagers.get(activeCacheManager);
      try {
         String[] cacheNames = (String[]) mbsc.getAttribute(manager.getObjectName(), "cacheNames");
         List<String> cacheList = Arrays.asList(cacheNames);
         Collections.sort(cacheList);
         return cacheList;
      } catch (Exception e) {
         return Collections.emptyList();
      }
   }

   @Override
   public void execute(Context context) {
      ObjectInstance manager = cacheManagers.get(activeCacheManager);
      String sessionId = sessions.get(activeCacheManager);
      try {
         if(sessionId==null) {
            sessionId = (String) mbsc.invoke(manager.getObjectName(), "createSessionId", new Object[0], new String[0]);
            sessions.put(activeCacheManager, sessionId);
         }
         String result = (String) mbsc.invoke(manager.getObjectName(), "execute", new String[] { sessionId, context
               .getCommandBuffer().toString() }, new String[] { String.class.getName(), String.class.getName() });
         if (result != null) {
            context.println(result);
         }
      } catch (InstanceNotFoundException e) {
         context.error(e);
      } catch (MBeanException e) {
         Exception te = e.getTargetException();
         context.error(te.getCause() != null ? te.getCause() : te);
      } catch (ReflectionException e) {
         context.error(e);
      } catch (IOException e) {
         context.error(e);
      } finally {
         context.getCommandBuffer().reset();
      }
   }

   private static ObjectName createObjectName(String name) {
      try {
         return new ObjectName(name);
      } catch (MalformedObjectNameException e) {
         throw new RuntimeException(e);
      }
   }

   public static String unquote(String s) {
      if (s != null && ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'")))) {
         s = s.substring(1, s.length() - 1);
      }
      return s;
   }
}
