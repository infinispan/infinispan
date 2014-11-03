package org.infinispan.cli.connection.jmx;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
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

import org.infinispan.cli.CommandBuffer;
import org.infinispan.cli.Context;
import org.infinispan.cli.commands.ProcessedCommand;
import org.infinispan.cli.connection.Connection;

public class JMXConnection implements Connection {
   private static final QueryExp INTERPRETER_QUERY = createObjectName("*:type=CacheManager,component=Interpreter,name=*");
   private JMXConnector jmxConnector;
   private Map<String, ObjectInstance> cacheManagers;
   private Map<String, String> sessions;
   private String activeCacheManager;
   private String activeCache;
   private final JMXUrl serviceUrl;
   private MBeanServerConnection mbsc;

   public JMXConnection(final JMXUrl serviceUrl) {
      this.serviceUrl = serviceUrl;
   }

   @Override
   public boolean needsCredentials() {
      return serviceUrl.needsCredentials();
   }

   @Override
   public void connect(String credentials) throws Exception {
      JMXServiceURL url = new JMXServiceURL(serviceUrl.getJMXServiceURL());
      try {
         jmxConnector = JMXConnectorFactory.connect(url, serviceUrl.getConnectionEnvironment(credentials));
      } catch (Throwable t) {
         throw new Exception(t);
      }
      mbsc = jmxConnector.getMBeanServerConnection();
      cacheManagers = new TreeMap<String, ObjectInstance>();
      for (ObjectInstance mbean : mbsc.queryMBeans(null, INTERPRETER_QUERY)) {
         if ("org.infinispan.cli.interpreter.Interpreter".equals(mbean.getClassName())) {
            cacheManagers.put(unquote(mbean.getObjectName().getKeyProperty("name")), mbean);
         }
      }
      if (cacheManagers.size() == 0) {
         throw new IllegalArgumentException("The remote server does not expose any CacheManagers");
      }
      sessions = new HashMap<String, String>();
      cacheManagers = Collections.unmodifiableMap(cacheManagers);
      activeCacheManager = serviceUrl.getContainer();
      if (activeCacheManager == null) {
         activeCacheManager = cacheManagers.keySet().iterator().next();
      } else if (!cacheManagers.containsKey(activeCacheManager)) {
         throw new Exception("No such container: " + activeCacheManager);
      }
      activeCache = serviceUrl.getCache();
      if (activeCache != null) {
         try {
            getSession(cacheManagers.get(activeCacheManager));
         } catch (MBeanException e) {
            Throwable ue = unwrapException(e);
            throw new Exception(ue.getMessage(), ue);
         }
      }

   }

   private Throwable unwrapException(MBeanException e) {
      Exception te = e.getTargetException();
      if (te != null) {
         if (te instanceof InvocationTargetException) {
            return ((InvocationTargetException) te).getCause();
         } else {
            return te;
         }
      } else {
         return e;
      }
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
   public String getActiveCache() {
      return activeCache;
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
   public void execute(Context context, CommandBuffer commandBuffer) {
      ObjectInstance manager = cacheManagers.get(activeCacheManager);
      // Shallow copy of the commands to avoid being cleared when the buffer is reset
      List<ProcessedCommand> bufferedCommands = new ArrayList<ProcessedCommand>(commandBuffer.getBufferedCommands());
      try {
         String sessionId = getSession(manager);
         Map<String, String> response = (Map<String, String>) mbsc.invoke(manager.getObjectName(), "execute", new String[] { sessionId, commandBuffer.toString() },
               new String[] { String.class.getName(), String.class.getName() });
         if (response.containsKey("OUTPUT")) {
            context.result(bufferedCommands, response.get("OUTPUT"), false);
         }
         if (response.containsKey("ERROR")) {
            context.result(bufferedCommands, response.get("ERROR"), true);
         }
         if (response.containsKey("CACHE")) {
            activeCache = response.get("CACHE");
         }
      } catch (InstanceNotFoundException e) {
         context.result(bufferedCommands, e.toString(), true);
      } catch (MBeanException e) {
         Exception te = e.getTargetException();
         context.result(bufferedCommands, te.getCause() != null ? te.getCause().toString() : te.toString(), true);
      } catch (ReflectionException e) {
         context.result(bufferedCommands, e.toString(), true);
      } catch (IOException e) {
         context.result(bufferedCommands, e.toString(), true);
      }
   }

   private String getSession(ObjectInstance manager) throws InstanceNotFoundException, MBeanException, ReflectionException, IOException {
      String sessionId = sessions.get(activeCacheManager);
      if (sessionId == null) {
         sessionId = (String) mbsc.invoke(manager.getObjectName(), "createSessionId", new Object[] { activeCache }, new String[] { String.class.getName() });
         sessions.put(activeCacheManager, sessionId);
      }
      return sessionId;
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
