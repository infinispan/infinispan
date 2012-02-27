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
package org.infinispan.test.fwk;

import org.infinispan.config.Configuration;
import org.infinispan.config.FluentConfiguration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.Marshaller;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.LegacyKeySupportSystemProperties;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.test.fwk.JGroupsConfigBuilder.getJGroupsConfig;

/**
 * CacheManagers in unit tests should be created with this factory, in order to avoid resource clashes. See
 * http://community.jboss.org/wiki/ParallelTestSuite for more details.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 */
public class TestCacheManagerFactory {


   private static AtomicInteger jmxDomainPostfix = new AtomicInteger();

   public static final String MARSHALLER = LegacyKeySupportSystemProperties.getProperty("infinispan.test.marshaller.class", "infinispan.marshaller.class");
   private static final Log log = LogFactory.getLog(TestCacheManagerFactory.class);

   private static ThreadLocal<PerThreadCacheManagers> perThreadCacheManagers = new ThreadLocal<PerThreadCacheManagers>() {
      @Override
      protected PerThreadCacheManagers initialValue() {
         return new PerThreadCacheManagers();
      }
   };

   private static DefaultCacheManager newDefaultCacheManager(boolean start, GlobalConfiguration gc, Configuration c, boolean keepJmxDomain) {
      if (!keepJmxDomain) {
         gc.setJmxDomain("infinispan" + jmxDomainPostfix.incrementAndGet());
      }
      return newDefaultCacheManager(start, gc, c);
   }

   private static DefaultCacheManager newDefaultCacheManager(boolean start, GlobalConfigurationBuilder gc, ConfigurationBuilder c, boolean keepJmxDomain) {
      if (!keepJmxDomain) {
         gc.globalJmxStatistics().jmxDomain("infinispan" + jmxDomainPostfix.incrementAndGet());
      }
      return newDefaultCacheManager(start, gc, c);
   }

   public static EmbeddedCacheManager fromXml(String xmlFile) throws IOException {
      return new DefaultCacheManager(xmlFile);
   }

   public static EmbeddedCacheManager fromStream(InputStream is) throws IOException {
      return new DefaultCacheManager(is);
   }

   /**
    * Creates an cache manager that does not support clustering.
    *
    * @param transactional if true, the cache manager will support transactions by default.
    */
   public static EmbeddedCacheManager createLocalCacheManager(boolean transactional) {
      return createLocalCacheManager(transactional, -1);
   }

   public static EmbeddedCacheManager createLocalCacheManager(boolean transactional, long lockAcquisitionTimeout) {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      amendMarshaller(globalConfiguration);
      minimizeThreads(globalConfiguration);
      Configuration c = new Configuration();
      markAsTransactional(transactional, c);
      if (lockAcquisitionTimeout > -1) c.setLockAcquisitionTimeout(lockAcquisitionTimeout);
      updateTransactionSupport(c);
      return newDefaultCacheManager(true, globalConfiguration, c, false);
   }

   private static void markAsTransactional(boolean transactional, Configuration c) {
      c.fluent().transaction().transactionMode(transactional ? TransactionMode.TRANSACTIONAL : TransactionMode.NON_TRANSACTIONAL);
   }

   private static void markAsTransactional(boolean transactional, ConfigurationBuilder builder) {
      builder.transaction().transactionMode(transactional ? TransactionMode.TRANSACTIONAL : TransactionMode.NON_TRANSACTIONAL);
   }

   private static void updateTransactionSupport(Configuration c) {
      if (c.isTransactionalCache()) amendJTA(c);
   }

   private static void updateTransactionSupport(boolean transactional, ConfigurationBuilder builder) {
      if (transactional) amendJTA(builder);
   }

   private static void amendJTA(Configuration c) {
      if (c.isTransactionalCache() && c.getTransactionManagerLookupClass() == null && c.getTransactionManagerLookup() == null) {
         c.setTransactionManagerLookupClass(TransactionSetup.getManagerLookup());
      }
   }

   private static void amendJTA(ConfigurationBuilder builder) {
      org.infinispan.configuration.cache.Configuration c = builder.build();
      if (c.transaction().transactionMode().equals(TransactionMode.TRANSACTIONAL) && c.transaction().transactionManagerLookup() == null) {
         builder.transaction().transactionManagerLookup((TransactionManagerLookup) Util.getInstance(TransactionSetup.getManagerLookup(), TestCacheManagerFactory.class.getClassLoader()));
      }
   }

   /**
    * Creates an cache manager that does support clustering.
    */
   public static EmbeddedCacheManager createClusteredCacheManager() {
      return createClusteredCacheManager(new TransportFlags());
   }

   public static EmbeddedCacheManager createClusteredCacheManager(TransportFlags flags) {
      return createClusteredCacheManager(new Configuration(), flags);
   }


   public static EmbeddedCacheManager createClusteredCacheManager(Configuration defaultCacheConfig) {
      return createClusteredCacheManager(defaultCacheConfig, new TransportFlags());
   }

   public static EmbeddedCacheManager createClusteredCacheManager(Configuration defaultCacheConfig, TransportFlags flags) {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getClusteredDefault();
      amendMarshaller(globalConfiguration);
      minimizeThreads(globalConfiguration);
      amendTransport(globalConfiguration, flags);
      amendJTA(defaultCacheConfig);
      return newDefaultCacheManager(true, globalConfiguration, defaultCacheConfig, false);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(ConfigurationBuilder defaultCacheConfig, TransportFlags flags) {
      return createClusteredCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder(), defaultCacheConfig, flags);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(GlobalConfigurationBuilder gcb, ConfigurationBuilder defaultCacheConfig) {
      return createClusteredCacheManager(gcb, defaultCacheConfig, new TransportFlags());
   }

   public static EmbeddedCacheManager createClusteredCacheManager(ConfigurationBuilder defaultCacheConfig) {
      return createClusteredCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder(), defaultCacheConfig);
   }

   public static EmbeddedCacheManager createClusteredCacheManager(GlobalConfigurationBuilder gcb, ConfigurationBuilder defaultCacheConfig, TransportFlags flags) {
      amendMarshaller(gcb);
      minimizeThreads(gcb);
      amendTransport(gcb, flags);
      amendJTA(defaultCacheConfig);
      return newDefaultCacheManager(true, gcb, defaultCacheConfig, false);
   }
   
   public static EmbeddedCacheManager createCacheManager(ConfigurationBuilder builder) {      
      return createCacheManager(new GlobalConfigurationBuilder().nonClusteredDefault(), builder);
   }

   public static EmbeddedCacheManager createCacheManager(GlobalConfigurationBuilder globalBuilder, ConfigurationBuilder builder) {
      return newDefaultCacheManager(true, globalBuilder, builder, false);
   }

   /**
    * Creates a cache manager and amends the supplied configuration in order to avoid conflicts (e.g. jmx, jgroups)
    * during running tests in parallel.
    */
   public static EmbeddedCacheManager createCacheManager(GlobalConfiguration configuration) {
      return internalCreateJmxDomain(true, configuration, false);
   }

   public static EmbeddedCacheManager createCacheManager(boolean start, GlobalConfiguration configuration) {
      return internalCreateJmxDomain(start, configuration, false);
   }

   /**
    * Creates a cache manager that won't try to modify the configured jmx domain name: {@link
    * org.infinispan.config.GlobalConfiguration#getJmxDomain()}}. This method must be used with care, and one should
    * make sure that no domain name collision happens when the parallel suite executes. An approach to ensure this, is
    * to set the domain name to the name of the test class that instantiates the CacheManager.
    */
   public static EmbeddedCacheManager createCacheManagerEnforceJmxDomain(GlobalConfiguration configuration) {
      return internalCreateJmxDomain(true, configuration, true);
   }

   private static EmbeddedCacheManager internalCreateJmxDomain(boolean start, GlobalConfiguration configuration, boolean enforceJmxDomain) {
      amendMarshaller(configuration);
      minimizeThreads(configuration);
      amendTransport(configuration);
      return newDefaultCacheManager(start, configuration, new Configuration(), enforceJmxDomain);
   }

   public static EmbeddedCacheManager createCacheManager(Configuration.CacheMode mode, boolean indexing) {
      GlobalConfiguration gc = mode.isClustered() ? GlobalConfiguration.getClusteredDefault() : GlobalConfiguration.getNonClusteredDefault();
      Configuration c = new Configuration();
      FluentConfiguration fluentConfiguration = c.fluent();
      if (indexing) {
         //The property is not really needed as it defaults to the same value,
         //but since it's recommended we set it explicitly to avoid logging a noisy warning.
         fluentConfiguration.indexing().addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT");
      }
      fluentConfiguration.mode(mode);
      return createCacheManager(gc, fluentConfiguration.build());
   }

   public static EmbeddedCacheManager createCacheManager(Configuration defaultCacheConfig) {
      GlobalConfiguration globalConfiguration;
      if (defaultCacheConfig.getCacheMode().isClustered()) {
         globalConfiguration = GlobalConfiguration.getClusteredDefault();
         amendTransport(globalConfiguration);
      }
      else {
         globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      }
      amendMarshaller(globalConfiguration);
      minimizeThreads(globalConfiguration);
      updateTransactionSupport(defaultCacheConfig);

      // we stop caches during transactions all the time
      // so wait at most 1 second for ongoing transactions when stopping
      defaultCacheConfig.fluent().cacheStopTimeout(1000);

      return newDefaultCacheManager(true, globalConfiguration, defaultCacheConfig, false);
   }

   public static EmbeddedCacheManager createCacheManager(GlobalConfiguration configuration, Configuration defaultCfg) {
      minimizeThreads(configuration);
      amendMarshaller(configuration);
      amendTransport(configuration);
      updateTransactionSupport(defaultCfg);
      return newDefaultCacheManager(true, configuration, defaultCfg, false);
   }

   public static EmbeddedCacheManager createCacheManager(GlobalConfiguration configuration, Configuration defaultCfg, boolean keepJmxDomainName) {
      return createCacheManager(configuration, defaultCfg, keepJmxDomainName, false);
   }

   public static EmbeddedCacheManager createCacheManager(GlobalConfiguration configuration, Configuration defaultCfg, boolean keepJmxDomainName, boolean dontFixTransport) {
      minimizeThreads(configuration);
      amendMarshaller(configuration);
      if (!dontFixTransport) amendTransport(configuration);
      updateTransactionSupport(defaultCfg);
      return newDefaultCacheManager(true, configuration, defaultCfg, keepJmxDomainName);
   }

   /**
    * @see #createCacheManagerEnforceJmxDomain(String)
    */
   public static EmbeddedCacheManager createCacheManagerEnforceJmxDomain(String jmxDomain) {
      return createCacheManagerEnforceJmxDomain(jmxDomain, true, true);
   }

   /**
    * @see #createCacheManagerEnforceJmxDomain(String)
    */
   public static EmbeddedCacheManager createCacheManagerEnforceJmxDomain(String jmxDomain, boolean exposeGlobalJmx, boolean exposeCacheJmx) {
      return createCacheManagerEnforceJmxDomain(jmxDomain, null, exposeGlobalJmx, exposeCacheJmx);
   }

   /**
    * @see #createCacheManagerEnforceJmxDomain(String)
    */
   public static EmbeddedCacheManager createCacheManagerEnforceJmxDomain(String jmxDomain, String cacheManagerName, boolean exposeGlobalJmx, boolean exposeCacheJmx) {
      GlobalConfiguration globalConfiguration = GlobalConfiguration.getNonClusteredDefault();
      globalConfiguration.setJmxDomain(jmxDomain);
      if (cacheManagerName != null)
         globalConfiguration.setCacheManagerName(cacheManagerName);
      globalConfiguration.setMBeanServerLookup(PerThreadMBeanServerLookup.class.getName());
      globalConfiguration.setExposeGlobalJmxStatistics(exposeGlobalJmx);
      Configuration configuration = new Configuration();
      configuration.setExposeJmxStatistics(exposeCacheJmx);
      return createCacheManager(globalConfiguration, configuration, true);
   }

   public static Configuration getDefaultConfiguration(boolean transactional) {
      Configuration c = new Configuration();
      markAsTransactional(transactional, c);
      updateTransactionSupport(c);
      return c;
   }

   public static ConfigurationBuilder getDefaultCacheConfiguration(boolean transactional) {
      return getDefaultCacheConfiguration(transactional, false);
   }

   public static ConfigurationBuilder getDefaultCacheConfiguration(boolean transactional, boolean useCustomTxLookup) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      markAsTransactional(transactional, builder);
      //don't changed the tx lookup.
      if (useCustomTxLookup) updateTransactionSupport(transactional, builder);
      return builder;
   }


   public static Configuration getDefaultConfiguration(boolean transactional, Configuration.CacheMode cacheMode) {
      Configuration c = new Configuration();
      markAsTransactional(transactional, c);
      updateTransactionSupport(c);
      c.setCacheMode(cacheMode);
      if (cacheMode.isClustered()) {
         c.setSyncRollbackPhase(true);
         c.setSyncCommitPhase(true);
      }
      return c;
   }

   public static void amendTransport(GlobalConfiguration cfg) {
      amendTransport(cfg, new TransportFlags());
   }

   private static void amendTransport(GlobalConfiguration configuration, TransportFlags flags) {
      if (configuration.getTransportClass() != null) { //this is local
         Properties newTransportProps = new Properties();
         Properties previousSettings = configuration.getTransportProperties();
         if (previousSettings != null) {
            newTransportProps.putAll(previousSettings);
         }

         String fullTestName = perThreadCacheManagers.get().fullTestName;
         String nextCacheName = perThreadCacheManagers.get().getNextCacheName();
         if (fullTestName == null) {
            // Either we're running from within the IDE or it's a
            // @Test(timeOut=nnn) test. We rely here on some specific TestNG
            // thread naming convention which can break, but TestNG offers no
            // other alternative. It does not offer any callbacks within the
            // thread that runs the test that can timeout.
            String threadName = Thread.currentThread().getName();
            String pattern = "TestNGInvoker-";
            if (threadName.startsWith(pattern)) {
               // This is a timeout test, so for the moment rely on the test
               // method name that comes in the thread name.
               fullTestName = threadName;
               nextCacheName = threadName.substring(
                     threadName.indexOf("-") + 1, threadName.indexOf('('));
            } // else, test is being run from IDE
         }

         newTransportProps.put(JGroupsTransport.CONFIGURATION_STRING,
            getJGroupsConfig(fullTestName, flags));

         configuration.setTransportProperties(newTransportProps);
         configuration.setTransportNodeName(nextCacheName);
      }
   }

   private static void amendTransport(GlobalConfigurationBuilder builder, TransportFlags flags) {
      org.infinispan.configuration.global.GlobalConfiguration gc = builder.build();
      if (gc.transport().transport() != null) { //this is local
         String fullTestName = perThreadCacheManagers.get().fullTestName;
         String nextCacheName = perThreadCacheManagers.get().getNextCacheName();
         if (fullTestName == null) {
            // Either we're running from within the IDE or it's a
            // @Test(timeOut=nnn) test. We rely here on some specific TestNG
            // thread naming convention which can break, but TestNG offers no
            // other alternative. It does not offer any callbacks within the
            // thread that runs the test that can timeout.
            String threadName = Thread.currentThread().getName();
            String pattern = "TestNGInvoker-";
            if (threadName.startsWith(pattern)) {
               // This is a timeout test, so for the moment rely on the test
               // method name that comes in the thread name.
               fullTestName = threadName;
               nextCacheName = threadName.substring(
                     threadName.indexOf("-") + 1, threadName.indexOf('('));
            } // else, test is being run from IDE
         }

         builder
               .transport()
                  .addProperty(JGroupsTransport.CONFIGURATION_STRING, getJGroupsConfig(fullTestName, flags))
                  .nodeName(nextCacheName);
      }
   }


   public static void minimizeThreads(GlobalConfiguration gc) {
      Properties p = new Properties();
      p.setProperty("maxThreads", "4");
      gc.setAsyncTransportExecutorProperties(p);
   }

   public static void minimizeThreads(GlobalConfigurationBuilder builder) {
      builder.asyncTransportExecutor().addProperty("maxThreads", "4");
   }

   public static void amendMarshaller(GlobalConfiguration configuration) {
      if (MARSHALLER != null) {
         try {
            Util.loadClassStrict(MARSHALLER, Thread.currentThread().getContextClassLoader());
            configuration.setMarshallerClass(MARSHALLER);
         } catch (ClassNotFoundException e) {
            // No-op, stick to GlobalConfiguration default.
         }
      }
   }

   @SuppressWarnings("unchecked")
   public static void amendMarshaller(GlobalConfigurationBuilder builder) {
      if (MARSHALLER != null) {
         try {
            Marshaller marshaller = Util.getInstanceStrict(MARSHALLER, Thread.currentThread().getContextClassLoader());
            builder.serialization().marshaller(marshaller);
         } catch (ClassNotFoundException e) {
            // No-op, stick to GlobalConfiguration default.
         } catch (InstantiationException e) {
            // No-op, stick to GlobalConfiguration default.
         } catch (IllegalAccessException e) {
            // No-op, stick to GlobalConfiguration default.
         }
      }
   }

   private static DefaultCacheManager newDefaultCacheManager(boolean start, GlobalConfiguration gc, Configuration c) {
      DefaultCacheManager defaultCacheManager = new DefaultCacheManager(gc, c, start);
      PerThreadCacheManagers threadCacheManagers = perThreadCacheManagers.get();
      String methodName = extractMethodName();
      log.trace("Adding DCM (" + defaultCacheManager.getAddress() + ") for method: '" + methodName + "'");
      threadCacheManagers.add(methodName, defaultCacheManager);
      return defaultCacheManager;
   }

   private static DefaultCacheManager newDefaultCacheManager(boolean start, GlobalConfigurationBuilder gc, ConfigurationBuilder c) {
      DefaultCacheManager defaultCacheManager = new DefaultCacheManager(gc.build(), c.build(), start);
      PerThreadCacheManagers threadCacheManagers = perThreadCacheManagers.get();
      String methodName = extractMethodName();
      log.trace("Adding DCM (" + defaultCacheManager.getAddress() + ") for method: '" + methodName + "'");
      threadCacheManagers.add(methodName, defaultCacheManager);
      return defaultCacheManager;
   }


   private static String extractMethodName() {
      StackTraceElement[] stack = Thread.currentThread().getStackTrace();
      if (stack.length == 0) return null;
      for (int i = stack.length - 1; i > 0; i--) {
         StackTraceElement e = stack[i];
         String className = e.getClassName();
         if ((className.indexOf("org.infinispan") != -1) && className.indexOf("org.infinispan.test") < 0)
            return e.toString();
      }
      return null;
   }

   static void testStarted(String testName, String fullName) {
      perThreadCacheManagers.get().setTestName(testName, fullName);
   }

   static void testFinished(String testName) {
      perThreadCacheManagers.get().checkManagersClosed(testName);
      perThreadCacheManagers.get().unsetTestName();
   }

   private static class PerThreadCacheManagers {
      String testName = null;
      private String oldThreadName;
      HashMap<EmbeddedCacheManager, String> cacheManagers = new HashMap<EmbeddedCacheManager, String>();
      String fullTestName;

      public void checkManagersClosed(String testName) {
         for (Map.Entry<EmbeddedCacheManager, String> cmEntry : cacheManagers.entrySet()) {
            if (cmEntry.getKey().getStatus().allowInvocations()) {
               String thName = Thread.currentThread().getName();
               String errorMessage = '\n' +
                     "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n" +
                     "!!!!!! (" + thName + ") Exiting because " + testName + " has NOT shut down all the cache managers it has started !!!!!!!\n" +
                     "!!!!!! (" + thName + ") The still-running cacheManager was created here: " + cmEntry.getValue() + " !!!!!!!\n" +
                     "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";
               log.error(errorMessage);
               System.err.println(errorMessage);
               System.exit(9);
            }
         }
         cacheManagers.clear();
      }

      public String getNextCacheName() {
         int index = cacheManagers.size();
         char name = (char) ((int) 'A' + index);
         return (testName != null ? testName + "-" : "") + "Node" + name;
      }

      public void add(String methodName, DefaultCacheManager cm) {
         cacheManagers.put(cm, methodName);
      }

      public void setTestName(String testName, String fullTestName) {
         this.testName = testName;
         this.fullTestName = fullTestName;
         this.oldThreadName = Thread.currentThread().getName();
         Thread.currentThread().setName("testng-" + testName);
      }

      public void unsetTestName() {
         this.testName = null;
         Thread.currentThread().setName(oldThreadName);
         this.oldThreadName = null;
      }
   }
}
