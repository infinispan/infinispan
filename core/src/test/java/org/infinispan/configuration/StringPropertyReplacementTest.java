package org.infinispan.configuration;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Properties;

import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.remoting.transport.jgroups.FileJGroupsChannelConfigurator;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * /**
 * Tests that string property replacement works properly when parsing
 * a config file.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test (groups = "functional", testName = "config.StringPropertyReplacementTest")
public class StringPropertyReplacementTest extends AbstractInfinispanTest {

   protected ConfigurationBuilderHolder parse() throws Exception {
      Properties properties = new Properties();
      properties.setProperty("StringPropertyReplacementTest.asyncListenerMaxThreads","2");
      properties.setProperty("StringPropertyReplacementTest.persistenceMaxThreads","4");
      properties.setProperty("StringPropertyReplacementTest.IsolationLevel","READ_COMMITTED");
      properties.setProperty("StringPropertyReplacementTest.writeSkewCheck","true");
      properties.setProperty("StringPropertyReplacementTest.SyncCommitPhase","true");
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true, properties);
      return parserRegistry.parseFile("configs/string-property-replaced.xml");
   }

   public void testGlobalConfig() throws Exception {
      ConfigurationBuilderHolder holder = parse();
      GlobalConfiguration gc = holder.getGlobalConfigurationBuilder().build();
      BlockingThreadPoolExecutorFactory listenerThreadPool =
            gc.listenerThreadPool().threadPoolFactory();
      assertEquals(2, listenerThreadPool.maxThreads());

      BlockingThreadPoolExecutorFactory persistenceThreadPool =
            gc.persistenceThreadPool().threadPoolFactory();
      assertEquals(4, persistenceThreadPool.maxThreads());

      FileJGroupsChannelConfigurator transportConfigurator = (FileJGroupsChannelConfigurator) gc.transport().properties().get(JGroupsTransport.CHANNEL_CONFIGURATOR);
      assertEquals("stacks/tcp.xml", transportConfigurator.getPath());

      Configuration configuration = holder.getDefaultConfigurationBuilder().build();
      assertEquals(IsolationLevel.READ_COMMITTED, configuration.locking().isolationLevel());
   }
}
