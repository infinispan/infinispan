package org.infinispan.configuration;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Properties;

import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
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
      System.setProperty("StringPropertyReplacementTest.asyncListenerMaxThreads","2");
      System.setProperty("StringPropertyReplacementTest.persistenceMaxThreads","4");
      System.setProperty("StringPropertyReplacementTest.IsolationLevel","READ_COMMITTED");
      System.setProperty("StringPropertyReplacementTest.writeSkewCheck","true");
      System.setProperty("StringPropertyReplacementTest.SyncCommitPhase","true");
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader(), true);
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

      Properties transportProps = gc.transport().properties();
      assertEquals("jgroups-tcp.xml", transportProps.get("configurationFile"));

      Configuration configuration = holder.getDefaultConfigurationBuilder().build();
      assertEquals(IsolationLevel.READ_COMMITTED, configuration.locking().isolationLevel());
   }
}
