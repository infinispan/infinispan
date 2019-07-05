package org.infinispan.server.functional;

import org.infinispan.server.test.InfinispanServerTestConfiguration;
import org.infinispan.server.test.InfinispanServerRule;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({HotRodOperations.class, RestOperations.class, MemcachedOperations.class, CounterOperations.class})
public class ClusteredTests {

   @ClassRule
   public static final InfinispanServerRule SERVERS = new InfinispanServerRule(new InfinispanServerTestConfiguration("configuration/ClusteredServerTest.xml").numServers(2));
}
