package org.infinispan.server.extensions;

import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
import org.infinispan.server.test.ServerRunMode;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({
      ScriptingTasks.class,
      ServerTasks.class,
})
public class ExtensionsIT {

   @ClassRule
   public static final InfinispanServerRule SERVERS = new InfinispanServerRule(
         new InfinispanServerRuleConfigurationBuilder("configuration/ClusteredServerTest.xml")
               .serverRunMode(ServerRunMode.CONTAINER)
               .numServers(2)
               .archive(HelloServerTask.artifact())
   );
}
