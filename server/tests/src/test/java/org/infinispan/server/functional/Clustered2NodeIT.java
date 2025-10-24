package org.infinispan.server.functional;

import org.infinispan.server.functional.hotrod.HotRodCache2NodeQueries;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite(failIfNoTests = false)
@SelectClasses({
      HotRodCache2NodeQueries.class,
})
public class Clustered2NodeIT extends InfinispanSuite {

   public static int SERVER_COUNT = 2;
   public static int MAX_BOOLEAN_CLAUSES = 1025;

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(SERVER_COUNT)
               .runMode(ServerRunMode.CONTAINER)
               .property("infinispan.query.lucene.max-boolean-clauses", Integer.toString(MAX_BOOLEAN_CLAUSES))
               .build();
}
