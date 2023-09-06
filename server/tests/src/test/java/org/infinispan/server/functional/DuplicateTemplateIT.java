package org.infinispan.server.functional;

import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.junit.ClassRule;
import org.junit.Test;

public class DuplicateTemplateIT {

   @ClassRule
   public static final InfinispanServerRule SERVER =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .dataFiles("data/templates.xml")
               .build();

   @Test
   public void testDuplicateInternalTemplatesAllowed() {
      // no-op. Container startup fails if duplicate internal templates are not ignored
   }
}
