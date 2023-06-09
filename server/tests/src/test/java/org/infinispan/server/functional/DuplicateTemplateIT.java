package org.infinispan.server.functional;

import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class DuplicateTemplateIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .dataFiles("data/templates.xml")
               .build();

   @Test
   public void testDuplicateInternalTemplatesAllowed() {
      // no-op. Container startup fails if duplicate internal templates are not ignored
   }
}
