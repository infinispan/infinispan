package org.infinispan.test.integration.security.utils;

import java.io.File;

import org.infinispan.test.integration.security.embedded.AbstractLdapAuthentication;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;

/**
 * @author vjuranek
 * @since 7.0
 */
public final class Deployments {

   public static WebArchive createDeployment() {
      WebArchive war = ShrinkWrap
            .create(WebArchive.class)
            .addAsLibraries(new File("target/test-libs/infinispan-core.jar"),
                  new File("target/test-libs/infinispan-commons.jar"),
                  new File("target/test-libs/jboss-marshalling.jar"),
                  new File("target/test-libs/jboss-marshalling-river.jar"))
            .addPackage(Deployments.class.getPackage())
            .addPackage(AbstractLdapAuthentication.class.getPackage());
      return war;
   }
}
