package org.infinispan.test.integration.security.utils;

import java.io.File;

import org.infinispan.test.integration.security.embedded.AbstractAuthentication;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;

/**
 * @author vjuranek
 * @since 7.0
 */
public final class Deployments {

   public static WebArchive createLdapTestDeployment() {
      WebArchive war = ShrinkWrap
            .create(WebArchive.class)
            .addAsLibraries(new File("target/test-libs/infinispan-core.jar"),
                  new File("target/test-libs/infinispan-commons.jar"),
                  new File("target/test-libs/jboss-marshalling.jar"),
                  new File("target/test-libs/jboss-marshalling-river.jar"))
            .addPackage(Deployments.class.getPackage())
            .addPackage(AbstractAuthentication.class.getPackage());
      return war;
   }
   
   public static WebArchive createKrbLdapTestDeployment() {
      WebArchive war = ShrinkWrap
            .create(WebArchive.class)
            .addAsLibraries(
                  new File("target/test-libs/infinispan-core.jar"),
                  new File("target/test-libs/infinispan-commons.jar"),
                  new File("target/test-libs/jboss-marshalling.jar"),
                  new File("target/test-libs/jboss-marshalling-river.jar")
            )
            .addAsWebInfResource(new File("target/test-classes/jboss-deployment-structure.xml"))
            .addPackage(Deployments.class.getPackage())
            .addPackage(AbstractAuthentication.class.getPackage());
      return war;
   }
   
}
