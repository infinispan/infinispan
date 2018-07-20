package org.infinispan.test.integration.security.utils;

import java.io.File;

import org.infinispan.test.integration.security.embedded.AbstractAuthentication;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;

/**
 * @author <a href="mailto:vjuranek@redhat.com">Vojtech Juranek</a>
 * @since 7.0
 */
public final class Deployments {

   public static WebArchive createLdapTestDeployment() {
      WebArchive war = createBaseTestDeployment();
      return war;
   }

   public static WebArchive createKrbLdapTestDeployment() {
      WebArchive war = createBaseTestDeployment()
            .addAsWebInfResource(new File("target/test-classes/jboss-deployment-structure.xml"));
      return war;
   }

   public static WebArchive createNodeAuthTestDeployment(String jgroupsConfig) {
      WebArchive war = createBaseTestDeployment()
            .addAsLibraries(new File("target/test-libs/jgroups.jar"))
            .addAsResource(new File("target/test-classes/" + jgroupsConfig));
      return war;
   }

   public static WebArchive createNodeAuthKrbTestDeployment(String jgroupsConfig) {
      WebArchive war = createNodeAuthTestDeployment(jgroupsConfig)
            .addAsWebInfResource(new File("target/test-classes/jboss-deployment-structure.xml"));
      return war;
   }

   public static WebArchive createBaseTestDeployment() {
      WebArchive war = ShrinkWrap
            .create(WebArchive.class)
            .addAsLibraries(
                  new File("target/test-libs/infinispan-core.jar"),
                  new File("target/test-libs/infinispan-commons.jar"),
                  new File("target/test-libs/caffeine.jar"),
                  new File("target/test-libs/rxjava.jar"),
                  new File("target/test-libs/reactive-streams.jar"),
                  new File("target/test-libs/jboss-marshalling.jar"),
                  new File("target/test-libs/jboss-marshalling-river.jar"),
                  new File("target/test-libs/protostream.jar"),
                  new File("target/test-libs/wildfly-controller-client.jar"))
            .addPackage(Deployments.class.getPackage())
            .addPackage(AbstractAuthentication.class.getPackage());
      return war;
   }

}
