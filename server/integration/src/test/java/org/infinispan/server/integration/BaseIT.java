package org.infinispan.server.integration;

import org.infinispan.server.integration.enricher.ArquillianSupport;
import org.infinispan.server.integration.enricher.AutodiscoverRemoteExtension;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.jboss.arquillian.container.test.spi.RemoteLoadableExtension;
import org.jboss.shrinkwrap.api.spec.WebArchive;

/**
 * @see InstrumentedArquillianTestClass
 */
public abstract class BaseIT {

   /**
    * @Deployment will be add dynamic by InstrumentedArquillianTestClass
    * @return
    */
   public static WebArchive createDeployment() {
      try {
         WebArchive war = DeploymentBuilder.war();
         war.addClass(InstrumentedArquillianTestClass.class);
         war.addClass(BaseIT.class);
         war.addAsServiceProvider(RemoteLoadableExtension.class, AutodiscoverRemoteExtension.class);
         war.addPackage(InfinispanServerTestMethodRule.class.getPackage().getName());
         war.addPackage(AutodiscoverRemoteExtension.class.getPackage().getName());
         war.addClass(ArquillianSupport.class);
         war.addClass(InfinispanServerIntegrationUtil.class);
         war.addClass(ServerRunMode.class);
         war.addClass(InfinispanResourceTest.class);
         war.addClass(InfinispanTest.class);
         war.addClass(InfinispanArchive.class);
         war.addPackage("org.infinispan.lifecycle");
         war.addPackage("org.infinispan.configuration.cache");
         war.addPackage("org.infinispan.util.logging");
         war.addPackage("org.infinispan.remoting.transport.jgroups");

         ExportHelper.export("web-app.war", war);

         return war;
      } catch (Exception e) {
         throw new IllegalStateException(e);
      }
   }
}
