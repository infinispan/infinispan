package org.infinispan.test.integration.as.cdi;

import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jboss.shrinkwrap.api.GenericArchive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.resolver.api.DependencyResolvers;
import org.jboss.shrinkwrap.resolver.api.maven.MavenDependencyResolver;

import java.io.File;

/**
 * @author Kevin Pollet <pollet.kevin@gmail.com> (C) 2011
 */
public final class Deployments {

   // Disable instantiation
   private Deployments() {
   }

   public static WebArchive baseDeployment() {
      return ShrinkWrap.create(WebArchive.class)
            .addPackage(Config.class.getPackage())
            .addClass(TestCacheManagerFactory.class)
            .addAsWebInfResource(new File("src/test/webapp/WEB-INF/beans.xml"), "beans.xml")
            .addAsLibraries(
                  DependencyResolvers.use(MavenDependencyResolver.class)
                        .loadReposFromPom("pom.xml")
                        .artifact("javax.cache:cache-api")
                        .artifact("org.infinispan:infinispan-cdi")
                        .artifact("org.infinispan:infinispan-jcache")
                        .resolveAs(GenericArchive.class)
            );
   }
}
