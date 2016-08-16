package org.infinispan.test.integration.as.wildfly;

import static org.infinispan.test.integration.as.VersionTestHelper.addHibernateSearchManifestDependencies;

import org.infinispan.test.integration.as.wildfly.controller.MemberRegistration;
import org.infinispan.test.integration.as.wildfly.model.Member;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.container.test.api.TargetsContainer;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.Asset;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.asset.StringAsset;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.jboss.shrinkwrap.descriptor.api.Descriptors;
import org.jboss.shrinkwrap.descriptor.api.persistence20.PersistenceDescriptor;
import org.junit.runner.RunWith;

/**
 * Test the Hibernate Search module combined with an Infinispan Directory usage.
 *
 * @author Sanne Grinovero
 */
@RunWith(Arquillian.class)
public class InfinispanModuleMemberRegistrationIT extends MemberRegistrationBase {

   @Deployment(name = "dep.active-1")
   @TargetsContainer("container.active-1")
   public static Archive<?> createTestDeploymentOne() {
      return createTestArchive();
   }

   @Deployment(name = "dep.active-2")
   @TargetsContainer("container.active-2")
   public static Archive<?> createTestDeploymentTwo() {
      return createTestArchive();
   }

   private static Archive<?> createTestArchive() {
      WebArchive webArchive = ShrinkWrap
              .create(WebArchive.class, InfinispanModuleMemberRegistrationIT.class.getSimpleName() + ".war")
              .addClasses(Member.class, MemberRegistration.class, MemberRegistrationBase.class)
              .addAsResource(persistenceXml(), "META-INF/persistence.xml")
              //This test is simply reusing the default configuration file, but we copy
              //this configuration into the Archive to verify that resources can be loaded from it:
              .addAsResource("user-provided-infinispan.xml", "user-provided-infinispan.xml")
              .addAsWebInfResource(EmptyAsset.INSTANCE, "beans.xml");
      addHibernateSearchManifestDependencies(webArchive);
      return webArchive;
   }

   private static Asset persistenceXml() {
      String persistenceXml = Descriptors.create(PersistenceDescriptor.class)
              .version("2.0")
              .createPersistenceUnit()
              .name("primary")
              .jtaDataSource("java:jboss/datasources/ExampleDS")
              .getOrCreateProperties()
              .createProperty()
              .name("hibernate.hbm2ddl.auto")
              .value("create-drop")
              .up()
              .createProperty()
              .name("wildfly.jpa.hibernate.search.module")
              .value("none")
              .up()
              .createProperty()
              .name("hibernate.search.default.lucene_version")
              .value("LUCENE_CURRENT")
              .up()
              .createProperty()
              .name("hibernate.search.default.directory_provider")
              .value("infinispan")
              .up()
              .createProperty()
              .name("hibernate.search.default.exclusive_index_use")
              .value("false")
              .up()
              .createProperty()
              .name("hibernate.search.infinispan.configuration_resourcename")
              .value("user-provided-infinispan.xml")
              .up()
              .up()
              .up()
              .exportAsString();
      return new StringAsset(persistenceXml);
   }

}
