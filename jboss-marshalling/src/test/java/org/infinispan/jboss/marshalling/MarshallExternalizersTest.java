package org.infinispan.jboss.marshalling;

import java.lang.reflect.Method;

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jboss.marshalling.core.JBossUserMarshaller;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "marshall.MarshallExternalizersTest")
public class MarshallExternalizersTest extends org.infinispan.marshall.MarshallExternalizersTest {

   @Override
   protected GlobalConfigurationBuilder globalConfigurationBuilder() {
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.serialization().marshaller(new JBossUserMarshaller());
      return globalBuilder;
   }

   public void testReplicateJBossExternalizePojo(Method m) {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(34, TestingUtil.k(m));
      doReplicatePojo(m, pojo);
   }

   @Test(dependsOnMethods = "testReplicateJBossExternalizePojo")
   public void testReplicateJBossExternalizePojoToNewJoiningNode(Method m) {
      PojoWithJBossExternalize pojo = new PojoWithJBossExternalize(48, TestingUtil.k(m));
      doReplicatePojoToNewJoiningNode(m, pojo);
   }
}
