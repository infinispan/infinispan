package org.infinispan.persistence.factory;

import org.infinispan.persistence.factory.configuration.MyCustomStore;
import org.infinispan.persistence.factory.configuration.MyCustomStoreConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "unit", testName = "persistence.DeployedCacheStoreFactoryTest")
public class DeployedCacheStoreFactoryTest {

   @Test
   public void testAddingInstance() throws Exception {
      //given
      MyCustomStore testedObject = new MyCustomStore();
      DeployedCacheStoreFactory factory = new DeployedCacheStoreFactory();

      //when
      factory.addInstance(MyCustomStore.class.getName(), testedObject);
      Object instance = factory.createInstance(new MyCustomStoreConfiguration());

      //then
      assertEquals(testedObject, instance);
   }

   @Test
   public void testAddingAndRemovingInstance() throws Exception {
      //given
      MyCustomStore testedObject = new MyCustomStore();
      DeployedCacheStoreFactory factory = new DeployedCacheStoreFactory();

      //when
      factory.addInstance(MyCustomStore.class.getName(), testedObject);
      factory.removeInstance(MyCustomStore.class.getName());
      Object instance = factory.createInstance(new MyCustomStoreConfiguration());

      //then
      assertNull(instance);
   }
}