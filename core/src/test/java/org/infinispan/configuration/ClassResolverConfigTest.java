package org.infinispan.configuration;

import org.infinispan.api.WithClassLoaderTest;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.commons.marshall.jboss.DefaultContextClassResolver;
import org.testng.annotations.Test;

/**
 * A test that verifies that a class resolver can be configured successfully.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "configuration.ClassResolverConfigTest")
public class ClassResolverConfigTest extends WithClassLoaderTest {

   @Override
   protected GlobalConfigurationBuilder createSecondGlobalCfgBuilder(ClassLoader cl) {
      GlobalConfigurationBuilder gcBuilder = super.createSecondGlobalCfgBuilder(cl);
      ClassLoader systemClassLoader = ClassResolverConfigTest.class.getClassLoader();
      gcBuilder.serialization().classResolver(new DefaultContextClassResolver(systemClassLoader));
      return gcBuilder;
   }

   @Override
   @Test(expectedExceptions = AssertionError.class,
      expectedExceptionsMessageRegExp = "Expected a ClassNotFoundException")
   public void testReadingWithCorrectClassLoaderAfterReplication() {
      // With the default context class resolver, if configured correctly,
      // the classloader that we set with the invocation context (i.e.
      // coming from global configuration) is ignored (the super class test
      // has one specific classloader that forces not finding a class), and
      // so the class is found.
      super.testReadingWithCorrectClassLoaderAfterReplication();
   }

   @Override
   @Test(expectedExceptions = AssertionError.class,
         expectedExceptionsMessageRegExp = "Expected a ClassNotFoundException")
   public void testReadingWithCorrectClassLoaderAfterReplicationWithDelegateCache() {
      // Same reason as method above...
      super.testReadingWithCorrectClassLoaderAfterReplicationWithDelegateCache();
   }

}
