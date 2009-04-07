package org.infinispan.factories.scopes;

import org.testng.annotations.Test;

/**
 * @author Manik Surtani
 * @since 4.0
 */
@Test(groups = "unit", testName = "factories.scopes.ScopeDetectorTest")
public class ScopeDetectorTest {
   public void testScopeOnClass() {
      testScopes(Test1.class, Scopes.GLOBAL);

   }

   public void testScopeOnInterface() {
      testScopes(Test2.class, Scopes.GLOBAL);
   }

   public void testScopeOnSuperClass() {
      testScopes(Test3.class, Scopes.GLOBAL);
   }

   public void testScopeOnSuperInterface() {
      testScopes(Test4.class, Scopes.GLOBAL);
   }

   public void testNoScopes() {
      testScopes(Test6.class, Scopes.NAMED_CACHE);
   }

   private void testScopes(Class clazz, Scopes expected) {
      Scopes detected = ScopeDetector.detectScope(clazz);
      assert detected == expected : "Expected " + expected + " but was " + detected;
   }

   public static interface Unscoped {

   }

   @Scope(Scopes.GLOBAL)
   public static interface Scoped {

   }

   @Scope(Scopes.GLOBAL)
   public static class SuperScoped {

   }

   public static class SuperUnScoped {

   }

   @Scope(Scopes.GLOBAL)
   public static class Test1 {

   }

   public static class Test2 implements Scoped {

   }

   public static class Test3 extends SuperScoped {

   }

   public static class Test4 extends Test2 {

   }

   public static class Test6 extends SuperUnScoped implements Unscoped {

   }
}
