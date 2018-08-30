package org.infinispan.test.fwk;

import java.lang.reflect.Method;

import org.testng.ITestClass;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.TestNGException;
import org.testng.internal.MethodInstance;
import org.testng.internal.TestNGMethod;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlTest;

public class FakeTestClass implements ITestClass {
   private static final long serialVersionUID = -4871120395482207788L;

   private final Object instance;
   private final ITestNGMethod method;
   private final XmlTest xmlTest;

   public static MethodInstance newFailureMethodInstance(Exception e, XmlTest xmlTest, ITestContext context) {
      Method failMethod = null;
      try {
         failMethod = TestFrameworkFailure.class.getMethod("fail");
      } catch (NoSuchMethodException e1) {
         e1.addSuppressed(e);
         e1.printStackTrace(System.err);
         throw new TestNGException(e1);
      }
      TestFrameworkFailure testInstance = new TestFrameworkFailure(e);
      TestNGMethod testNGMethod = new TestNGMethod(failMethod, context.getSuite().getAnnotationFinder(),
                                                   xmlTest, testInstance);
      ITestClass testClass = new FakeTestClass(testNGMethod, testInstance, xmlTest);
      testNGMethod.setTestClass(testClass);
      return new MethodInstance(testNGMethod);
   }

   FakeTestClass(ITestNGMethod method, Object instance, XmlTest xmlTest) {
      this.method = method;
      this.instance = instance;
      this.xmlTest = xmlTest;
   }

   @Override
   public Object[] getInstances(boolean reuse) {
      return new Object[]{instance};
   }

   @Override
   public long[] getInstanceHashCodes() {
      return new long[]{instance.hashCode()};
   }

   @Override
   public int getInstanceCount() {
      return 1;
   }

   @Override
   public ITestNGMethod[] getTestMethods() {
      return new ITestNGMethod[]{method};
   }

   @Override
   public ITestNGMethod[] getBeforeTestMethods() {
      return new ITestNGMethod[0];
   }

   @Override
   public ITestNGMethod[] getAfterTestMethods() {
      return new ITestNGMethod[0];
   }

   @Override
   public ITestNGMethod[] getBeforeClassMethods() {
      return new ITestNGMethod[0];
   }

   @Override
   public ITestNGMethod[] getAfterClassMethods() {
      return new ITestNGMethod[0];
   }

   @Override
   public ITestNGMethod[] getBeforeSuiteMethods() {
      return new ITestNGMethod[0];
   }

   @Override
   public ITestNGMethod[] getAfterSuiteMethods() {
      return new ITestNGMethod[0];
   }

   @Override
   public ITestNGMethod[] getBeforeTestConfigurationMethods() {
      return new ITestNGMethod[0];
   }

   @Override
   public ITestNGMethod[] getAfterTestConfigurationMethods() {
      return new ITestNGMethod[0];
   }

   @Override
   public ITestNGMethod[] getBeforeGroupsMethods() {
      return new ITestNGMethod[0];
   }

   @Override
   public ITestNGMethod[] getAfterGroupsMethods() {
      return new ITestNGMethod[0];
   }

   @Override
   public String getName() {
      return instance.getClass().getName();
   }

   @Override
   public XmlTest getXmlTest() {
      return xmlTest;
   }

   @Override
   public XmlClass getXmlClass() {
      return null;
   }

   @Override
   public String getTestName() {
      return null;
   }

   @Override
   public Class getRealClass() {
      return instance.getClass();
   }

   @Override
   public void addInstance(Object instance) {

   }
}
