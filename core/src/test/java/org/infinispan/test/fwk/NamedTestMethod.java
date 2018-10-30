package org.infinispan.test.fwk;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.testng.IClass;
import org.testng.IRetryAnalyzer;
import org.testng.ITestClass;
import org.testng.ITestNGMethod;
import org.testng.internal.ConstructorOrMethod;
import org.testng.xml.XmlTest;

public class NamedTestMethod implements ITestNGMethod {
   private final ITestNGMethod method;
   private final String name;

   public NamedTestMethod(ITestNGMethod method, String name) {
      this.method = method;
      this.name = name;
   }

   @Override
   public Class getRealClass() {
      return method.getRealClass();
   }

   @Override
   public ITestClass getTestClass() {
      return method.getTestClass();
   }

   @Override
   public void setTestClass(ITestClass cls) {
      method.setTestClass(cls);
   }

   @Override
   @Deprecated
   public Method getMethod() {
      return method.getMethod();
   }

   @Override
   public String getMethodName() {
      return name;
   }

   @Override
   @Deprecated
   public Object[] getInstances() {
      return method.getInstances();
   }

   @Override
   public Object getInstance() {
      return method.getInstance();
   }

   @Override
   public long[] getInstanceHashCodes() {
      return method.getInstanceHashCodes();
   }

   @Override
   public String[] getGroups() {
      return method.getGroups();
   }

   @Override
   public String[] getGroupsDependedUpon() {
      return method.getGroupsDependedUpon();
   }

   @Override
   public String getMissingGroup() {
      return method.getMissingGroup();
   }

   @Override
   public void setMissingGroup(String group) {
      method.setMissingGroup(group);
   }

   @Override
   public String[] getBeforeGroups() {
      return method.getBeforeGroups();
   }

   @Override
   public String[] getAfterGroups() {
      return method.getAfterGroups();
   }

   @Override
   public String[] getMethodsDependedUpon() {
      return method.getMethodsDependedUpon();
   }

   @Override
   public void addMethodDependedUpon(String methodName) {
      method.addMethodDependedUpon(methodName);
   }

   @Override
   public boolean isTest() {
      return method.isTest();
   }

   @Override
   public boolean isBeforeMethodConfiguration() {
      return method.isBeforeMethodConfiguration();
   }

   @Override
   public boolean isAfterMethodConfiguration() {
      return method.isAfterMethodConfiguration();
   }

   @Override
   public boolean isBeforeClassConfiguration() {
      return method.isBeforeClassConfiguration();
   }

   @Override
   public boolean isAfterClassConfiguration() {
      return method.isAfterClassConfiguration();
   }

   @Override
   public boolean isBeforeSuiteConfiguration() {
      return method.isBeforeSuiteConfiguration();
   }

   @Override
   public boolean isAfterSuiteConfiguration() {
      return method.isAfterSuiteConfiguration();
   }

   @Override
   public boolean isBeforeTestConfiguration() {
      return method.isBeforeTestConfiguration();
   }

   @Override
   public boolean isAfterTestConfiguration() {
      return method.isAfterTestConfiguration();
   }

   @Override
   public boolean isBeforeGroupsConfiguration() {
      return method.isBeforeGroupsConfiguration();
   }

   @Override
   public boolean isAfterGroupsConfiguration() {
      return method.isAfterGroupsConfiguration();
   }

   @Override
   public long getTimeOut() {
      return method.getTimeOut();
   }

   @Override
   public void setTimeOut(long timeOut) {
      method.setTimeOut(timeOut);
   }

   @Override
   public int getInvocationCount() {
      return method.getInvocationCount();
   }

   @Override
   public void setInvocationCount(int count) {
      method.setInvocationCount(count);
   }

   @Override
   public int getTotalInvocationCount() {
      return method.getTotalInvocationCount();
   }

   @Override
   public int getSuccessPercentage() {
      return method.getSuccessPercentage();
   }

   @Override
   public String getId() {
      return method.getId();
   }

   @Override
   public void setId(String id) {
      method.setId(id);
   }

   @Override
   public long getDate() {
      return method.getDate();
   }

   @Override
   public void setDate(long date) {
      method.setDate(date);
   }

   @Override
   public boolean canRunFromClass(IClass testClass) {
      return method.canRunFromClass(testClass);
   }

   @Override
   public boolean isAlwaysRun() {
      return method.isAlwaysRun();
   }

   @Override
   public int getThreadPoolSize() {
      return method.getThreadPoolSize();
   }

   @Override
   public void setThreadPoolSize(int threadPoolSize) {
      method.setThreadPoolSize(threadPoolSize);
   }

   @Override
   public boolean getEnabled() {
      return method.getEnabled();
   }

   @Override
   public String getDescription() {
      return method.getDescription();
   }

   @Override
   public void setDescription(String s) {
      method.setDescription(s);
   }

   @Override
   public void incrementCurrentInvocationCount() {
      method.incrementCurrentInvocationCount();
   }

   @Override
   public int getCurrentInvocationCount() {
      return method.getCurrentInvocationCount();
   }

   @Override
   public void setParameterInvocationCount(int n) {
      method.setParameterInvocationCount(n);
   }

   @Override
   public int getParameterInvocationCount() {
      return method.getParameterInvocationCount();
   }

   @Override
   public void setMoreInvocationChecker(Callable<Boolean> moreInvocationChecker) {
      method.setMoreInvocationChecker(moreInvocationChecker);
   }

   @Override
   public boolean hasMoreInvocation() {
      return method.hasMoreInvocation();
   }

   @Override
   public ITestNGMethod clone() {
      return method.clone();
   }

   @Override
   public IRetryAnalyzer getRetryAnalyzer() {
      return method.getRetryAnalyzer();
   }

   @Override
   public void setRetryAnalyzer(IRetryAnalyzer retryAnalyzer) {
      method.setRetryAnalyzer(retryAnalyzer);
   }

   @Override
   public boolean skipFailedInvocations() {
      return method.skipFailedInvocations();
   }

   @Override
   public void setSkipFailedInvocations(boolean skip) {
      method.setSkipFailedInvocations(skip);
   }

   @Override
   public long getInvocationTimeOut() {
      return method.getInvocationTimeOut();
   }

   @Override
   public boolean ignoreMissingDependencies() {
      return method.ignoreMissingDependencies();
   }

   @Override
   public void setIgnoreMissingDependencies(boolean ignore) {
      method.setIgnoreMissingDependencies(ignore);
   }

   @Override
   public List<Integer> getInvocationNumbers() {
      return method.getInvocationNumbers();
   }

   @Override
   public void setInvocationNumbers(List<Integer> numbers) {
      method.setInvocationNumbers(numbers);
   }

   @Override
   public void addFailedInvocationNumber(int number) {
      method.addFailedInvocationNumber(number);
   }

   @Override
   public List<Integer> getFailedInvocationNumbers() {
      return method.getFailedInvocationNumbers();
   }

   @Override
   public int getPriority() {
      return method.getPriority();
   }

   @Override
   public void setPriority(int priority) {
      method.setPriority(priority);
   }

   @Override
   public XmlTest getXmlTest() {
      return method.getXmlTest();
   }

   @Override
   public ConstructorOrMethod getConstructorOrMethod() {
      return method.getConstructorOrMethod();
   }

   @Override
   public Map<String, String> findMethodParameters(XmlTest test) {
      return method.findMethodParameters(test);
   }

   @Override
   public String getQualifiedName() {
      return method.getQualifiedName();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || !(o instanceof ITestNGMethod)) return false;

      return method.equals(o);
   }

   @Override
   public int hashCode() {
      return method.hashCode();
   }
}
