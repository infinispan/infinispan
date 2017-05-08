package org.infinispan.query.distributed;


import java.security.PrivilegedAction;

import javax.security.auth.Subject;

import org.infinispan.security.Security;
import org.infinispan.test.TestingUtil;
import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.distributed.SecureMassIndexingTest")
public class SecureMassIndexingTest extends DistributedMassIndexingTest {

   private static final Subject ADMIN = TestingUtil.makeSubject("admin");

   @Override
   protected String getConfigurationFile() {
      return "mass-index-with-security.xml";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      runAs(ADMIN, super::createCacheManagers);
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
      runAs(ADMIN, () -> super.clearContent());
   }

   @Override
   public void testPartiallyReindex() throws Exception {
      runAs(ADMIN, super::testPartiallyReindex);
   }

   @Override
   public void testReindexing() throws Exception {
      runAs(ADMIN, super::testReindexing);
   }

   interface TestExecution {
      void apply() throws Throwable;
   }

   private void runAs(Subject subject, TestExecution execution) {
      Security.doAs(subject, new PrivilegedAction<Void>() {
         @Override
         public Void run() {
            try {
               execution.apply();
            } catch (Throwable e) {
               e.printStackTrace();
               Assert.fail();
            }
            return null;
         }
      });
   }

}
