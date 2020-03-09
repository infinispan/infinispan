package org.infinispan.server.integration.enricher;

import static org.infinispan.server.integration.InfinispanServerIntegrationUtil.getInfinispanServerTestMethodRule;

import java.lang.reflect.Field;
import java.util.Properties;

import org.infinispan.server.integration.InfinispanResourceTest;
import org.infinispan.server.integration.InfinispanServerIntegrationUtil;
import org.infinispan.server.integration.InfinispanTest;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.jboss.arquillian.core.api.annotation.Observes;
import org.jboss.arquillian.test.spi.event.suite.After;
import org.jboss.arquillian.test.spi.event.suite.Before;

/*
 * Responsible to inject InfinispanServerTestMethodRule in the client and server side
 */
public class Enricher {

   public void onBefore(@Observes Before event) {
      Class<?> testCase = event.getTestClass().getJavaClass();
      InfinispanTest infinispanTest = testCase.getAnnotation(InfinispanTest.class);
      if (infinispanTest != null) {

         Field[] fields = testCase.getDeclaredFields();
         for (final Field field : fields) {
            if (field.getAnnotation(InfinispanResourceTest.class) != null) {
               String testName = testCase.getName();
               InfinispanServerTestConfiguration configuration =
                     new InfinispanServerTestConfiguration(infinispanTest.config(), infinispanTest.numberOfServers(),
                           ServerRunMode.REMOTE_CONTAINER, new Properties(), null,
                           null, false, true);
               RemoteInfinispanServerRule serverRule = new RemoteInfinispanServerRule(configuration);
               serverRule.before(testName);
               InfinispanServerTestMethodRule infinispanServerTestMethodRule = new InfinispanServerTestMethodRule(serverRule);
               InfinispanServerIntegrationUtil.setFieldValue(field, null, infinispanServerTestMethodRule);
               infinispanServerTestMethodRule.before(testName, event.getTestMethod().getName());
            }
         }
      }
   }

   public void onAfter(@Observes After event) {
      Class<?> testCase = event.getTestClass().getJavaClass();
      if (testCase.isAnnotationPresent(InfinispanTest.class)) {
         String testName = testCase.getName();
         Field[] fields = testCase.getDeclaredFields();
         for (final Field field : fields) {
            if (field.getAnnotation(InfinispanResourceTest.class) != null) {
               InfinispanServerTestMethodRule infinispanServerTestMethodRule = getInfinispanServerTestMethodRule(field, null);
               infinispanServerTestMethodRule.getInfinispanServerRule().after(testName);
               infinispanServerTestMethodRule.after();
            }
         }
      }
   }

   public static class RemoteInfinispanServerRule extends InfinispanServerRule {

      public RemoteInfinispanServerRule(InfinispanServerTestConfiguration configuration) {
         super(configuration);
      }

      @Override
      protected void createAndStartServerDriver(String testName) {
         this.serverDriver = configuration.newDriver();
         // the container is remote
         if (this.configurationEnhancers.size() > 0) {
            throw new IllegalStateException("RemoteContainer doesn't support configuration enhancers");
         }
      }
   }
}
