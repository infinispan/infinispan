package org.infinispan.server.integration;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.server.integration.enricher.ArquillianSupport;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.core.ServerRunMode;
import org.jboss.arquillian.core.api.annotation.Observes;
import org.jboss.arquillian.test.spi.event.suite.AfterClass;
import org.jboss.arquillian.test.spi.event.suite.BeforeClass;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

/*
 * It starts the ServerRunMode.CONTAINER in the client side
 */
public class InfinispanServerIntegrationProcessorExecuter {

   private InfinispanServerRule rule;

   public void onBeforeClass(@Observes BeforeClass event) {
      if (ArquillianSupport.isClientMode()) {
         Class<?> testCase = event.getTestClass().getJavaClass();
         InfinispanTest infinispanTest = testCase.getAnnotation(InfinispanTest.class);
         if (infinispanTest != null) {
            List<JavaArchive> archiveList = new ArrayList<>();
            Method[] methods = testCase.getDeclaredMethods();
            for (final Method method : methods) {
               InfinispanArchive infinispanArchive = method.getDeclaredAnnotation(InfinispanArchive.class);
               if (infinispanArchive != null) {
                  try {
                     JavaArchive archive = (JavaArchive) method.invoke(testCase);
                     archiveList.add(archive);
                  } catch (IllegalAccessException e) {
                     throw new IllegalStateException(e);
                  } catch (InvocationTargetException e) {
                     throw new IllegalStateException(e);
                  }
               }
            }

            Field[] fields = testCase.getDeclaredFields();
            for (final Field field : fields) {
               if (field.getAnnotation(InfinispanResourceTest.class) != null) {
                  JavaArchive[] archives = archiveList.toArray(new JavaArchive[archiveList.size()]);
                  this.rule = InfinispanServerRuleBuilder.config(infinispanTest.config())
                        .numServers(infinispanTest.numberOfServers())
                        .runMode(ServerRunMode.CONTAINER)
                        .artifacts(archives)
                        .build();
                  this.rule.before(testCase.getName());
               }
            }
         }
      }
   }

   public void onAfterClass(@Observes AfterClass event) {
      if (this.rule != null) {
         Class<?> testCase = event.getTestClass().getJavaClass();
         this.rule.after(testCase.getName());
      }
   }
}
