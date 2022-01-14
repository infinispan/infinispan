package org.infinispan.server.functional;

import static org.junit.Assert.assertFalse;

import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * @author Dan Berindei
 * @since 14
 **/
public class StartupFailureIT {

   @Test
   public void testAddressAlreadyBound() throws Throwable {
      try (ServerSocket serverSocket = new ServerSocket(11222)) {
         AtomicBoolean ran = new AtomicBoolean();
         InfinispanServerRule rule = InfinispanServerRuleBuilder.server(false);
         Statement serverStatement = rule.apply(new Statement() {
            @Override
            public void evaluate() throws Throwable {
               ran.set(true);
            }
         }, Description.createTestDescription(StartupFailureIT.class, "testAddressAlreadyBound"));

         try {
            serverStatement.evaluate();
         } catch (Throwable e) {
            // Expected?
         }
         assertFalse(ran.get());
      }
   }
}
