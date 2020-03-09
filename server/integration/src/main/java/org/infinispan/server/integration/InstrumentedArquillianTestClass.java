package org.infinispan.server.integration;

import org.infinispan.server.integration.enricher.ArquillianSupport;
import org.jboss.arquillian.junit.ArquillianTestClass;

/**
 * Add @Deployment annotation in BaseIT when we wish to test in the container
 */
public class InstrumentedArquillianTestClass extends ArquillianTestClass {

   static {
      if (ArquillianSupport.isClientMode()) {
         // the static code in InstrumentArquillianContainer will be execute once
      }
   }
}
