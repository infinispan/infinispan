package org.infinispan.compatibility.adaptor52x;

import org.infinispan.persistence.spi.PersistenceException;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "compatibility.adaptor52x.Adaptor52xCustomLoaderTest")
public class JdbcStringTest extends Adaptor52xCustomLoaderTest {

   public JdbcStringTest() {
      configurationFile = "52x-jdbc-string-loader.xml";
   }

   public void testLocationIsCorrect() {
   }

   @Override
   public void testPreloadAndExpiry() {
      //not applicable
   }

   @Override
   public void testLoadAndStoreMarshalledValues() throws PersistenceException {
      //not applicable
   }

   @Override
   public void testStopStartDoesNotNukeValues() throws InterruptedException, PersistenceException {
      //not applicable
   }
}
