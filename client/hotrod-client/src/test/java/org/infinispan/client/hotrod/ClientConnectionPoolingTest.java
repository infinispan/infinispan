package org.infinispan.client.hotrod;

import org.infinispan.test.MultipleCacheManagersTest;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ClientConnectionPoolingTest extends MultipleCacheManagersTest {


   @Override
   protected void createCacheManagers() throws Throwable {
      // TODO: Customise this generated block
   }

   /**
    * What happens if a server goes down and after that, we try to create a connection to that server.
    */
   public void testServerGoesDown() {

   }
}
