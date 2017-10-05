package org.infinispan.server.test.client.rest;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.RESTSingleNode;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * @since 9.2
 */
@RunWith(Arquillian.class)
@Category({RESTSingleNode.class})
public class RESTOffHeapClientIT extends AbstractRESTClientIT {

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @Override
   protected void addRestServer() {
      rest.addServer(server1.getRESTEndpoint().getInetAddress().getHostName(), server1.getRESTEndpoint().getContextPath());
   }

   @Override
   protected String getDefaultCache() {
      return "offHeapCache";
   }

   @Test
   @Ignore
   public void testPutDataWithTimeToLive() throws Exception {
   }

   @Test
   @Ignore
   public void testPutDataTTLMaxIdleCombo1() throws Exception {
   }

   @Test
   @Ignore
   public void testPutDataTTLMaxIdleCombo2() throws Exception {
   }

   @Test
   @Ignore
   public void testPutDataWithMaxIdleTime() throws Exception {
   }
}
