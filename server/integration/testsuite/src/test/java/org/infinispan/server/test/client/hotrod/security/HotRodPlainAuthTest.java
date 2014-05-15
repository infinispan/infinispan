package org.infinispan.server.test.client.hotrod.security;

import org.infinispan.server.test.category.SecurityHotRod;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * 
 * HotRodPlainAuthTest tests PLAIN SASL authentication of HotRod client.
 * 
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
@Category({ SecurityHotRod.class })
public class HotRodPlainAuthTest extends HotRodSaslAuthTestBase {

   @Override
   public String getTestedMech() {
      return "PLAIN";
   }

}
