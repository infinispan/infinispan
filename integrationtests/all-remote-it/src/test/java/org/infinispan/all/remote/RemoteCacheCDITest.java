package org.infinispan.all.remote;

import static org.junit.Assert.assertEquals;

import org.infinispan.all.remote.cdi.CDITestBean;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.weld.environment.se.Weld;
import org.jboss.weld.environment.se.WeldContainer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests the basic CDI stuff with remote uber-jar
 * 
 * @author vjuranek
 */
@RunWith(Arquillian.class)
public class RemoteCacheCDITest {

   private CDITestBean bean;

   @Before
   public void loadBean() {
      WeldContainer weld = new Weld().initialize();  
      bean = weld.instance().select(CDITestBean.class).get(); 
   }
   
   @Test
   public void testDefaultCache() {
      bean.cachePut("pete", "British");
      bean.cachePut("manik", "Sri Lankan");

      assertEquals("British", bean.cacheGet("pete"));
      assertEquals("Sri Lankan", bean.cacheGet("manik"));
      assertEquals("British", bean.remoteCacheGet("pete"));
      assertEquals("Sri Lankan", bean.remoteCacheGet("manik"));
   }
   
   @Test
   public void testRemoteCacheManager() {
      Configuration cfg = bean.getRemoteCacheManagerConfiguration();
      assertEquals("127.0.0.1", cfg.servers().get(0).host());
      assertEquals(ConfigurationProperties.DEFAULT_PROTOCOL_VERSION, cfg.protocolVersion());
      assertEquals(ProtoStreamMarshaller.class, cfg.marshaller().getClass());
   }

}
