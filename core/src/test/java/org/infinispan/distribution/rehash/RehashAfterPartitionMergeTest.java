package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.manager.CacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.JGroupsConfigBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.jgroups.Channel;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.remoting.transport.jgroups.JGroupsTransport.CONFIGURATION_STRING;
import static org.infinispan.test.fwk.TestCacheManagerFactory.amendMarshaller;
import static org.infinispan.test.fwk.TestCacheManagerFactory.minimizeThreads;

@Test(groups = "functional", testName =  "distribution.rehash.RehashAfterPartitionMergeTest")
public class RehashAfterPartitionMergeTest extends MultipleCacheManagersTest {

   Cache<Object, Object> c1, c2;
   List<Cache<Object, Object>> caches;
   DISCARD d1, d2;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration c = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      c.setLockAcquisitionTimeout(1000);

      GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();      
      amendMarshaller(gc);
      minimizeThreads(gc);
      Properties newTransportProps = new Properties();
      String jgc = JGroupsConfigBuilder.getJGroupsConfig();
      String discardString = "):DISCARD(use_gui=false";
      String newString = jgc.substring(0, jgc.indexOf("):")) + discardString + jgc.substring(jgc.indexOf("):"));
      newTransportProps.put(CONFIGURATION_STRING, newString);
      gc.setTransportProperties(newTransportProps);
      CacheManager cm1 = TestCacheManagerFactory.createCacheManager(gc, c, true, false, true);
      CacheManager cm2 = TestCacheManagerFactory.createCacheManager(gc, c, true, false, true);
      registerCacheManager(cm1, cm2);
      c1 = cm1.getCache();
      c2 = cm2.getCache();
      caches = Arrays.asList(c1, c2);
      d1 = getDiscardForCache(c1);
      d2 = getDiscardForCache(c2);
   }

   private DISCARD getDiscardForCache(Cache<?, ?> c) {
      JGroupsTransport jgt = (JGroupsTransport) TestingUtil.extractComponent(c, Transport.class);
      Channel ch = jgt.getChannel();
      return (DISCARD) ch.getProtocolStack().findProtocol(DISCARD.class);
   }

   public void testCachePartition() {
      c1.put("1", "value");
      c2.put("2", "value");

      for (Cache<Object, Object> c: caches) {
         assert "value".equals(c.get("1"));
         assert "value".equals(c.get("2"));
         assert c.getCacheManager().getMembers().size() == 2;
      }
      AtomicInteger ai = new AtomicInteger(0);
      c1.getCacheManager().addListener(new ViewChangeListener(ai));
      c2.getCacheManager().addListener(new ViewChangeListener(ai));

      d1.setDiscardAll(true);
      d2.setDiscardAll(true);

      // Wait till *both* instances have seen the view change.
      while (ai.get() < 2) TestingUtil.sleepThread(500);

      // we should see a network partition
      for (Cache<Object, Object> c: caches) assert c.getCacheManager().getMembers().size() == 1;

      c1.put("3", "value");
      c2.put("4", "value");

      assert "value".equals(c1.get("3"));
      assert null == c2.get("3");

      assert "value".equals(c2.get("4"));
      assert null == c1.get("4");

      ai.set(0);

      // lets "heal" the partition
      d1.setDiscardAll(false);
      d2.setDiscardAll(false);

      // wait till we see the view change
      while (ai.get() < 2) TestingUtil.sleepThread(500);

      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(c1, c2);

      c1.put("5", "value");
      c2.put("6", "value");
      for (Cache<Object, Object> c: caches) {
         assert "value".equals(c.get("5"));
         assert "value".equals(c.get("6"));
         assert c.getCacheManager().getMembers().size() == 2;
      }      
   }

   @Listener
   public static class ViewChangeListener {
      AtomicInteger ai;

      private ViewChangeListener(AtomicInteger ai) {
         this.ai = ai;
      }

      @ViewChanged
      public void handle(ViewChangedEvent e) {
         ai.getAndIncrement();
      }
   }
}
