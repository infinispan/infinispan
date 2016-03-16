package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.JGroupsConfigBuilder;
import org.infinispan.test.fwk.TestResourceTracker;
import org.infinispan.test.fwk.TransportFlags;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.fork.ForkChannel;
import org.jgroups.fork.UnknownForkHandler;
import org.jgroups.protocols.FORK;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.test.TestingUtil.blockUntilViewsReceived;
import static org.infinispan.test.TestingUtil.waitForRehashToComplete;

/**
 * Tests concurrent startup of caches using ForkChannels.
 *
 * @author Dan Berindei
 * @since 9.0
 */
@Test(testName = "statetransfer.ConcurrentStartTest", groups = "functional")
@CleanupAfterMethod
public class ConcurrentStartForkChannelTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      // The test method will create the cache managers
   }

   @DataProvider(name = "startOrder")
   public Object[][] startOrder() {
      return new Object[][]{{0, 1}, {1, 0}};
   }

   @Test(timeOut = 30000, dataProvider = "startOrder")
   public void testConcurrentStart(int eagerManager, int lazyManager) throws Exception {
      TestResourceTracker.testThreadStarted(this);
      byte[] cacheNotFoundResponseBytes = getCacheNotFoundResponseBytes();

      ConfigurationBuilder replCfg = new ConfigurationBuilder();
      replCfg.clustering().cacheMode(CacheMode.REPL_SYNC).stateTransfer().timeout(30, TimeUnit.SECONDS);

      String name1 = TestResourceTracker.getNextNodeName();
      String name2 = TestResourceTracker.getNextNodeName();

      // Create and connect both channels beforehand
      JChannel ch1 = createChannel(name1, 0);
      JChannel ch2 = createChannel(name2, 1);

      // Create the cache managers, but do not start them yet
      EmbeddedCacheManager cm1 = createCacheManager(replCfg, name1, ch1, cacheNotFoundResponseBytes);
      EmbeddedCacheManager cm2 = createCacheManager(replCfg, name2, ch2, cacheNotFoundResponseBytes);

      try {
         log.debugf("Cache managers created. Starting the caches");
         // When the coordinator starts first, it's ok to just start the caches in sequence.
         // When the coordinator starts last, however, the other node is not able to start before the
         // coordinator has the ClusterTopologyManager running.
         Future<Cache<String, String>> c1rFuture = fork(() -> manager(eagerManager).getCache("repl"));
         Thread.sleep(1000);
         Cache<String, String> c2r = manager(lazyManager).getCache("repl");
         Cache<String, String> c1r = c1rFuture.get(10, TimeUnit.SECONDS);

         blockUntilViewsReceived(10000, cm1, cm2);
         waitForRehashToComplete(c1r, c2r);
      } finally {
         // Stopping the cache managers isn't enough, because it will only close the ForkChannels
         cm1.stop();
         ch1.close();
         cm2.stop();
         ch2.close();
      }
   }

   private byte[] getCacheNotFoundResponseBytes() throws Exception {
      // The server modifies the RpcDispatcher so it can send a byte[0] instead of a serialized object,
      // but in a test it's easy to produce the correct payload.
      DefaultCacheManager manager = new DefaultCacheManager(true);
      try {
         Method getOrCreateComponent = ReflectionUtil
               .findMethod(GlobalComponentRegistry.class, "getOrCreateComponent",
                     new Class[]{Class.class, String.class});
         getOrCreateComponent.setAccessible(true);
         GlobalComponentRegistry gcr = manager.getGlobalComponentRegistry();
         StreamingMarshaller marshaller = (StreamingMarshaller) getOrCreateComponent
               .invoke(gcr, StreamingMarshaller.class, KnownComponentNames.GLOBAL_MARSHALLER);
         return marshaller.objectToByteBuffer(CacheNotFoundResponse.INSTANCE);
      } finally {
         manager.stop();
      }
   }

   private EmbeddedCacheManager createCacheManager(ConfigurationBuilder cacheCfg, String name,
         JChannel channel, final byte[] cacheNotFoundResponseBytes) throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder();
      gcb.transport().nodeName(channel.getName());
      gcb.globalJmxStatistics().allowDuplicateDomains(true);

      FORK fork = new FORK();
      fork.setUnknownForkHandler(new UnknownForkHandler() {
         @Override
         public Object handleUnknownForkStack(Message message, String forkStackId) {
            return handle(message);
         }

         @Override
         public Object handleUnknownForkChannel(Message message, String forkChannelId) {
            return handle(message);
         }

         private Object handle(Message message) {
            short id = ClassConfigurator.getProtocolId(RequestCorrelator.class);
            RequestCorrelator.Header header = (RequestCorrelator.Header) message.getHeader(id);
            if (header != null) {
               log.debugf("Sending CacheNotFoundResponse reply for %s", header);
               Message response = message.makeReply().setFlag(message.getFlags())
                     .clearFlag(Message.Flag.RSVP, Message.Flag.SCOPED);

               response.putHeader(FORK.ID, message.getHeader(FORK.ID));
               response.putHeader(id,
                     new RequestCorrelator.Header(RequestCorrelator.Header.RSP, header.req_id, id));
               response.setBuffer(cacheNotFoundResponseBytes);

               fork.down(new Event(Event.MSG, response));
            }
            return null;
         }
      });
      channel.getProtocolStack().addProtocol(fork);
      ForkChannel fch = new ForkChannel(channel, "stack1", "channel1");
      CustomChannelLookup.registerChannel(gcb, fch, name, true);

      EmbeddedCacheManager cm = new DefaultCacheManager(gcb.build(), cacheCfg.build(), false);
      registerCacheManager(cm);
      return cm;
   }

   private JChannel createChannel(String name, int portRange) throws Exception {
      JChannel ch1 = new JChannel(JGroupsConfigBuilder
            .getJGroupsConfig(ConcurrentStartForkChannelTest.class.getName(),
                  new TransportFlags().withPortRange(portRange)));
      ch1.setName(name);
      ch1.connect(ConcurrentStartForkChannelTest.class.getSimpleName());
      log.tracef("Channel %s connected: %s", ch1, ch1.getViewAsString());
      return ch1;
   }

}
