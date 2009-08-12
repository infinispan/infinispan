package org.infinispan.test.fwk;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.JChannel;
import org.jgroups.View;
import static org.testng.Assert.fail;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * The purpose of this class is to test that/if tcp + mping works fine in the given environment.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(testName = "test.fwk.TcpMPingEnvironmentTest", groups = "functional")
public class TcpMPingEnvironmentTest {

   Log log = LogFactory.getLog(TcpMPingEnvironmentTest.class);

   List<JChannel> openedChannles = new ArrayList<JChannel>();
   private boolean success = false;

   @AfterMethod
   public void destroyCaches() {
      for (JChannel ch : openedChannles) {
         ch.disconnect();
         ch.close();
      }
      if (!success) {
         Properties properties = System.getProperties();
         log.trace("System props are " + properties);
         System.out.println("System props are " + properties);
      }
   }

   /**
    * Tests that different clusters are created and that they don't overlap.
    */
   public void testDifferentClusters() throws Exception {
      log.trace("aaaaa");
      log.info("aaaaa");

      JChannel first1 = new JChannel("stacks/tcp_mping/tcp1.xml");
      JChannel first2 = new JChannel("stacks/tcp_mping/tcp1.xml");
      JChannel first3 = new JChannel("stacks/tcp_mping/tcp1.xml");
      initiChannel(first1);
      initiChannel(first2);
      initiChannel(first3);

      expectView(first1, first2, first3);

      JChannel second1 = new JChannel("stacks/tcp_mping/tcp2.xml");
      JChannel second2 = new JChannel("stacks/tcp_mping/tcp2.xml");
      JChannel second3 = new JChannel("stacks/tcp_mping/tcp2.xml");
      initiChannel(second1);
      initiChannel(second2);
      initiChannel(second3);

      expectView(first1, first2, first3);
      expectView(second1, second2, second3);
      success = true;
   }


   public void testMcastSocketCreation() throws Exception {
      InetAddress mcast_addr = InetAddress.getByName("228.10.10.5");
      SocketAddress saddr = new InetSocketAddress(mcast_addr, 43589);
      MulticastSocket retval = null;
      try {
         success = false;
         retval = new MulticastSocket(saddr);
         success = true;
      } finally {
         if (retval != null) {
            try {
               retval.close();
            } catch (Exception e) {
               e.printStackTrace();
            }
         }
      }
   }


   private void expectView(JChannel... channels) throws Exception {
      for (int i = 0; i < 20; i++) {
         boolean success = true;
         for (int j = 0; j < channels.length; j++) {
            View view = channels[j].getView();
            if (view == null) {
               success = false;
               break;
            }
            success = success && (view.size() == channels.length);
            if (view.size() > channels.length) assert false : "Clusters see each other!";
         }
         if (success) return;
         Thread.sleep(1000);
      }
      fail("Could not form cluster in given timeout");
   }


   private void initiChannel(JChannel channel) throws Exception {
      openedChannles.add(channel);
      channel.setOpt(org.jgroups.Channel.LOCAL, false);
      channel.setOpt(org.jgroups.Channel.AUTO_RECONNECT, true);
      channel.setOpt(org.jgroups.Channel.AUTO_GETSTATE, false);
      channel.setOpt(org.jgroups.Channel.BLOCK, true);
      channel.connect("someChannel");
   }
}
