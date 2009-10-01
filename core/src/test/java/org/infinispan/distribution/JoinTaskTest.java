package org.infinispan.distribution;

import org.easymock.EasyMock;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

@Test(groups = "unit", testName = "distribution.JoinTaskTest")
public class JoinTaskTest {
   public void testCalculatingWhosensStateRC2() {
      doTest(2);
   }

   public void testCalculatingWhosensStateRC4() {
      doTest(4);
   }

   private void doTest(int rc) {
      Address a1 = new TestAddress(10);
      Address a2 = new TestAddress(20);
      Address a3 = new TestAddress(30);
      Address a4 = new TestAddress(40);
      Address a5 = new TestAddress(50);
      Address a6 = new TestAddress(60);

      Address joiner = new TestAddress(33);

      Transport trans = EasyMock.createNiceMock(Transport.class);
      EasyMock.expect(trans.getAddress()).andReturn(joiner).anyTimes();

      RpcManager rpc = EasyMock.createNiceMock(RpcManager.class);
      EasyMock.expect(rpc.getTransport()).andReturn(trans).anyTimes();

      EasyMock.replay(trans, rpc);

      JoinTask jt = new JoinTask(rpc, null, null, null, null, null);

      ConsistentHash ch = new DefaultConsistentHash();
      ch.setCaches(Arrays.asList(a1, a2, a3, a4, a5, a6, joiner));

      jt.chNew = ch;

      List<Address> a = jt.getAddressesWhoMaySendStuff(rc);
      List<Address> expected;
      if (rc == 2)
         expected = Arrays.asList(a3, a4);
      else
         expected = Arrays.asList(a1, a2, a3, a4);

      assert a.equals(expected) : "Expected " + expected + " but was " + a;
   }
}
