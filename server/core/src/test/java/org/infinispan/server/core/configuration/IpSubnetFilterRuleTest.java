package org.infinispan.server.core.configuration;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.infinispan.server.core.transport.IpSubnetFilterRule;
import org.testng.annotations.Test;

import io.netty.handler.ipfilter.IpFilterRuleType;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
@Test(groups = "unit", testName = "server.configuration.IpSubnetFilterRuleTest")
public class IpSubnetFilterRuleTest {
   public void testIpSubnetFilterRule() throws UnknownHostException {
      IpSubnetFilterRule rule = new IpSubnetFilterRule("192.168.0.0/16", IpFilterRuleType.ACCEPT);
      assertTrue(rule.matches(new InetSocketAddress(InetAddress.getByName("192.168.0.1"), 11222)));
      assertFalse(rule.matches(new InetSocketAddress(InetAddress.getByName("10.11.12.13"), 11222)));
      rule = new IpSubnetFilterRule("/0", IpFilterRuleType.REJECT);
      assertTrue(rule.matches(new InetSocketAddress(InetAddress.getByName("192.168.0.1"), 11222)));
      assertTrue(rule.matches(new InetSocketAddress(InetAddress.getByName("10.11.12.13"), 11222)));

      rule = new IpSubnetFilterRule("fe80::/64", IpFilterRuleType.ACCEPT);
      assertTrue(rule.matches(new InetSocketAddress(InetAddress.getByName("fe80::9656:d028:8652:66b6"), 11222)));
      assertFalse(rule.matches(new InetSocketAddress(InetAddress.getByName("2001:0db8:0123:4567:89ab:fcde:1234:5670"), 11222)));
   }
}
