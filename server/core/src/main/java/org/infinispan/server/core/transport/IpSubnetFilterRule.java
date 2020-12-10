package org.infinispan.server.core.transport;

import java.net.InetSocketAddress;

import io.netty.handler.ipfilter.IpFilterRule;
import io.netty.handler.ipfilter.IpFilterRuleType;
import io.netty.util.internal.ObjectUtil;

/**
 * This differs from Netty's equivalent {@link io.netty.handler.ipfilter.IpSubnetFilterRule} in that it parses CIDR
 * addresses
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
public class IpSubnetFilterRule implements IpFilterRule {
   private final io.netty.handler.ipfilter.IpSubnetFilterRule rule;
   private final String cidr;

   public IpSubnetFilterRule(String cidr, IpFilterRuleType type) {
      ObjectUtil.checkNotNull(cidr, "cidr");
      ObjectUtil.checkNotNull(type, "type");

      int sep = cidr.indexOf('/');
      if (sep < 0) {
         throw new IllegalArgumentException(cidr);
      }
      this.cidr = cidr;
      this.rule = new io.netty.handler.ipfilter.IpSubnetFilterRule(cidr.substring(0, sep), Integer.parseInt(cidr.substring(sep + 1)), type);
   }

   public String cidr() {
      return cidr;
   }

   @Override
   public boolean matches(InetSocketAddress inetSocketAddress) {
      return rule.matches(inetSocketAddress);
   }

   @Override
   public IpFilterRuleType ruleType() {
      return rule.ruleType();
   }

   @Override
   public String toString() {
      return "IpSubnetFilterRule{" +
            "rule=" + rule.ruleType() +
            ", cidr='" + cidr + '\'' +
            '}';
   }
}
