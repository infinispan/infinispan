package org.infinispan.client.rest;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class IpFilterRule {
   public enum RuleType {
      ACCEPT,
      REJECT
   }
   private final RuleType type;
   private final String cidr;

   public IpFilterRule(RuleType type, String cidr) {
      this.type = type;
      this.cidr = cidr;
   }

   public RuleType getType() {
      return type;
   }

   public String getCidr() {
      return cidr;
   }
}
