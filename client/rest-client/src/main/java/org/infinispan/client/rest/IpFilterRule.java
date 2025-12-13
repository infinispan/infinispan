package org.infinispan.client.rest;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public record IpFilterRule(RuleType type, String cidr) {
   public enum RuleType {
      ACCEPT,
      REJECT
   }
}
