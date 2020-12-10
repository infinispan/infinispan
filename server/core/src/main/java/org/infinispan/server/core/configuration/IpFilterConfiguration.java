package org.infinispan.server.core.configuration;

import java.util.Collections;
import java.util.List;

import org.infinispan.server.core.transport.IpSubnetFilterRule;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class IpFilterConfiguration {
   private volatile List<IpSubnetFilterRule> rules;

   public IpFilterConfiguration(List<IpSubnetFilterRule> rules) {
      this.rules = rules;
   }

   public List<IpSubnetFilterRule> rules() {
      return rules;
   }

   public void rules(List<IpSubnetFilterRule> rules) {
      this.rules = Collections.unmodifiableList(rules);
   }

   @Override
   public String toString() {
      return "IpFilterConfiguration{" +
            "rules=" + rules +
            '}';
   }
}
