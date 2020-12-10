package org.infinispan.cli.completers;

import org.infinispan.client.rest.IpFilterRule;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public class IpFilterRuleCompleter extends EnumCompleter<IpFilterRule.RuleType> {

   public IpFilterRuleCompleter() {
      super(IpFilterRule.RuleType.class);
   }
}
