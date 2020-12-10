package org.infinispan.server.core.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.server.core.transport.IpSubnetFilterRule;

import io.netty.handler.ipfilter.IpFilterRuleType;

/**
 * IpFilterConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
public class IpFilterConfigurationBuilder<T extends ProtocolServerConfiguration, S extends ProtocolServerConfigurationChildBuilder<T, S>>
      extends AbstractProtocolServerConfigurationChildBuilder<T, S>
      implements Builder<IpFilterConfiguration> {

   private final List<IpSubnetFilterRule> rules = new ArrayList<>();

   public IpFilterConfigurationBuilder(ProtocolServerConfigurationChildBuilder<T, S> builder) {
      super(builder);
   }

   public IpFilterConfigurationBuilder<T, S> allowFrom(String rule) {
      rules.add(new IpSubnetFilterRule(rule, IpFilterRuleType.ACCEPT));
      return this;
   }

   public IpFilterConfigurationBuilder<T, S> rejectFrom(String rule) {
      rules.add(new IpSubnetFilterRule(rule, IpFilterRuleType.REJECT));
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public IpFilterConfiguration create() {
      return new IpFilterConfiguration(rules);
   }

   @Override
   public IpFilterConfigurationBuilder<T, S> read(IpFilterConfiguration template) {
      rules.clear();
      rules.addAll(template.rules());
      return this;
   }

   @Override
   public S self() {
      return (S) this;
   }
}
