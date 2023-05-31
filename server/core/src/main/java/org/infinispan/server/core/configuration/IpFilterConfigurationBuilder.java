package org.infinispan.server.core.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.transport.IpSubnetFilterRule;

import io.netty.handler.ipfilter.IpFilterRuleType;

/**
 * IpFilterConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 12.1
 */
public class IpFilterConfigurationBuilder<T extends ProtocolServerConfiguration<T, A>, S extends ProtocolServerConfigurationChildBuilder<T, S, A>, A extends AuthenticationConfiguration>
      extends AbstractProtocolServerConfigurationChildBuilder<T, S, A>
      implements Builder<IpFilterConfiguration> {

   private final List<IpSubnetFilterRule> rules = new ArrayList<>();

   public IpFilterConfigurationBuilder(ProtocolServerConfigurationChildBuilder<T, S, A> builder) {
      super(builder);
   }

   @Override
   public AttributeSet attributes() {
      return AttributeSet.EMPTY;
   }

   public IpFilterConfigurationBuilder<T, S, A> allowFrom(String rule) {
      rules.add(new IpSubnetFilterRule(rule, IpFilterRuleType.ACCEPT));
      return this;
   }

   public IpFilterConfigurationBuilder<T, S, A> rejectFrom(String rule) {
      rules.add(new IpSubnetFilterRule(rule, IpFilterRuleType.REJECT));
      return this;
   }

   @Override
   public IpFilterConfiguration create() {
      return new IpFilterConfiguration(rules);
   }

   @Override
   public IpFilterConfigurationBuilder<T, S, A> read(IpFilterConfiguration template, Combine combine) {
      rules.clear();
      rules.addAll(template.rules());
      return this;
   }

   @Override
   public S self() {
      return (S) this;
   }
}
