package org.infinispan.configuration.serializer;

import static org.assertj.core.api.Assertions.assertThat;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.distribution.ch.impl.HashFunctionPartitioner;
import org.infinispan.distribution.ch.impl.RESPHashFunctionPartitioner;
import org.infinispan.remoting.transport.jgroups.EmbeddedJGroupsChannelConfigurator;
import org.testng.annotations.Test;

@Test(testName = "configuration.serializer.ConfigurationSerializerTest", groups = "functional")
public class ConfigurationSerializerTest extends AbstractConfigurationSerializerTest {

   public void testSimpleCacheConversion() {
      String name = "some-name";
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.LOCAL);
      cb.clustering().hash()
            .numOwners(1)
            .keyPartitioner(new RESPHashFunctionPartitioner());
      cb.invocationBatching().enable(true);
      assertConfigurationMatch(name, cb.build());
   }

   public void testReplicatedCacheConversion() {
      String name = "some-name";
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_SYNC);
      cb.clustering().hash().numOwners(1).keyPartitioner(new RESPHashFunctionPartitioner());
      cb.invocationBatching().enable(true);

      assertConfigurationMatch(name, cb.build());
   }

   public void testReplicatedCacheClusteringMatch() {
      ConfigurationBuilder cb1 = new ConfigurationBuilder();
      cb1.clustering().cacheMode(CacheMode.REPL_SYNC);
      cb1.clustering().hash().numOwners(1).keyPartitioner(new RESPHashFunctionPartitioner());
      Configuration numOwners1 = cb1.build();

      ConfigurationBuilder cb2 = new ConfigurationBuilder();
      cb2.clustering().cacheMode(CacheMode.REPL_SYNC);
      cb2.clustering().hash().numOwners(100).keyPartitioner(new RESPHashFunctionPartitioner());
      Configuration numOwners100 = cb2.build();

      // Assert only num-owners still match.
      assertThat(numOwners1.matches(numOwners100)).isTrue();

      // Assert that other parameters are taken into account.
      cb2.clustering().hash().keyPartitioner(new HashFunctionPartitioner());
      assertThat(numOwners1.matches(cb2.build())).isFalse();
   }

   public void testDistributedCacheConversion() {
      String name = "some-name";
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC);
      cb.clustering().hash().numOwners(1).keyPartitioner(new RESPHashFunctionPartitioner());
      cb.invocationBatching().enable(true);

      assertConfigurationMatch(name, cb.build());
   }

   @Override
   protected void compareExtraGlobalConfiguration(GlobalConfiguration before, GlobalConfiguration after) {
      for (var stackBefore : before.transport().jgroups().stacks()) {
         EmbeddedJGroupsChannelConfigurator.RemoteSites sitesBefore = stackBefore.configurator().getUncombinedRemoteSites();
         if (sitesBefore != null) {
            var stackAfter = after.transport().jgroups().stacks().stream()
                  .filter(s -> s.name().equals(stackBefore.name())).findFirst().orElse(null);
            assertThat(stackAfter).as("Stack %s missing after roundtrip", stackBefore.name()).isNotNull();
            EmbeddedJGroupsChannelConfigurator.RemoteSites sitesAfter = stackAfter.configurator().getUncombinedRemoteSites();
            assertThat(sitesAfter).as("Remote sites for stack %s missing after roundtrip", stackBefore.name()).isNotNull();
            assertThat(sitesAfter.getDefaultStack()).isEqualTo(sitesBefore.getDefaultStack());
            assertThat(sitesAfter.getRemoteSites().keySet()).isEqualTo(sitesBefore.getRemoteSites().keySet());
            for (var entry : sitesBefore.getRemoteSites().entrySet()) {
               assertThat(sitesAfter.getRemoteSites().get(entry.getKey()).getStack())
                     .isEqualTo(entry.getValue().getStack());
            }
         }
      }
   }

   private static void assertConfigurationMatch(String name, Configuration before) {
      String converted = before.toStringConfiguration(name);

      ParserRegistry registry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
      ConfigurationBuilderHolder holder = registry.parse(converted, MediaType.APPLICATION_XML);
      ConfigurationBuilder afterBuilder = holder.getNamedConfigurationBuilders().get(name);

      assertThat(afterBuilder).isNotNull();
      Configuration after = afterBuilder.build();
      assertThat(after.matches(before)).isTrue();

      before.validateUpdate(name, after);
      before.update(name, after);
   }
}
