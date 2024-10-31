package org.infinispan.server.resp.test;

import static org.infinispan.server.resp.test.RespTestingUtil.ADMIN;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;

public interface TestSetup {

   int clusterSize();

   EmbeddedCacheManager createCacheManager(Supplier<ConfigurationBuilder> base, Consumer<GlobalConfigurationBuilder> globalDecorator, Consumer<ConfigurationBuilder> decorator);

   static TestSetup singleNodeTestSetup() {
      return SingleNodeTestSetup.INSTANCE;
   }

   static TestSetup clusteredTestSetup(int numNodes) {
      return new MultiNodeTestSetup(numNodes);
   }

   static TestSetup authorizationEnabled(TestSetup delegate) {
      return new AuthorizationSetup(delegate);
   }

   class SingleNodeTestSetup implements TestSetup {
      private static final TestSetup INSTANCE = new SingleNodeTestSetup();

      private SingleNodeTestSetup() { }

      @Override
      public int clusterSize() {
         return 1;
      }

      @Override
      public EmbeddedCacheManager createCacheManager(Supplier<ConfigurationBuilder> base, Consumer<GlobalConfigurationBuilder> globalDecorator, Consumer<ConfigurationBuilder> decorator) {
         GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
         TestCacheManagerFactory.amendGlobalConfiguration(globalBuilder, new TransportFlags());
         globalDecorator.accept(globalBuilder);
         ConfigurationBuilder builder = base.get();
         decorator.accept(builder);
         return TestCacheManagerFactory.newDefaultCacheManager(true, globalBuilder, builder);
      }
   }

   record MultiNodeTestSetup(int clusterSize) implements TestSetup {

      @Override
      public EmbeddedCacheManager createCacheManager(Supplier<ConfigurationBuilder> base, Consumer<GlobalConfigurationBuilder> globalDecorator, Consumer<ConfigurationBuilder> decorator) {
         ConfigurationBuilder builder = base.get();
         decorator.accept(builder);
         GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
         globalDecorator.accept(gcb);
         return TestCacheManagerFactory.createClusteredCacheManager(gcb, builder);
      }
   }

   class AuthorizationSetup implements TestSetup {

      private final TestSetup delegate;

      public AuthorizationSetup(TestSetup delegate) {
         this.delegate = delegate;
      }

      @Override
      public int clusterSize() {
         return delegate.clusterSize();
      }

      @Override
      public EmbeddedCacheManager createCacheManager(Supplier<ConfigurationBuilder> base, Consumer<GlobalConfigurationBuilder> globalDecorator, Consumer<ConfigurationBuilder> decorator) {
         Consumer<GlobalConfigurationBuilder> wrapGlobal = builder -> {
            globalDecorator.accept(builder);
            enableAuthorization(builder);
         };
         return Security.doAs(ADMIN, () -> delegate.createCacheManager(base, wrapGlobal, decorator));
      }

      private void enableAuthorization(GlobalConfigurationBuilder builder) {
         GlobalAuthorizationConfigurationBuilder gcb = builder.security().authorization().enable()
               .principalRoleMapper(new IdentityRoleMapper());
         for (AuthorizationPermission perm : AuthorizationPermission.values()) {
            gcb.role(perm.toString()).permission(perm);
         }
      }
   }
}
