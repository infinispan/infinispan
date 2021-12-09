package org.infinispan.server.security;

import java.security.Provider;
import java.util.function.Supplier;

import org.wildfly.security.password.WildFlyElytronPasswordProvider;

/**
 * @since 14.0
 **/
public class ElytronPasswordProviderSupplier implements Supplier<Provider[]> {
   public static final Supplier<Provider[]> INSTANCE = new ElytronPasswordProviderSupplier();
   public static final Provider[] PROVIDERS = new Provider[]{WildFlyElytronPasswordProvider.getInstance()};

   private ElytronPasswordProviderSupplier() {
   }

   @Override
   public Provider[] get() {
      return PROVIDERS;
   }
}
