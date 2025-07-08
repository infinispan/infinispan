package org.infinispan.server.security.realm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.security.Principal;
import java.security.spec.AlgorithmParameterSpec;

import org.junit.Test;
import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.principal.NamePrincipal;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.evidence.Evidence;

public class CaffeineRealmIdentityCacheTest {

   @Test
   public void testBoundedImmortal() {
      CaffeineRealmIdentityCache cache = new CaffeineRealmIdentityCache(5, -1);
      TestRealmIdentity identity = new TestRealmIdentity("1");
      cache.put(new NamePrincipal("1"), identity);
      assertEquals(identity, cache.get(new NamePrincipal("1")));
      assertEquals(1, cache.identityCache().estimatedSize());
   }

   @Test
   public void testBoundedMortal() throws InterruptedException {
      CaffeineRealmIdentityCache cache = new CaffeineRealmIdentityCache(5, 2000);
      TestRealmIdentity identity = new TestRealmIdentity("1");
      cache.put(new NamePrincipal("1"), identity);
      assertEquals(identity, cache.get(new NamePrincipal("1")));
      Thread.sleep(3000);
      cache.identityCache().cleanUp();
      assertNull(cache.get(new NamePrincipal("1")));

   }

   static class TestRealmIdentity implements RealmIdentity {
      private final String name;

      TestRealmIdentity(String name) {
         this.name = name;
      }

      @Override
      public Principal getRealmIdentityPrincipal() {
         return new NamePrincipal(name);
      }

      @Override
      public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> aClass, String s, AlgorithmParameterSpec algorithmParameterSpec) throws RealmUnavailableException {
         return null;
      }

      @Override
      public <C extends Credential> C getCredential(Class<C> aClass) throws RealmUnavailableException {
         return null;
      }

      @Override
      public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> aClass, String s) throws RealmUnavailableException {
         return null;
      }

      @Override
      public boolean verifyEvidence(Evidence evidence) throws RealmUnavailableException {
         return false;
      }

      @Override
      public boolean exists() throws RealmUnavailableException {
         return false;
      }
   }
}
