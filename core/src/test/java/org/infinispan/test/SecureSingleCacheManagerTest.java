package org.infinispan.test;

import javax.security.auth.Subject;

import org.infinispan.manager.EmbeddedCacheManager;

public abstract class SecureSingleCacheManagerTest extends SingleCacheManagerTest {
   static final Subject ADMIN = TestingUtil.makeSubject("admin");

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return null;
   }

}
