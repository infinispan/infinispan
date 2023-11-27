package org.infinispan.manager.impl;

import static org.mockito.Mockito.when;

import java.util.HashSet;

import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.TestModuleRepository;
import org.mockito.Mockito;

/**
 * @since 15.0
 **/
public class InternalCacheManagerMock {
   public static InternalCacheManager mock(GlobalConfiguration gc) {
      InternalCacheManager cm = Mockito.mock(InternalCacheManager.class);
      GlobalComponentRegistry gcr = new GlobalComponentRegistry(
            gc, cm, new HashSet<>(),
            TestModuleRepository.defaultModuleRepository(),
            Mockito.mock(ConfigurationManager.class)
      );

      when(cm.getGlobalComponentRegistry()).thenReturn(gcr);
      return cm;
   }
}
