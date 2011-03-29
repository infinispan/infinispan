/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2011, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.marshall;

import org.infinispan.config.ExternalizerConfig;
import org.infinispan.config.GlobalConfiguration;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests configuration of user defined {@link Externalizer} implementations
 * using helpers methods in {@link GlobalConfiguration}.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "marshall.ForeignExternalizerQuickConfigTest")
public class ForeignExternalizerQuickConfigTest extends ForeignExternalizerTest {

   @Override
   protected GlobalConfiguration createForeignExternalizerGlobalConfig() {
      GlobalConfiguration globalCfg = GlobalConfiguration.getClusteredDefault();
      globalCfg.fluent().serialization()
         .addExternalizer(1234, new IdViaConfigObj.Externalizer())
         .addExternalizer(new IdViaAnnotationObj.Externalizer())
         .addExternalizer(3456, new IdViaBothObj.Externalizer());
      return globalCfg;
   }

   public void testExternalizerConfigInfo() {
      List<ExternalizerConfig> externalizers = manager(0).getGlobalConfiguration().getExternalizers();
      assert externalizers.size() == 3;
      ExternalizerConfig config = externalizers.get(0);
      assert config.getExternalizer() != null;
      assert config.getExternalizerClass() == IdViaConfigObj.Externalizer.class.getName();
      assert config.getId() == 1234;
      config = externalizers.get(1);
      assert config.getExternalizer() != null;
      assert config.getExternalizerClass() == IdViaAnnotationObj.Externalizer.class.getName();
      assert config.getId() == 5678;
      config = externalizers.get(2);
      assert config.getExternalizer() != null;
      assert config.getExternalizerClass() == IdViaBothObj.Externalizer.class.getName();
      assert config.getId() == 3456;
   }

   @Override
   protected String getCacheName() {
      return "ForeignExternalizersQuickConfig";
   }

}
