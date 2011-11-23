/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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

import java.util.List;

import org.infinispan.api.marshall.AdvancedExternalizer;
import org.infinispan.config.AdvancedExternalizerConfig;
import org.infinispan.config.GlobalConfiguration;
import org.testng.annotations.Test;

/**
 * Tests configuration of user defined {@link AdvancedExternalizer} implementations
 * using helpers methods in {@link GlobalConfiguration}.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "marshall.AdvancedExternalizerQuickConfigTest")
public class AdvancedExternalizerQuickConfigTest extends AdvancedExternalizerTest {

   @Override
   protected GlobalConfiguration createForeignExternalizerGlobalConfig() {
      GlobalConfiguration globalCfg = GlobalConfiguration.getClusteredDefault();
      globalCfg.fluent().serialization()
         .addAdvancedExternalizer(1234, new IdViaConfigObj.Externalizer())
         .addAdvancedExternalizer(new IdViaAnnotationObj.Externalizer())
         .addAdvancedExternalizer(3456, new IdViaBothObj.Externalizer());
      return globalCfg;
   }

   public void testExternalizerConfigInfo() {
      List<AdvancedExternalizerConfig> advancedExternalizers = manager(0).getGlobalConfiguration().getExternalizers();
      assert advancedExternalizers.size() == 3;
      AdvancedExternalizerConfig config = advancedExternalizers.get(0);
      assert config.getAdvancedExternalizer() != null;
      assert config.getExternalizerClass() == IdViaConfigObj.Externalizer.class.getName();
      assert config.getId() == 1234;
      config = advancedExternalizers.get(1);
      assert config.getAdvancedExternalizer() != null;
      assert config.getExternalizerClass() == IdViaAnnotationObj.Externalizer.class.getName();
      assert config.getId() == 5678;
      config = advancedExternalizers.get(2);
      assert config.getAdvancedExternalizer() != null;
      assert config.getExternalizerClass() == IdViaBothObj.Externalizer.class.getName();
      assert config.getId() == 3456;
   }

   @Override
   protected String getCacheName() {
      return "ForeignExternalizersQuickConfig";
   }

}
