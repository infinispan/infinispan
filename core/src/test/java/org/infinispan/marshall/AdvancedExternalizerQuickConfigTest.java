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

import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.testng.annotations.Test;

import java.util.Map;

/**
 * Tests configuration of user defined {@link AdvancedExternalizer} implementations
 * using helpers methods in {@link GlobalConfigurationBuilder}.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@Test(groups = "functional", testName = "marshall.AdvancedExternalizerQuickConfigTest")
public class AdvancedExternalizerQuickConfigTest extends AdvancedExternalizerTest {

   @Override
   protected GlobalConfigurationBuilder createForeignExternalizerGlobalConfig() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder().clusteredDefault();
      builder.serialization()
         .addAdvancedExternalizer(1234, new IdViaConfigObj.Externalizer())
         .addAdvancedExternalizer(new IdViaAnnotationObj.Externalizer())
         .addAdvancedExternalizer(3456, new IdViaBothObj.Externalizer());
      return builder;
   }

   public void testExternalizerConfigInfo() {
      Map<Integer, AdvancedExternalizer<?>> advExts =
            manager(0).getCacheManagerConfiguration().serialization().advancedExternalizers();
      assert advExts.size() == 3;
      AdvancedExternalizer<?> ext = advExts.get(1234);
      assert ext != null;
      assert ext.getClass() == IdViaConfigObj.Externalizer.class;
      ext = advExts.get(5678);
      assert ext != null;
      assert ext.getClass() == IdViaAnnotationObj.Externalizer.class;
      assert ext.getId() == 5678;
      ext = advExts.get(3456);
      assert ext != null;
      assert ext.getClass() == IdViaBothObj.Externalizer.class;
   }

   @Override
   protected String getCacheName() {
      return "ForeignExternalizersQuickConfig";
   }

}
