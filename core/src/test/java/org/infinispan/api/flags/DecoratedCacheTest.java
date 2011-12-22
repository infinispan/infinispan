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
package org.infinispan.api.flags;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheImpl;
import org.infinispan.DecoratedCache;
import org.infinispan.context.Flag;
import org.infinispan.test.CherryPickClassLoader;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
@Test(groups = "functional", testName = "api.flags.DecoratedCacheTest")
public class DecoratedCacheTest {

   public void testDecoratedCacheFlagsSet() {
      ClassLoader thisClassLoader = this.getClass().getClassLoader();
      CacheImpl impl = new CacheImpl("baseCache");
      DecoratedCache decoratedCache = new DecoratedCache(impl, thisClassLoader);
      DecoratedCache nofailCache = (DecoratedCache) decoratedCache.withFlags(Flag.FAIL_SILENTLY);
      assert nofailCache.getFlags().contains(Flag.FAIL_SILENTLY);
      assert nofailCache.getFlags().size() == 1;
      DecoratedCache asyncNoFailCache = (DecoratedCache) nofailCache.withFlags(Flag.FORCE_ASYNCHRONOUS);
      assert asyncNoFailCache.getFlags().size() == 2;
      assert asyncNoFailCache.getFlags().contains(Flag.FAIL_SILENTLY);
      assert asyncNoFailCache.getFlags().contains(Flag.FORCE_ASYNCHRONOUS);
      AdvancedCache again = asyncNoFailCache.withFlags(Flag.FAIL_SILENTLY);
      assert again == asyncNoFailCache; // as FAIL_SILENTLY was already specified
      
      CherryPickClassLoader cl = new CherryPickClassLoader(null, null, null, thisClassLoader);
      assert again.getClassLoader() == thisClassLoader;
      DecoratedCache clCache = (DecoratedCache) again.with(cl);
      assert clCache.getClassLoader() == cl;
      assert clCache.getFlags().size() == 2; //Flags inherited from previous withFlag()
      assert again.getClassLoader() == thisClassLoader; //original cache unaffected
      assert decoratedCache.getFlags() == null || decoratedCache.getFlags().size() == 0;
   }

}
