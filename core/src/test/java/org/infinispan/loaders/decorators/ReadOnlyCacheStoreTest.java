/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.loaders.decorators;

import static org.easymock.EasyMock.*;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.decorators.ReadOnlyCacheStoreTest")
public class ReadOnlyCacheStoreTest extends AbstractInfinispanTest {
   public void testWriteMethods() throws CacheLoaderException {
      CacheStore mock = createMock(CacheStore.class);
      ReadOnlyStore store = new ReadOnlyStore(mock);
      InternalCacheEntry mockEntry = createNiceMock(InternalCacheEntry.class);
      expect(mock.load(eq("key"))).andReturn(mockEntry).once();
      replay(mock);

      // these should be "silent" no-ops and not actually change anything.
      store.clear();
      store.purgeExpired();
      store.remove("key");
      store.store(null);
      store.fromStream(null);
      store.prepare(null, null, true);
      store.commit(null);
      store.rollback(null);
      assert mockEntry == store.load("key");

      verify(mock);
   }
}
