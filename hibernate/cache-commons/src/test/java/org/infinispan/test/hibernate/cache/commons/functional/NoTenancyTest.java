package org.infinispan.test.hibernate.cache.commons.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.hibernate.cache.commons.InfinispanBaseRegion;
import org.infinispan.test.hibernate.cache.commons.functional.entities.Item;
import org.junit.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class NoTenancyTest extends SingleNodeTest {
	 @Override
	 public List<Object[]> getParameters() {
		  return Collections.singletonList(READ_ONLY_INVALIDATION);
	 }

	 @Test
	 public void testNoTenancy() throws Exception {
		  final Item item = new Item("my item", "description" );

		  withTxSession(s -> s.persist(item));
		  for (int i = 0; i < 5; ++i) { // make sure we get something cached
				withTxSession(s -> {
					  Item item2 = s.get(Item.class, item.getId());
					  assertNotNull(item2);
					  assertEquals(item.getName(), item2.getName());
				});

		  }
		  InfinispanBaseRegion region = TEST_SESSION_ACCESS.getRegion(sessionFactory(), Item.class.getName());
		  AdvancedCache localCache = region.getCache().withFlags(Flag.CACHE_MODE_LOCAL);
		  assertEquals(1, localCache.size());
	 }
}
