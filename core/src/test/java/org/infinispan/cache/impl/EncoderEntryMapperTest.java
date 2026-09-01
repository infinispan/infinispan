package org.infinispan.cache.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.encoding.DataConversion;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "cache.impl.EncoderEntryMapperTest")
public class EncoderEntryMapperTest extends AbstractInfinispanTest {

   public void testApplyWithNullMetadata() {
      InternalEntryFactory entryFactory = new InternalEntryFactoryImpl();
      TestingUtil.inject(entryFactory, TIME_SERVICE);

      DataConversion keyConversion = DataConversion.newKeyDataConversion();
      DataConversion valueConversion = DataConversion.newValueDataConversion();

      EncoderEntryMapper<Object, Object, CacheEntry<Object, Object>> mapper =
            EncoderEntryMapper.newCacheEntryMapper(keyConversion, valueConversion, entryFactory);

      WrappedByteArray key = new WrappedByteArray(new byte[]{1, 2, 3});
      WrappedByteArray value = new WrappedByteArray(new byte[]{4, 5, 6});
      MetadataMortalCacheEntry entry = new MetadataMortalCacheEntry(key, value, null, -1);

      CacheEntry<Object, Object> result = assertDoesNotThrow(() -> mapper.apply(entry));
      assertNotNull(result);
   }
}
