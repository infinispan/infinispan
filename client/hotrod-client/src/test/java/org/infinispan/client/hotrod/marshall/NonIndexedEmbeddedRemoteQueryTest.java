package org.infinispan.client.hotrod.marshall;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Tests interoperability between remote query and embedded mode. Do not enable indexing for query.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "client.hotrod.marshall.NonIndexedEmbeddedRemoteQueryTest", groups = "functional")
@CleanupAfterMethod
public class NonIndexedEmbeddedRemoteQueryTest extends EmbeddedRemoteInteropQueryTest {

   @Override
   protected ConfigurationBuilder createConfigBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      builder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      return builder;
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Indexing was not enabled on this cache.*")
   @Override
   public void testEmbeddedLuceneQuery() throws Exception {
      // this would only make sense for Lucene based query
      super.testEmbeddedLuceneQuery();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type sample_bank_account.Transaction unless the property is indexed and analyzed.")
   @Override
   public void testRemoteFullTextQuery() {
      super.testRemoteFullTextQuery();
   }
}
