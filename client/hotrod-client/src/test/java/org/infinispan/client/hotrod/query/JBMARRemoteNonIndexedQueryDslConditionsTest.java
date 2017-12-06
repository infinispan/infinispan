package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test for query conditions (filtering). Exercises the whole query DSL on the sample domain model.
 *
 * @author anistor@redhat.com
 * @since 9.1
 */
@Test(groups = "functional", testName = "client.hotrod.query.JBMARRemoteNonIndexedQueryDslConditionsTest")
public class JBMARRemoteNonIndexedQueryDslConditionsTest extends JBMARRemoteQueryDslConditionsTest {

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.compatibility()
            .enable()
            .marshaller(new GenericJBossMarshaller());
      return builder;
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Indexing was not enabled on this cache.*")
   @Override
   public void testIndexPresence() {
      org.infinispan.query.Search.getSearchManager(getEmbeddedCache());
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextTerm() {
      super.testFullTextTerm();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS unless the property is indexed and analyzed.")
   @Override
   public void testFullTextPhrase() {
      super.testFullTextPhrase();
   }
}
