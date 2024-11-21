package org.infinispan.client.hotrod.marshall;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Date;
import java.util.List;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS;
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

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = "org.infinispan.objectfilter.ParsingException: ISPN028521: Full-text queries cannot be applied to property 'longDescription' in type sample_bank_account.Transaction unless the property is indexed and analyzed.")
   @Override
   public void testRemoteFullTextQuery() {
      Transaction transaction = new TransactionHS();
      transaction.setId(3);
      transaction.setDescription("Hotel");
      transaction.setLongDescription("Expenses for Infinispan F2F meeting");
      transaction.setAccountId(2);
      transaction.setAmount(99);
      transaction.setDate(new Date(42));
      transaction.setDebit(true);
      transaction.setValid(true);
      cache.put(transaction.getId(), transaction);

      Query<Transaction> q = remoteCache.query("from sample_bank_account.Transaction where longDescription:'Expenses for Infinispan F2F meeting'");

      List<Transaction> list = q.execute().list();
      assertEquals(1, list.size());
   }
}
