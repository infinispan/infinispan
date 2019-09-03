package org.infinispan.client.hotrod.query;

import org.testng.annotations.Test;

/**
 * Non-index querying should still work with a message that is not annotated for indexing.
 *
 * @author anistor@redhat.com
 * @since 9.3
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryDisableLegacyIndexingTest")
public class RemoteQueryDisableLegacyIndexingTest extends RemoteQueryDisableIndexingTest {

   private static final String NOT_INDEXED_PROTO_SCHEMA = "package sample_bank_account;\n" +
         "message NotIndexed {\n" +
         "\toptional string notIndexedField = 1;\n" +
         "}\n";

   public RemoteQueryDisableLegacyIndexingTest() {
      super(NOT_INDEXED_PROTO_SCHEMA);
   }
}
