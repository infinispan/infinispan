package org.infinispan.client.hotrod.query;

import org.testng.annotations.Test;

/**
 * Test the disabling of indexing of not annotated message using the "indexed_by_default" option. Non-indexing querying
 * should still work.
 *
 * @author anistor@redhat.com
 * @since 9.3
 */
@Test(groups = "functional", testName = "client.hotrod.query.RemoteQueryDisableLegacyIndexingTest")
public class RemoteQueryDisableLegacyIndexingTest extends RemoteQueryDisableIndexingTest {

   private static final String NOT_INDEXED_PROTO_SCHEMA = "package sample_bank_account;\n" +
         "option indexed_by_default = false;\n" +
         "message NotIndexed {\n" +
         "\toptional string notIndexedField = 1;\n" +
         "}\n";

   public RemoteQueryDisableLegacyIndexingTest() {
      super(NOT_INDEXED_PROTO_SCHEMA);
   }
}
