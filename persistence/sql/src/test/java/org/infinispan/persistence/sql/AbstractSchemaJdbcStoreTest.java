package org.infinispan.persistence.sql;

import java.sql.Types;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sql.AbstractSchemaJdbcStoreTest")
class AbstractSchemaJdbcStoreTest {

   @Test
   void testProtostreamFieldTypeFrom() {
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.INT_32,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.INTEGER));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.INT_32,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.NUMERIC));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.INT_64,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.BIGINT));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.FLOAT,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.FLOAT));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.DOUBLE,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.DOUBLE));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BOOL,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.BIT));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BOOL,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.BOOLEAN));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.STRING,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.VARCHAR));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.STRING,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.NVARCHAR));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.STRING,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.LONGVARCHAR));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.STRING,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.LONGNVARCHAR));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BYTES,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.BLOB));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BYTES,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.BINARY));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BYTES,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.VARBINARY));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BYTES,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.LONGVARBINARY));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.DATE,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.DATE));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.DATE,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.TIME));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.DATE,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.TIMESTAMP));
      AssertJUnit.assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.DATE,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.TIMESTAMP_WITH_TIMEZONE));
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "SqlType not supported:.*")
   void testExceptionProtostreamFieldTypeFrom() {
      AbstractSchemaJdbcStore.ProtostreamFieldType.from(1234);
   }
}
