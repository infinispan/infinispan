package org.infinispan.persistence.sql;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Types;

import org.junit.jupiter.api.Test;

class AbstractSchemaJdbcStoreTest {

   @Test
   void testProtostreamFieldTypeFrom() {
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.INT_32,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.INTEGER));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.INT_32,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.NUMERIC));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.INT_64,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.BIGINT));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.FLOAT,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.FLOAT));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.DOUBLE,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.DOUBLE));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BOOL,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.BIT));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BOOL,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.BOOLEAN));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.STRING,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.VARCHAR));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.STRING,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.NVARCHAR));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.STRING,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.LONGVARCHAR));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.STRING,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.LONGNVARCHAR));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BYTES,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.BLOB));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BYTES,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.BINARY));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BYTES,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.VARBINARY));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.BYTES,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.LONGVARBINARY));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.DATE,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.DATE));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.DATE,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.TIME));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.DATE,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.TIMESTAMP));
      assertEquals(AbstractSchemaJdbcStore.ProtostreamFieldType.DATE,
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(Types.TIMESTAMP_WITH_TIMEZONE));
      assertThrows(IllegalArgumentException.class, () -> {
         AbstractSchemaJdbcStore.ProtostreamFieldType.from(1234);
      });

   }
}
