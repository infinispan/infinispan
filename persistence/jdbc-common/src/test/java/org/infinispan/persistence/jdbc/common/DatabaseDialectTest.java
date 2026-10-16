package org.infinispan.persistence.jdbc.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Verifies dialect detection and the upsert SQL generated for each {@link DatabaseType} without requiring a live database.
 */
class DatabaseDialectTest {

   private static final List<String> KEYS = Arrays.asList("id");
   private static final List<String> ALL = Arrays.asList("id", "name");

   @Test
   void cockroachdbIsGuessed() {
      assertEquals(DatabaseType.COCKROACHDB, DatabaseType.guessDialect("CockroachDB"));
      assertEquals(DatabaseType.COCKROACHDB, DatabaseType.guessDialect("cockroach"));
   }

   @Test
   void postgresIsStillGuessed() {
      assertEquals(DatabaseType.POSTGRES, DatabaseType.guessDialect("PostgreSQL"));
      assertEquals(DatabaseType.POSTGRES, DatabaseType.guessDialect("edb"));
   }

   @Test
   void unknownDialectIsNull() {
      assertEquals(null, DatabaseType.guessDialect(null));
      assertEquals(null, DatabaseType.guessDialect("unknown-db"));
   }

   @Test
   void cockroachdbUsesPostgresUpsert() {
      String upsert = upsertFor(DatabaseType.COCKROACHDB);
      assertTrue(upsert.contains("ON CONFLICT"), "expected Postgres upsert for CockroachDB but got: " + upsert);
   }

   @Test
   void postgresUsesOnConflictUpsert() {
      assertTrue(upsertFor(DatabaseType.POSTGRES).contains("ON CONFLICT"));
   }

   @Test
   void oracleXeUsesOracleUpsert() {
      String upsert = upsertFor(DatabaseType.ORACLE_XE);
      assertTrue(upsert.contains("from dual"), "expected Oracle upsert for Oracle XE but got: " + upsert);
   }

   @Test
   void oracleUsesOracleUpsert() {
      assertTrue(upsertFor(DatabaseType.ORACLE).contains("from dual"));
   }

   @Test
   void sql2003UsesGenericUpsert() {
      String upsert = upsertFor(DatabaseType.SQL2003);
      assertTrue(upsert.contains("USING (VALUES"), "expected generic SQL:2003 upsert but got: " + upsert);
   }

   private static String upsertFor(DatabaseType type) {
      return SqlManager.fromDatabaseType(type, "t").getUpsertStatement(KEYS, ALL);
   }
}
