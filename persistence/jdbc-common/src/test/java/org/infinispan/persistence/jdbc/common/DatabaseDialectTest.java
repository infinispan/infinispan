package org.infinispan.persistence.jdbc.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.testng.annotations.Test;

/**
 * Verifies dialect detection and the upsert SQL generated for each {@link DatabaseType} without requiring a live database.
 */
@Test(groups = "unit", testName = "persistence.jdbc.common.DatabaseDialectTest")
public class DatabaseDialectTest {

   private static final List<String> KEYS = List.of("id");
   private static final List<String> ALL = List.of("id", "name");

   private static final String POSTGRES_UPSERT =
         "INSERT INTO t (id, name) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name";
   private static final String ORACLE_UPSERT =
         "MERGE INTO t t USING (SELECT ? id, ? name from dual) tmp ON (((t.id = tmp.id) OR (t.id IS NULL AND tmp.id IS NULL))) " +
               "WHEN MATCHED THEN UPDATE SET t.name = tmp.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (tmp.id, tmp.name)";
   private static final String SQL2003_UPSERT =
         "MERGE INTO t USING (VALUES (?, ?)) AS tmp (id, name) ON (id = tmp.id) " +
               "WHEN MATCHED THEN UPDATE SET name = tmp.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (tmp.id, tmp.name)";

   public void cockroachdbIsGuessed() {
      assertEquals(DatabaseType.COCKROACHDB, DatabaseType.guessDialect("CockroachDB"));
      assertEquals(DatabaseType.COCKROACHDB, DatabaseType.guessDialect("cockroach"));
   }

   public void postgresIsStillGuessed() {
      assertEquals(DatabaseType.POSTGRES, DatabaseType.guessDialect("PostgreSQL"));
      assertEquals(DatabaseType.POSTGRES, DatabaseType.guessDialect("edb"));
   }

   public void unknownDialectIsNull() {
      assertNull(DatabaseType.guessDialect(null));
      assertNull(DatabaseType.guessDialect("unknown-db"));
   }

   public void cockroachdbUsesPostgresUpsert() {
      assertEquals(POSTGRES_UPSERT, upsertFor(DatabaseType.COCKROACHDB));
   }

   public void postgresUsesOnConflictUpsert() {
      assertEquals(POSTGRES_UPSERT, upsertFor(DatabaseType.POSTGRES));
   }

   public void oracleXeUsesOracleUpsert() {
      assertEquals(ORACLE_UPSERT, upsertFor(DatabaseType.ORACLE_XE));
   }

   public void oracleUsesOracleUpsert() {
      assertEquals(ORACLE_UPSERT, upsertFor(DatabaseType.ORACLE));
   }

   public void sql2003UsesGenericUpsert() {
      assertEquals(SQL2003_UPSERT, upsertFor(DatabaseType.SQL2003));
   }

   private static String upsertFor(DatabaseType type) {
      return SqlManager.fromDatabaseType(type, "t").getUpsertStatement(KEYS, ALL);
   }
}
