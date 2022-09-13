package org.infinispan.persistence.sql;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.util.IntSet;
import org.infinispan.persistence.jdbc.common.DatabaseType;
import org.infinispan.persistence.jdbc.common.SqlManager;
import org.infinispan.persistence.jdbc.common.TableOperations;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.logging.Log;
import org.infinispan.persistence.sql.configuration.TableJdbcStoreConfiguration;
import org.infinispan.util.logging.LogFactory;

@ConfiguredBy(TableJdbcStoreConfiguration.class)
public class TableJdbcStore<K, V> extends AbstractSchemaJdbcStore<K, V, TableJdbcStoreConfiguration> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   @Override
   protected TableOperations<K, V> actualCreateTableOperations(ProtoSchemaOptions<K, V, TableJdbcStoreConfiguration> schemaOptions) {
      return new TableTableOperations(schemaOptions, schemaOptions.valueParameters);
   }

   public class TableTableOperations extends SchemaTableOperations<K, V, TableJdbcStoreConfiguration> {
      private final String selectSql;
      private final String selectAllSql;
      private final String deleteSql;
      private final String deleteAllSql;
      private final String upsertSql;
      private final String sizeSql;

      public TableTableOperations(ProtoSchemaOptions<K, V, TableJdbcStoreConfiguration> options, Parameter[] upsertParameters) {
         super(options, upsertParameters);
         DatabaseType type = options.config.dialect();
         if (type == null) {
            Connection connection = null;
            try {
               connection = connectionFactory.getConnection();
               String dbProduct = connection.getMetaData().getDatabaseProductName();
               type = DatabaseType.guessDialect(dbProduct);
               log.debugf("Guessing database dialect as '%s'.  If this is incorrect, please specify the correct " +
                           "dialect using the 'dialect' attribute in your configuration.  Supported database dialect strings are %s",
                     type, Arrays.toString(DatabaseType.values()));
            } catch (Exception e) {
               throw log.unableToDetectDialect(Arrays.toString(DatabaseType.values()));
            } finally {
               connectionFactory.releaseConnection(connection);
            }
         }
         SqlManager statements = SqlManager.fromDatabaseType(type, config.tableName());
         List<String> keyNames = Arrays.stream(options.keyParameters)
               .map(Parameter::getName).collect(Collectors.toList());
         List<String> allNames = Arrays.stream(options.valueParameters)
               .map(Parameter::getName).collect(Collectors.toList());
         selectSql = statements.getSelectStatement(keyNames, allNames);
         selectAllSql = statements.getSelectAllStatement(allNames);
         deleteSql = statements.getDeleteStatement(keyNames);
         deleteAllSql = statements.getDeleteAllStatement();
         upsertSql = statements.getUpsertStatement(keyNames, allNames);
         sizeSql = statements.getSizeCommand();
      }

      @Override
      public String getSelectRowSql() {
         return selectSql;
      }

      @Override
      public String getDeleteRowSql() {
         return deleteSql;
      }

      @Override
      public String getUpsertRowSql() {
         return upsertSql;
      }

      @Override
      public String getSelectAllSql(IntSet segments) {
         return selectAllSql;
      }

      @Override
      public String getDeleteAllSql() {
         return deleteAllSql;
      }

      @Override
      public String getSizeSql() {
         return sizeSql;
      }
   }

   @Override
   Parameter[] generateParameterInformation(TableJdbcStoreConfiguration config, ConnectionFactory connectionFactory)
         throws SQLException {
      String schemaAndTableName = config.tableName();
      String[] tableAndSchemaSplit = schemaAndTableName.split("\\.");
      String tableName;
      String schemaName;
      if (tableAndSchemaSplit.length == 1) {
         schemaName = null;
         tableName = schemaAndTableName;
      } else if (tableAndSchemaSplit.length == 2) {
         schemaName = tableAndSchemaSplit[0];
         tableName = tableAndSchemaSplit[1];
      } else {
         throw log.tableNotInCorrectFormat(schemaAndTableName);
      }

      Connection connection = connectionFactory.getConnection();
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      List<String> primaryKeyList = new ArrayList<>();
      try (ResultSet rs = databaseMetaData.getPrimaryKeys(null, schemaName, tableName)) {
         while (rs.next()) {
            primaryKeyList.add(rs.getString("COLUMN_NAME").toUpperCase());
         }
      }
      if (primaryKeyList.isEmpty()) {
         throw log.noPrimaryKeysFoundForTable(schemaAndTableName);
      }

      boolean containsNonPrimary = false;
      List<Parameter> parameters = new ArrayList<>();
      try (ResultSet rs = databaseMetaData.getColumns(null, schemaName, tableName, null)) {
         while (rs.next()) {
            String name = rs.getString("COLUMN_NAME");
            int sqlColumnType = rs.getInt("DATA_TYPE");
            int scale = rs.getInt("DECIMAL_DIGITS");
            int actualType = typeWeUse(sqlColumnType, rs.getString("TYPE_NAME"), scale);

            ProtostreamFieldType schemaType = ProtostreamFieldType.from(actualType);
            boolean isPrimary = primaryKeyList.contains(name.toUpperCase());
            parameters.add(new Parameter(name, schemaType, isPrimary, sqlColumnType));
            containsNonPrimary |= !isPrimary;
         }
      }
      if (!containsNonPrimary) {
         throw log.noValueColumnForTable(schemaAndTableName);
      }

      return parameters.toArray(new Parameter[0]);
   }

   @Override
   Parameter[] handleUnusedValueParams(Parameter[] parameters, List<Parameter> unusedValueParams) {
      // If it is a loader, we can ignore missing values as it is read only
      if (!config.ignoreModifications()) {
         throw unusedValueParamsException(unusedValueParams);
      }
      Log.CONFIG.debugf("TableJdbcStore has extra columns that are not part of the schema %s, ignoring since read only", unusedValueParams);
      Parameter[] newParams = new Parameter[parameters.length - unusedValueParams.size()];
      int i = 0;
      for (Parameter parameter : parameters) {
         if (!unusedValueParams.contains(parameter)) {
            newParams[i++] = parameter;
         }
      }
      return newParams;
   }
}
