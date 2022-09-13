package org.infinispan.persistence.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.TableOperations;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.sql.configuration.QueriesJdbcConfiguration;
import org.infinispan.persistence.sql.configuration.QueriesJdbcConfigurationBuilder;
import org.infinispan.persistence.sql.configuration.QueriesJdbcStoreConfiguration;
import org.infinispan.persistence.sql.configuration.QueriesJdbcStoreConfigurationBuilder;

@ConfiguredBy(QueriesJdbcStoreConfigurationBuilder.class)
public class QueriesJdbcStore<K, V> extends AbstractSchemaJdbcStore<K, V, QueriesJdbcStoreConfiguration> {
   @Override
   protected TableOperations<K, V> actualCreateTableOperations(ProtoSchemaOptions<K, V, QueriesJdbcStoreConfiguration> options) {
      QueriesJdbcConfigurationBuilder<?> builder = new QueriesJdbcConfigurationBuilder<>(new ConfigurationBuilder().persistence().addStore(QueriesJdbcStoreConfigurationBuilder.class));
      QueriesJdbcConfiguration originalConfig = config.getQueriesJdbcConfiguration();
      builder.read(originalConfig);

      QueryNamedParameterParser.ParserResults selectResults = QueryNamedParameterParser.parseSqlStatement(originalConfig.select());
      builder.select(selectResults.getSqlToUse());

      if (config.ignoreModifications()) {
         return new QueryTableOperations(options, null, builder.create());
      }
      QueryNamedParameterParser.ParserResults deleteResults = QueryNamedParameterParser.parseSqlStatement(originalConfig.delete());
      builder.delete(deleteResults.getSqlToUse());

      // Delete all should not have any parameters
      if (QueryNamedParameterParser.parseSqlStatement(originalConfig.deleteAll()).getOrderedParameters().size() > 0) {
         throw log.deleteAllCannotHaveParameters(config.getQueriesJdbcConfiguration().selectAll());
      }

      // Size should not have any parameters
      if (QueryNamedParameterParser.parseSqlStatement(originalConfig.size()).getOrderedParameters().size() > 0) {
         throw log.sizeCannotHaveParameters(config.getQueriesJdbcConfiguration().selectAll());
      }

      // This ensures that delete and select parameters match, so we only need on instance of key parameters for both
      if (!deleteResults.getOrderedParameters().equals(selectResults.getOrderedParameters())) {
         throw log.deleteAndSelectQueryMismatchArguments(deleteResults.getOrderedParameters(), selectResults.getOrderedParameters());
      }

      // (e.g.) INSERT INTO books (isbn, title) VALUES (:key, :value) ON CONFLICT (isbn) DO UPDATE SET title = :value
      QueryNamedParameterParser.ParserResults upsertResults = QueryNamedParameterParser.parseSqlStatement(
            originalConfig.upsert());
      builder.upsert(upsertResults.getSqlToUse());

      Map<String, Parameter> parameterMap = new HashMap<>();
      // This includes all the keys as well
      for (Parameter parameter : options.valueParameters) {
         parameterMap.put(parameter.getName().toUpperCase(), parameter);
      }

      Parameter[] upsertParameters = upsertResults.getOrderedParameters().stream().map(name -> {
         Parameter param = parameterMap.get(name.toUpperCase());
         if (param == null) {
            throw log.deleteAndSelectQueryMismatchArguments(name, originalConfig.upsert(), originalConfig.selectAll());
         }
         return param;
      }).toArray(Parameter[]::new);

      return new QueryTableOperations(options, upsertParameters, builder.create());
   }

   @Override
   Parameter[] generateParameterInformation(QueriesJdbcStoreConfiguration config, ConnectionFactory connectionFactory)
         throws SQLException {
      QueryNamedParameterParser.ParserResults parserResults = QueryNamedParameterParser.parseSqlStatement(
            config.getQueriesJdbcConfiguration().selectAll());
      if (parserResults.getOrderedParameters().size() > 0) {
         throw log.selectAllCannotHaveParameters(config.getQueriesJdbcConfiguration().selectAll());
      }
      String selectAllSql = parserResults.getSqlToUse();
      String[] keyColumns = config.keyColumns().split(",");
      int keyCount = keyColumns.length;

      Map<String, Parameter> namedParams = new HashMap<>();

      Connection connection = connectionFactory.getConnection();
      try (PreparedStatement ps = connection.prepareStatement(selectAllSql)) {
         // Only retrieve 1 - we can't do 0 as this means use default
         ps.setFetchSize(1);
         try (ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData rsMetadata = rs.getMetaData();
            Parameter[] parameters = new Parameter[rsMetadata.getColumnCount()];
            for (int i = 1; i <= rsMetadata.getColumnCount(); ++i) {
               int columnType = rsMetadata.getColumnType(i);
               String name = rsMetadata.getColumnName(i);
               int scale = rsMetadata.getScale(i);
               int actualType = typeWeUse(columnType, rsMetadata.getColumnTypeName(i), scale);
               ProtostreamFieldType type = ProtostreamFieldType.from(actualType);
               String lowerCaseName = name.toLowerCase();
               // Make sure to reuse same parameter instance just with different offset
               Parameter parameter = namedParams.get(lowerCaseName);
               if (parameter == null) {
                  boolean primaryIdentifier = isPresent(keyColumns, name);
                  if (primaryIdentifier) {
                     keyCount--;
                  }
                  parameter = new Parameter(name.toLowerCase(), type, primaryIdentifier, columnType);
                  namedParams.put(lowerCaseName, parameter);
               }
               // TODO: what if the schema is in camel case?
               parameters[i - 1] = parameter;
            }
            if (keyCount != 0) {
               throw log.keyColumnsNotReturnedFromSelectAll(Arrays.toString(keyColumns),
                     config.getQueriesJdbcConfiguration().selectAll());
            }
            return parameters;
         }
      } finally {
         connectionFactory.releaseConnection(connection);
      }
   }

   @Override
   protected Parameter[] determinePrimaryParameters(QueriesJdbcStoreConfiguration config, Parameter[] allParameters) {
      QueryNamedParameterParser.ParserResults selectResults = QueryNamedParameterParser.parseSqlStatement(
            config.getQueriesJdbcConfiguration().select());

      return selectResults.getOrderedParameters().stream().map(name -> {
         for (Parameter parameter : allParameters) {
            if (parameter.getName().equals(name)) {
               return parameter;
            }
         }
         throw log.namedParamNotReturnedFromSelect(name, config.getQueriesJdbcConfiguration().selectAll(),
               config.getQueriesJdbcConfiguration().select());
      }).toArray(Parameter[]::new);
   }

   private static boolean isPresent(String[] array, String value) {
      for (String s : array) {
         // TODO: some DBs may not be case sensitive?
         if (s.trim().equalsIgnoreCase(value)) {
            return true;
         }
      }
      return false;
   }

   public class QueryTableOperations extends SchemaTableOperations<K, V, QueriesJdbcStoreConfiguration> {
      private final QueriesJdbcConfiguration modifiedQueryConfig;

      public QueryTableOperations(ProtoSchemaOptions<K, V, QueriesJdbcStoreConfiguration> options, Parameter[] upsertParameters,
            QueriesJdbcConfiguration modifiedQueryConfig) {
         super(options, upsertParameters);
         this.modifiedQueryConfig = modifiedQueryConfig;
      }

      @Override
      public String getSelectRowSql() {
         return modifiedQueryConfig.select();
      }

      @Override
      public String getSelectAllSql(IntSet segments) {
         return modifiedQueryConfig.selectAll();
      }

      @Override
      public String getDeleteRowSql() {
         return modifiedQueryConfig.delete();
      }

      @Override
      public String getUpsertRowSql() {
         return modifiedQueryConfig.upsert();
      }

      @Override
      public String getDeleteAllSql() {
         return modifiedQueryConfig.deleteAll();
      }

      @Override
      public String getSizeSql() {
         return modifiedQueryConfig.size();
      }
   }
}
