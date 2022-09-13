package org.infinispan.persistence.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.SerializationContextRegistry;
import org.infinispan.persistence.jdbc.common.TableOperations;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.BaseJdbcStore;
import org.infinispan.persistence.jdbc.common.sql.BaseTableOperations;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;
import org.infinispan.persistence.sql.configuration.AbstractSchemaJdbcConfiguration;
import org.infinispan.persistence.sql.configuration.SchemaJdbcConfiguration;
import org.infinispan.protostream.ImmutableSerializationContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.EnumDescriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.protostream.descriptors.GenericDescriptor;
import org.infinispan.protostream.descriptors.Type;

public abstract class AbstractSchemaJdbcStore<K, V, C extends AbstractSchemaJdbcConfiguration> extends BaseJdbcStore<K, V, C> {
   @Override
   protected TableOperations<K, V> createTableOperations(InitializationContext ctx, C config) throws SQLException {
      AdvancedCache<K, V> advancedCache = ctx.getCache().getAdvancedCache();
      // We use a type as the protostream -> json conversion leaves it as a String instead of byte[]
      MediaType jsonStringType = MediaType.fromString(MediaType.APPLICATION_JSON_TYPE + ";type=String");
      // This seems like a bug that `withRequestMediaType` isn't injected...
      DataConversion keyDataConversion = advancedCache.getKeyDataConversion()
            .withRequestMediaType(jsonStringType);
      DataConversion valueDataConversion = advancedCache.getValueDataConversion()
            .withRequestMediaType(jsonStringType);

      ComponentRegistry componentRegistry = advancedCache.getComponentRegistry();
      componentRegistry.wireDependencies(keyDataConversion, true);
      componentRegistry.wireDependencies(valueDataConversion, true);

      Parameter[] parameters = generateParameterInformation(config, connectionFactory);
      assert parameters.length != 0;
      Parameter[] primaryParameters = determinePrimaryParameters(config, parameters);
      assert primaryParameters.length != 0;
      assert Arrays.stream(primaryParameters).allMatch(Parameter::isPrimaryIdentifier);

      // We have to use the user serialization context as it will have the schemas they registered
      ImmutableSerializationContext serializationContext = componentRegistry.getComponent(SerializationContextRegistry.class).getUserCtx();

      ProtoSchemaOptions<K, V, C> options = verifySchemaAndCreateOptions(serializationContext,
            config.schema(), parameters, primaryParameters, keyDataConversion, valueDataConversion,
            ctx.getMarshallableEntryFactory());

      return actualCreateTableOperations(options);
   }

   protected Parameter[] determinePrimaryParameters(C config, Parameter[] allParameters) {
      return Arrays.stream(allParameters)
            .filter(Parameter::isPrimaryIdentifier)
            .toArray(Parameter[]::new);
   }

   /**
    * Implementation specific method to return a table operations which will then be used appropriately for store
    * operations. It is recommended to extend {@link SchemaTableOperations} providing ways to retrieve the statements
    * needed.
    *
    * @param schemaOptions the operations for the schema for this store
    * @return the operations object to use
    */
   protected abstract TableOperations<K, V> actualCreateTableOperations(ProtoSchemaOptions<K, V, C> schemaOptions);

   /**
    * Method to be overridden to determine what the parameters are for the various sql statements that will be used Only
    * the {@link #connectionFactory} will be initialized at this point
    *
    * @param config            store configuration object
    * @param connectionFactory connection factory to use
    * @return all the parameters for this table. This can include duplicate named columns
    * @throws SQLException exception if there is any problem determining the paramaters from the DB
    */
   abstract Parameter[] generateParameterInformation(C config, ConnectionFactory connectionFactory) throws SQLException;

   int typeWeUse(int sqlType, String typeName, int scale) {
      if (sqlType == Types.VARCHAR) {
         // Some DBs store VARBINARY as VARCHAR FOR BIT DATA (ahem... DB2)
         if (typeName.contains("BIT") || typeName.contains("BINARY")) {
            return Types.VARBINARY;
         }
      } else if (typeName.toUpperCase().startsWith("BOOL")) {
         // Some databases store as int32 or something similar but have the typename as BOOLEAN or some derivation
         return Types.BOOLEAN;
      } else if (sqlType == Types.NUMERIC && scale == 0) {
         // If scale is 0 we don't want to use float or double types
         return Types.INTEGER;
      }
      return sqlType;
   }

   ProtoSchemaOptions<K, V, C> verifySchemaAndCreateOptions(ImmutableSerializationContext ctx,
         SchemaJdbcConfiguration schemaJdbcConfiguration, Parameter[] parameters, Parameter[] primaryParameters,
         DataConversion keyConversion, DataConversion valueConversion, MarshallableEntryFactory<K, V> marshallableEntryFactory) {
      // Keys should all be upper case to provide case insensitivity
      Map<String, Parameter> parameterMap = new HashMap<>();
      int uniquePrimaryParameters = 0;
      // Load up a map of names to parameter while also tracking the number of unique primary identifiers
      for (Parameter parameter : parameters) {
         // We can have mixed cases for the characters so just force all upper case to allow for O(1)
         if (parameterMap.put(parameter.name.toUpperCase(), parameter) == null && parameter.primaryIdentifier) {
            uniquePrimaryParameters++;
         }
      }

      String packageName = schemaJdbcConfiguration.packageName();
      String keyMessageName = schemaJdbcConfiguration.keyMessageName();
      String fullKeyMessageName = null;
      // Only generate a schema for the key if there is more than 1 field or they explicitly defined one
      if (uniquePrimaryParameters != 1 || keyMessageName != null) {
         if (keyMessageName == null || packageName == null) {
            throw log.primaryKeyMultipleColumnWithoutSchema();
         }
         String fullMessageName = packageName + "." + keyMessageName;
         verifyParametersPresentForMessage(ctx, fullMessageName, parameterMap, true);
         fullKeyMessageName = fullMessageName;
      } else {
         updatePrimitiveJsonConsumer(primaryParameters[0], true);
      }
      String valueMessageName = schemaJdbcConfiguration.messageName();
      String fullValueMessageName = null;
      boolean hasEmbeddedKey = config.schema().embeddedKey();
      if (parameterMap.size() - (hasEmbeddedKey ? 0 : uniquePrimaryParameters) > 1 || valueMessageName != null) {
         if (valueMessageName == null || packageName == null) {
            throw log.valueMultipleColumnWithoutSchema();
         }
         String fullMessageName = packageName + "." + valueMessageName;
         verifyParametersPresentForMessage(ctx, fullMessageName, parameterMap, false);
         fullValueMessageName = fullMessageName;
      } else {
         // This variable is only for assertion - it should be that we can only have 1 non primary parameter,
         // but just in case
         boolean updatedPrimitive = false;
         for (Parameter parameter : parameters) {
            if (parameter.primaryIdentifier) {
               continue;
            }
            assert !updatedPrimitive;
            updatePrimitiveJsonConsumer(parameter, false);
            updatedPrimitive = true;
         }
      }

      List<Parameter> unusedValueParams = null;

      for (Parameter parameter : parameters) {
         if (parameter.jsonConsumerValue == null && parameter.jsonConsumerKey == null) {
            if (parameter.primaryIdentifier) {
               throw log.keyNotInSchema(parameter.name, fullKeyMessageName);
            } else {
               if (unusedValueParams == null) {
                  unusedValueParams = new ArrayList<>();
               }
               unusedValueParams.add(parameter);
            }
         }
      }

      if (unusedValueParams != null) {
         parameters = handleUnusedValueParams(parameters, unusedValueParams);
      }

      if (hasEmbeddedKey) {
         // Make sure all values are mapped as they must be when embedded key
         assert Arrays.stream(parameters).noneMatch(parameter -> parameter.unwrapJsonValue == null);
      } else {
         // Primary identifiers shouldn't have any values mapped as they aren't embedded
         assert Arrays.stream(parameters).noneMatch(parameter -> parameter.primaryIdentifier && parameter.unwrapJsonValue != null);
         assert Arrays.stream(parameters).noneMatch(parameter -> !parameter.primaryIdentifier && parameter.unwrapJsonValue == null);
      }

      assert Arrays.stream(parameters).filter(Parameter::isPrimaryIdentifier).noneMatch(parameter -> parameter.unwrapJsonKey == null);

      return new ProtoSchemaOptions<>(config, primaryParameters, fullKeyMessageName, parameters, fullValueMessageName,
            keyConversion, valueConversion, marshallableEntryFactory);
   }

   Parameter[] handleUnusedValueParams(Parameter[] parameters, List<Parameter> unusedValueParams) {
      throw unusedValueParamsException(unusedValueParams);
   }

   CacheConfigurationException unusedValueParamsException(List<Parameter> unusedParamNames) {
      return log.valueNotInSchema(unusedParamNames.stream().map(Parameter::getName).collect(Collectors.toList()),
            config.schema().messageName());
   }

   private void updatePrimitiveJsonConsumer(Parameter parameter, boolean key) {
      updateUnwrap(parameter, key, json -> json.at("_value"));
      updateJsonConsumer(parameter, key, (json, value) -> {
         json.set("_type", parameter.getType().protostreamType);
         json.set("_value", value);
      });
   }

   void verifyParametersPresentForMessage(ImmutableSerializationContext ctx, String fullTypeName, Map<String, Parameter> parameterMap, boolean key) {
      GenericDescriptor genericDescriptor;
      try {
         genericDescriptor = ctx.getDescriptorByName(fullTypeName);
      } catch (IllegalArgumentException t) {
         throw log.schemaNotFound(fullTypeName);
      }
      Set<String> seenNames = new HashSet<>();
      if (genericDescriptor instanceof Descriptor) {
         recursiveUpdateParameters((Descriptor) genericDescriptor, parameterMap, null, seenNames, key);
      } else if (genericDescriptor instanceof EnumDescriptor) {
         if (!key && config.schema().embeddedKey()) {
            throw log.keyCannotEmbedWithEnum(fullTypeName);
         }
         String name = genericDescriptor.getName();
         // treat an enum as just a string
         Parameter enumParam = parameterMap.get(name.toUpperCase());
         if (enumParam != null) {
            assert enumParam.getType() == ProtostreamFieldType.STRING;
            updateUnwrap(enumParam, key, json -> json.at("_value"));
            updateJsonConsumer(enumParam, key, (json, o) -> {
               json.set("_type", fullTypeName);
               json.set("_value", o);
            });
         }
      } else {
         throw new UnsupportedOperationException("Unsupported descriptor found " + genericDescriptor);
      }
   }

   void recursiveUpdateParameters(Descriptor descriptor, Map<String, Parameter> parameterMap,
         String[] nestedMessageNames, Set<String> seenNames, boolean key) {
      for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
         String name = fieldDescriptor.getName();
         if (fieldDescriptor.isRepeated()) {
            throw log.repeatedFieldsNotSupported(name, fieldDescriptor.getTypeName());
         }
         Descriptor fieldMessageDescriptor = fieldDescriptor.getMessageType();
         if (fieldMessageDescriptor != null) {
            String[] newNestedMessageNames;
            if (nestedMessageNames == null) {
               newNestedMessageNames = new String[1];
               newNestedMessageNames[0] = name;
            } else {
               newNestedMessageNames = Arrays.copyOf(nestedMessageNames, nestedMessageNames.length + 1);
               newNestedMessageNames[nestedMessageNames.length] = name;
            }
            recursiveUpdateParameters(fieldMessageDescriptor, parameterMap, newNestedMessageNames, seenNames, key);
            continue;
         }

         if (!seenNames.add(name)) {
            throw log.duplicateFieldInSchema(name, fieldDescriptor.getTypeName());
         }

         Parameter parameter = parameterMap.get(name.toUpperCase());
         if (parameter == null) {
            if (fieldDescriptor.isRequired()) {
               throw log.requiredSchemaFieldNotPresent(name, fieldDescriptor.getTypeName());
            }
            continue;
         }
         if (parameter.primaryIdentifier && !key && !config.schema().embeddedKey()) {
            throw log.primaryKeyPresentButNotEmbedded(parameter.name, fieldDescriptor.getTypeName());
         }

         Function<Json, Json> retrievalFunction;
         BiConsumer<Json, Object> valueConsumer;

         // Oracle doesn't have a boolean type, so use a number of 0 or 1 instead
         if (parameter.type == ProtostreamFieldType.INT_32 && fieldDescriptor.getType() == Type.BOOL) {
            retrievalFunction = json -> Json.factory().number(json.at(name).asBoolean() ? 1 : 0);
            valueConsumer = (json, o) -> json.set(name, ((Integer) o) == 1);
         } else {
            retrievalFunction = json -> json.at(name);
            valueConsumer = (json, o) -> json.set(name, o);
         }

         if (nestedMessageNames == null) {
            updateUnwrap(parameter, key, retrievalFunction);
            updateJsonConsumer(parameter, key, valueConsumer);
         } else {
            updateUnwrap(parameter, key, json -> {
               for (String nestedName : nestedMessageNames) {
                  json = json.at(nestedName);
                  if (json == null) return null;
               }
               return retrievalFunction.apply(json);
            });
            updateJsonConsumer(parameter, key, (json, o) -> {
               Json nestedJSon = json;
               for (String nestedName : nestedMessageNames) {
                  nestedJSon = json.at(nestedName);
                  if (nestedJSon == null) {
                     nestedJSon = Json.object();
                     json.set(nestedName, nestedJSon);
                  }
                  json = nestedJSon;
               }
               valueConsumer.accept(nestedJSon, o);
            });
         }
      }
   }

   private void updateUnwrap(Parameter parameter, boolean key, Function<Json, Json> function) {
      if (key) {
         parameter.unwrapJsonKey = function;
      } else {
         parameter.unwrapJsonValue = function;
      }
   }

   private void updateJsonConsumer(Parameter parameter, boolean key, BiConsumer<Json, Object> jsonBiConsumer) {
      if (key) {
         parameter.jsonConsumerKey = jsonBiConsumer;
      } else {
         parameter.jsonConsumerValue = jsonBiConsumer;
      }
   }

   protected enum ProtostreamFieldType {
      INT_32("int32"),
      INT_64("int64"),
      FLOAT("float"),
      DOUBLE("double"),
      BOOL("bool"),
      STRING("string"),
      BYTES("bytes"),
      DATE("fixed64");

      /**
       * This field matches {@link org.infinispan.protostream.impl.JsonUtils} types
       */
      private final String protostreamType;

      ProtostreamFieldType(String protostreamType) {
         this.protostreamType = protostreamType;
      }

      protected static ProtostreamFieldType from(int sqlType) {
         switch (sqlType) {
            case Types.INTEGER:
               return INT_32;
            case Types.BIGINT:
               return INT_64;
            case Types.FLOAT:
            case Types.REAL:
               return FLOAT;
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
               return DOUBLE;
            case Types.BIT:
            case Types.BOOLEAN:
               return BOOL;
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
               return STRING;
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
               return BYTES;
            case Types.DATE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
               return DATE;
            default:
               throw new IllegalArgumentException("SqlType not supported: " + sqlType);
         }
      }
   }

   protected static class Parameter {
      private final String name;
      private final ProtostreamFieldType type;
      private final boolean primaryIdentifier;
      private final int sqlType;
      private BiConsumer<Json, Object> jsonConsumerValue;
      private BiConsumer<Json, Object> jsonConsumerKey;
      private Function<Json, Json> unwrapJsonValue;
      private Function<Json, Json> unwrapJsonKey;

      Parameter(String name, ProtostreamFieldType type, boolean primaryIdentifier, int sqlType) {
         this.name = name;
         this.type = type;
         this.primaryIdentifier = primaryIdentifier;
         this.sqlType = sqlType;
      }

      public String getName() {
         return name;
      }

      public ProtostreamFieldType getType() {
         return type;
      }

      public int getSqlType() {
         return sqlType;
      }

      public boolean isPrimaryIdentifier() {
         return primaryIdentifier;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         Parameter parameter = (Parameter) o;
         return Objects.equals(name, parameter.name);
      }

      @Override
      public int hashCode() {
         return Objects.hash(name);
      }

      @Override
      public String toString() {
         return "Parameter{" +
               "name='" + name + '\'' +
               ", type=" + type +
               ", primaryIdentifier=" + primaryIdentifier +
               '}';
      }
   }

   protected static class ProtoSchemaOptions<K, V, C extends AbstractSchemaJdbcConfiguration> {
      protected final C config;
      protected final Parameter[] keyParameters;
      protected final String keyMessageName;
      protected final Parameter[] valueParameters;
      protected final String valueMessageName;
      protected final DataConversion keyConversion;
      protected final DataConversion valueConversion;
      protected final MarshallableEntryFactory<K, V> marshallableEntryFactory;

      public ProtoSchemaOptions(C config, Parameter[] keyParameters, String keyMessageName, Parameter[] valueParameters,
            String valueMessageName, DataConversion keyConversion, DataConversion valueConversion,
            MarshallableEntryFactory<K, V> marshallableEntryFactory) {
         this.config = config;
         this.keyParameters = keyParameters;
         this.keyMessageName = keyMessageName;
         this.valueParameters = valueParameters;
         this.valueMessageName = valueMessageName;
         this.keyConversion = keyConversion;
         this.valueConversion = valueConversion;
         this.marshallableEntryFactory = marshallableEntryFactory;
      }
   }

   protected abstract static class SchemaTableOperations<K, V, C extends AbstractSchemaJdbcConfiguration> extends BaseTableOperations<K, V> {
      private final ProtoSchemaOptions<K, V, C> schemaOptions;
      private final Parameter[] upsertParameters;

      public SchemaTableOperations(ProtoSchemaOptions<K, V, C> schemaOptions, Parameter[] upsertParameters) {
         super(schemaOptions.config.maxBatchSize(), schemaOptions.config.writeQueryTimeout(),
               schemaOptions.config.readQueryTimeout());
         this.schemaOptions = schemaOptions;
         this.upsertParameters = upsertParameters;
      }

      /**
       * This method assigns a parameter based on the type using the parameter type. Normally this code would live in
       * the enum, but some implementations may require a different assignment based on the database and thus this
       * method can be extended to change that behavior.
       *
       * @param ps
       * @param type
       * @param position
       * @param json
       */
      protected void setParameter(PreparedStatement ps, ProtostreamFieldType type, int position, Json json) throws SQLException {
         switch (type) {
            case INT_32:
               ps.setInt(position, json.asInteger());
               break;
            case INT_64:
               ps.setLong(position, json.asLong());
               break;
            case FLOAT:
               ps.setFloat(position, json.asFloat());
               break;
            case DOUBLE:
               ps.setDouble(position, json.asDouble());
               break;
            case BOOL:
               ps.setBoolean(position, json.asBoolean());
               break;
            case STRING:
               ps.setString(position, json.asString());
               break;
            case BYTES:
               String base64Bytes = json.asString();
               byte[] bytes = Base64.getDecoder().decode(base64Bytes);
               ps.setBytes(position, bytes);
               break;
            case DATE:
               long dateTime = json.asLong();
               ps.setTimestamp(position, new Timestamp(dateTime));
               break;
            default:
               throw new IllegalArgumentException("Type " + type + " not supported!");
         }
      }

      protected void updateJsonWithParameter(ResultSet rs, Parameter parameter, int offset, Json json, boolean key) throws SQLException {
         Object value;
         switch (parameter.getType()) {
            case INT_32:
               value = rs.getInt(offset);
               break;
            case INT_64:
               value = rs.getLong(offset);
               break;
            case FLOAT:
               value = rs.getFloat(offset);
               break;
            case DOUBLE:
               value = rs.getDouble(offset);
               break;
            case BOOL:
               value = rs.getBoolean(offset);
               break;
            case STRING:
               value = rs.getString(offset);
               break;
            case BYTES:
               byte[] bytes = rs.getBytes(offset);
               value = bytes != null ? Base64.getEncoder().encodeToString(bytes) : null;
               break;
            case DATE:
               Timestamp timestamp = rs.getTimestamp(offset);
               value = timestamp != null ? timestamp.getTime() : null;
               break;
            default:
               throw new IllegalArgumentException("Type " + parameter.getType() + " not supported!");
         }
         if (value != null) {
            if (key) {
               parameter.jsonConsumerKey.accept(json, value);
            } else {
               parameter.jsonConsumerValue.accept(json, value);
            }
         }
      }

      @Override
      protected MarshallableEntry<K, V> entryFromResultSet(ResultSet rs, Object keyIfProvided, boolean fetchValue,
            Predicate<? super K> keyPredicate) throws SQLException {
         Json keyJson = keyIfProvided == null ? Json.object() : null;
         if (keyJson != null && schemaOptions.keyMessageName != null) {
            keyJson.set("_type", schemaOptions.keyMessageName);
         }
         Json valueJson = Json.object();
         if (schemaOptions.valueMessageName != null) {
            valueJson.set("_type", schemaOptions.valueMessageName);
         }
         Parameter[] valueParameters = schemaOptions.valueParameters;
         for (int i = 0; i < valueParameters.length; ++i) {
            Parameter parameter = valueParameters[i];
            if (parameter.isPrimaryIdentifier()) {
               if (keyJson != null) {
                  updateJsonWithParameter(rs, parameter, i + 1, keyJson, true);
               }
               if (!schemaOptions.config.schema().embeddedKey()) {
                  continue;
               }
            }
            updateJsonWithParameter(rs, parameter, i + 1, valueJson, false);
         }
         if (keyJson != null) {
            keyIfProvided = schemaOptions.keyConversion.toStorage(keyJson.toString());
         }
         if (keyPredicate != null && !keyPredicate.test((K) keyIfProvided)) {
            return null;
         }
         Object value = schemaOptions.valueConversion.toStorage(valueJson.toString());

         return schemaOptions.marshallableEntryFactory.create(keyIfProvided, value);
      }

      @Override
      protected void prepareKeyStatement(PreparedStatement ps, Object key) throws SQLException {
         Object jsonString = schemaOptions.keyConversion.fromStorage(key);
         Json json = Json.read((String) jsonString);
         for (int i = 0; i < schemaOptions.keyParameters.length; ++i) {
            Parameter parameter = schemaOptions.keyParameters[i];
            if (!parameter.primaryIdentifier) {
               continue;
            }
            Json innerJson = parameter.unwrapJsonKey.apply(json);
            if (innerJson != null) {
               setParameter(ps, parameter.getType(), i + 1, innerJson);
            } else {
               ps.setNull(i + 1, parameter.getSqlType());
            }
         }
      }

      @Override
      protected void prepareValueStatement(PreparedStatement ps, int segment, MarshallableEntry<? extends K, ? extends V> entry) throws SQLException {
         boolean embeddedKey = schemaOptions.config.schema().embeddedKey();
         Json valueJson = Json.read((String) schemaOptions.valueConversion.fromStorage(entry.getValue()));
         Json keyJson = embeddedKey ? valueJson : Json.read((String) schemaOptions.keyConversion.fromStorage(entry.getKey()));
         for (int i = 0; i < upsertParameters.length; ++i) {
            Parameter parameter = upsertParameters[i];
            Json json;
            if (parameter.primaryIdentifier) {
               json = embeddedKey ? parameter.unwrapJsonValue.apply(keyJson) : parameter.unwrapJsonKey.apply(keyJson);
            } else {
               json = parameter.unwrapJsonValue.apply(valueJson);
            }
            if (json != null) {
               setParameter(ps, parameter.getType(), i + 1, json);
            } else {
               ps.setNull(i + 1, parameter.getSqlType());
            }
         }
      }
   }
}
