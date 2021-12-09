ConfigurationBuilder builder = new ConfigurationBuilder();
builder.persistence().addStore(TableJdbcStoreConfigurationBuilder.class)
      .dialect(DatabaseType.H2)
      .shared("true")
      .tableName("authors")
      .schemaJdbcConfigurationBuilder()
         .messageName("Author")
         .packageName("library")
         .embeddedKey(true);
