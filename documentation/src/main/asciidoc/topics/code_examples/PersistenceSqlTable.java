ConfigurationBuilder builder = new ConfigurationBuilder();
builder.persistence().addStore(TableJdbcStoreConfigurationBuilder.class)
      .dialect(DatabaseType.H2)
      .shared("true")
      .tableName("books")
      .schemaJdbcConfigurationBuilder()
        .messageName("books_value")
        .packageName("library");
