@AutoProtoSchemaBuilder(
      includeClasses = {
            Book.class,
            Author.class,
            UUIDAdapter.class,
      },
      schemaFileName = "library.proto", <1>
      schemaFilePath = "proto/", <2>
      schemaPackageName = "book_sample")
interface LibraryInitializer extends SerializationContextInitializer {
}
