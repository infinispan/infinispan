@AutoProtoSchemaBuilder(
      includeClasses = {
            Book.class,
            Author.class,
      },
      schemaFileName = "library.proto",
      schemaFilePath = "proto/",
      schemaPackageName = "book_sample")
interface LibraryInitializer extends SerializationContextInitializer {
}