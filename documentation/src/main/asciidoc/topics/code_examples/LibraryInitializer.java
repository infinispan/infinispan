@AutoProtoSchemaBuilder(
      includeClasses = {
            Book.class,
            Author.class,
            UUIDAdapter.class,
            Language.class
      },
      schemaFileName = "library.proto",
      schemaFilePath = "proto/",
      schemaPackageName = "book_sample")
interface LibraryInitializer extends GeneratedSchema {
}
