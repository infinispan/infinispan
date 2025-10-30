// create or obtain your RemoteCacheManager
RemoteCacheManager manager = ...;
RemoteSchemasAdmin schemasAdmin = manager.administration().schemas();

Schema schema = Schema.buildFromStringContent("schema.proto", "message M { optional string json_key = 1; }");

// Create or Update schema - non-blocking
schemasAdmin.createOrUpdateAsync(schema);

// Create or Update schema - blocking
RemoteSchemasAdmin.SchemaOpResult result = schemasAdmin.createOrUpdate(schema);

// Prints 'true' if the schema validation reported an error
System.out.println(result.hasError());

// Prints an error if such exist
System.out.println(result.getError());

// Gets the op type : CREATE, UPDATE or NONE if nothing has been done.
System.out.println(result.getType());

// Prints an error if such exist
System.out.println(schemasAdmin.retrieveError("schema.proto"));

// Gets the schema if such exist
Optional<Schema> = schemasAdmin.get("schema.proto");

// Removes the schema if such exists
schemasAdmin.remove("schema.proto");
