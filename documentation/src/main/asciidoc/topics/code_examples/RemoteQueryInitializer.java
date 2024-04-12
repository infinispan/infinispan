import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoSchema;

@ProtoSchema(
      includeClasses = {
            Book.class
      },
      schemaFileName = "book.proto",
      schemaFilePath = "proto/",
      schemaPackageName = "book_sample")
public interface RemoteQueryInitializer extends SerializationContextInitializer {
}
