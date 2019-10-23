import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.SerializationContextInitializer;
...

public class ManualSerializationContextInitializer implements SerializationContextInitializer {
   @Override
   public String getProtoFileName() {
      return "library.proto";
   }

   @Override
   public String getProtoFile() throws UncheckedIOException {
      // Assumes that the file is located in a Jar's resources, we must provide the path to the library.proto file
      return FileDescriptorSource.getResourceAsString(getClass(), "/" + getProtoFileName());
   }

   @Override
   public void registerSchema(SerializationContext serCtx) {
      serCtx.registerProtoFiles(FileDescriptorSource.fromString(getProtoFileName(), getProtoFile()));
   }

   @Override
   public void registerMarshallers(SerializationContext serCtx) {
      serCtx.registerMarshaller(new AuthorMarshaller());
      serCtx.registerMarshaller(new BookMarshaller());
   }
}