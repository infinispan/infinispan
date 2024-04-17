import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;
import org.infinispan.protostream.GeneratedSchema;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;

@Indexed(keyEntity = "model.StructureKey")
public class Structure {

   private final String code;
   private final String description;
   private final Integer value;

   @ProtoFactory
   public Structure(String code, String description, Integer value) {
      this.code = code;
      this.description = description;
      this.value = value;
   }

   @ProtoField(1)
   @Basic
   public String getCode() {
      return code;
   }

   @ProtoField(2)
   @Text
   public String getDescription() {
      return description;
   }

   @ProtoField(3)
   @Basic
   public Integer getValue() {
      return value;
   }

   @ProtoSchema(includeClasses = { Structure.class, StructureKey.class }, schemaPackageName = "model")
   public interface StructureSchema extends GeneratedSchema {
      StructureSchema INSTANCE = new StructureSchemaImpl();
   }
}
