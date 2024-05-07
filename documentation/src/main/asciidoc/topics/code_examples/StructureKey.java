import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Keyword;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

@Indexed
public class StructureKey {

   private String zone;
   private Integer row;
   private Integer column;

   @ProtoFactory
   public StructureKey(String zone, Integer row, Integer column) {
      this.zone = zone;
      this.row = row;
      this.column = column;
   }

   @Keyword(projectable = true, sortable = true)
   @ProtoField(1)
   public String getZone() {
      return zone;
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(2)
   public Integer getRow() {
      return row;
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(3)
   public Integer getColumn() {
      return column;
   }
}
