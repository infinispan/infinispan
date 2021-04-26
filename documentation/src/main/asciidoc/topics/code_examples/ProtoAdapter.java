import java.util.UUID;

import org.infinispan.protostream.annotations.ProtoAdapter;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.descriptors.Type;

/**
 * Human readable UUID adapter for UUID marshalling
 */
@ProtoAdapter(UUID.class)
public class UUIDAdapter {

  @ProtoFactory
  UUID create(String stringUUID) {
    return UUID.fromString(stringUUID);
  }

  @ProtoField(1)
  String getStringUUID(UUID uuid) {
    return uuid.toString();
  }
}
