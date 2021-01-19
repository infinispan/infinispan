@ProtoAdapter(UUID.class)
public class UUIDAdapter {

   @ProtoFactory
   UUID create(Long mostSigBitsFixed, Long leastSigBitsFixed) {
         return new UUID(mostSigBitsFixed, leastSigBitsFixed);
   }

   @ProtoField(number = 1, type = Type.FIXED64, defaultValue = "0")
   Long getMostSigBitsFixed(UUID uuid) {
      return uuid.getMostSignificantBits();
   }

   @ProtoField(number = 2, type = Type.FIXED64, defaultValue = "0")
   Long getLeastSigBitsFixed(UUID uuid) {
      return uuid.getLeastSignificantBits();
   }
}
