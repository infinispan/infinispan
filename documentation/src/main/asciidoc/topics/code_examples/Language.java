import org.infinispan.protostream.annotations.ProtoEnumValue;

public enum Language {
  @ProtoEnumValue(number = 0, name = "EN")
  ENGLISH,
  @ProtoEnumValue(number = 1, name = "DE")
  GERMAN,
  @ProtoEnumValue(number = 2, name = "IT")
  ITALIAN,
  @ProtoEnumValue(number = 3, name = "ES")
  SPANISH,
  @ProtoEnumValue(number = 4, name = "FR")
  FRENCH;

}
