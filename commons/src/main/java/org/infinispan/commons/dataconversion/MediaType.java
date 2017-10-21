package org.infinispan.commons.dataconversion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;


/**
 * Represent a content type with optional parameters.
 *
 * @since 9.2
 */
@SerializeWith(value = MediaType.MediaTypeExternalizer.class)
public final class MediaType {

   public static final String APPLICATION_JSON_TYPE = "application/json";
   public static final String APPLICATION_OCTET_STREAM_TYPE = "application/octet-stream";
   public static final String APPLICATION_OBJECT_TYPE = "application/x-java-object";
   public static final String APPLICATION_PDF_TYPE = "application/pdf";
   public static final String APPLICATION_RTF_TYPE = "application/rtf";
   public static final String APPLICATION_SERIALIZED_OBJECT_TYPE = "application/x-java-serialized-object";
   public static final String APPLICATION_XML_TYPE = "application/xml";
   public static final String APPLICATION_ZIP_TYPE = "application/zip";
   public static final String APPLICATION_JBOSS_MARSHALLING_TYPE = "application/x-jboss-marshalling";
   public static final String APPLICATION_PROTOSTREAM_TYPE = "application/x-protostream";
   public static final String IMAGE_GIF_TYPE = "image/gif";
   public static final String IMAGE_JPEG_TYPE = "image/jpeg";
   public static final String IMAGE_PNG_TYPE = "image/png";
   public static final String TEXT_CSS_TYPE = "text/css";
   public static final String TEXT_CSV_TYPE = "text/csv";
   public static final String TEXT_PLAIN_TYPE = "text/plain";
   public static final String TEXT_HTML_TYPE = "text/html";
   public static final String APPLICATION_INFINISPAN_MARSHALLING_TYPE = "application/x-infinispan-marshalling";
   public static final String APPLICATION_INFINISPAN_BINARY_TYPE = "application/x-infinispan-binary";
   public static final String APPLICATION_PROTOSTUFF_TYPE = "application/x-protostuff";
   public static final String APPLICATION_KRYO_TYPE = "application/x-kryo";

   public static MediaType APPLICATION_JSON = fromString(APPLICATION_JSON_TYPE);
   public static MediaType APPLICATION_OCTET_STREAM = fromString(APPLICATION_OCTET_STREAM_TYPE);
   public static MediaType APPLICATION_OBJECT = fromString(APPLICATION_OBJECT_TYPE);
   public static MediaType APPLICATION_SERIALIZED_OBJECT = fromString(APPLICATION_SERIALIZED_OBJECT_TYPE);
   public static MediaType APPLICATION_XML = fromString(APPLICATION_XML_TYPE);
   public static MediaType APPLICATION_PROTOSTREAM = fromString(APPLICATION_PROTOSTREAM_TYPE);
   public static MediaType APPLICATION_JBOSS_MARSHALLED = fromString(APPLICATION_JBOSS_MARSHALLING_TYPE);
   public static MediaType APPLICATION_INFINISPAN_MARSHALLED = fromString(APPLICATION_INFINISPAN_MARSHALLING_TYPE);
   public static MediaType IMAGE_PNG = fromString(IMAGE_PNG_TYPE);
   public static MediaType TEXT_PLAIN = fromString(TEXT_PLAIN_TYPE);
   public static MediaType TEXT_HTML = fromString(TEXT_HTML_TYPE);
   public static MediaType APPLICATION_PROTOSTUFF = fromString(APPLICATION_PROTOSTUFF_TYPE);
   public static MediaType APPLICATION_KRYO = fromString(APPLICATION_KRYO_TYPE);
   public static MediaType APPLICATION_INFINISPAN_BINARY = fromString(APPLICATION_INFINISPAN_BINARY_TYPE);

   private static final String INVALID_TOKENS = "()<>@,;:/[]?=\\\"";

   private final Map<String, String> params = new HashMap<>(2);
   private final String type;
   private final String subType;
   private final String typeSubtype;

   public MediaType(String type, String subtype, Map<String, String> params) {
      this(type, subtype);
      params.forEach(this.params::put);
   }

   public MediaType(String type, String subtype) {
      this.type = validate(type);
      this.subType = validate(subtype);
      this.typeSubtype = type + "/" + subtype;
   }

   public static MediaType fromString(String mediaType) {
      return parse(mediaType);
   }

   public static MediaType parse(String str) {
      if (str == null || str.isEmpty()) throw new EncodingException("MediaType cannot be empty or null!");
      if (str.indexOf('/') == -1)
         throw new EncodingException("MediaType must contain a type and a subtype separated by '/'");
      boolean hasParams = str.indexOf(';') != -1;
      if (!hasParams) {
         String[] types = str.split("/");
         return new MediaType(types[0].trim(), types[1].trim());

      }
      int paramSeparator = str.indexOf(';');
      String types = str.substring(0, paramSeparator);
      String params = str.substring(paramSeparator + 1);
      String[] typeSubType = types.split("/");
      Map<String, String> paramMap = parseParams(params);
      return new MediaType(typeSubType[0].trim(), typeSubType[1].trim(), paramMap);
   }

   private static Map<String, String> parseParams(String params) {
      Map<String, String> parsed = new HashMap<>();
      String[] parameters = params.split(";");

      for (String p : parameters) {
         if (!p.contains("=")) throw new EncodingException("Failed to parse MediaType: Invalid param description" + p);
         String[] nameValue = p.split("=");
         String paramName = nameValue[0].trim();
         String paramValue = nameValue[1].trim();
         boolean isQuoted = paramValue.startsWith("\"") || paramValue.startsWith("\'");
         String parsedValue = paramValue;
         if (isQuoted) {
            checkValidQuotes(paramValue);
            String quoted = nameValue[1].trim();
            parsedValue = quoted.substring(1, quoted.length() - 1);
         }
         parsed.put(validate(paramName), validate(parsedValue));
      }
      return parsed;
   }

   private static boolean checkStartAndEnd(String toCheck, char c) {
      return toCheck != null && toCheck.charAt(0) == c && toCheck.charAt(toCheck.length() - 1) == c;
   }

   private static void checkValidQuotes(String paramValue) {
      if (!checkStartAndEnd(paramValue, '\'') && !checkStartAndEnd(paramValue, '\"')) {
         throw new EncodingException("Unclosed param value quote");
      }
   }

   public boolean match(MediaType other) {
      return other != null &&
            other.type.equals(this.type) &&
            other.subType.equals(this.subType);
   }

   public String getTypeSubtype() {
      return typeSubtype;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MediaType mediaType = (MediaType) o;
      return Objects.equals(params, mediaType.params) &&
            Objects.equals(type, mediaType.type) &&
            Objects.equals(subType, mediaType.subType);
   }

   @Override
   public int hashCode() {
      return Objects.hash(params, type, subType);
   }

   public String getType() {
      return type;
   }

   public String getSubType() {
      return subType;
   }

   public boolean hasParameters() {
      return !params.isEmpty();
   }

   public Optional<String> getParameter(String name) {
      return Optional.ofNullable(params.get(name));
   }

   private static String validate(String token) {
      for (char c : token.toCharArray()) {
         if (c < 0x20 || c > 0x7F || INVALID_TOKENS.indexOf(c) > 0) {
            throw new EncodingException("Invalid character '" + c + "' found in token '" + token + "'");
         }
      }
      return token;
   }

   @Override
   public String toString() {
      StringBuilder builder = new StringBuilder().append(type).append('/').append(subType);
      if (hasParameters()) {
         builder.append("; ");
         int i = 0;
         for (Map.Entry<String, String> param : params.entrySet()) {
            builder.append(param.getKey()).append("=").append(param.getValue());
            if (i++ < params.size() - 1) builder.append("; ");
         }
      }
      return builder.toString();
   }

   public static final class MediaTypeExternalizer implements Externalizer<MediaType> {
      @Override
      public void writeObject(ObjectOutput output, MediaType mediaType) throws IOException {
         String type = mediaType.type + "/" + mediaType.subType;
         Short id = MediaTypeIds.getId(type);
         if (id == null) {
            output.writeBoolean(false);
            output.writeUTF(mediaType.type);
            output.writeUTF(mediaType.subType);
            output.writeObject(mediaType.params);
         } else {
            output.writeBoolean(true);
            output.writeShort(id);
            output.writeObject(mediaType.params);
         }
      }

      @Override
      @SuppressWarnings("unchecked")
      public MediaType readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         boolean isInternal = input.readBoolean();
         if (isInternal) {
            short id = input.readShort();
            MediaType mediaType = MediaType.fromString(MediaTypeIds.getMediaType(id));
            Map<String, String> params = (Map<String, String>) input.readObject();
            params.forEach(mediaType.params::put);
            return mediaType;
         } else {
            String type = input.readUTF();
            String subType = input.readUTF();
            Map<String, String> params = (Map<String, String>) input.readObject();
            return new MediaType(type, subType, params);
         }
      }
   }

}
