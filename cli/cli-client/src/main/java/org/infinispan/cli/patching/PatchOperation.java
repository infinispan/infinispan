package org.infinispan.cli.patching;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class PatchOperation {

   public static final String ACTION = "action";
   public static final String PATH = "path";
   public static final String DIGEST = "digest";
   public static final String PERMISSIONS = "permissions";
   public static final String NEW_PATH = "new-path";
   public static final String NEW_DIGEST = "new-digest";
   public static final String NEW_PERMISSIONS = "new-permissions";

   enum Action {
      ADD,
      REMOVE,
      UPGRADE,
      HARD_REPLACE,
      SOFT_REPLACE
   }

   private final Action action;
   private final Path path;
   private final String digest;
   private final String permissions;
   private final Path newPath;
   private final String newDigest;
   private final String newPermissions;

   public static PatchOperation add(Path path, String digest, String permissions) {
      return new PatchOperation(Action.ADD, null, null, null, path, digest, permissions);
   }

   public static PatchOperation remove(Path path, String digest, String permissions) {
      return new PatchOperation(Action.REMOVE, path, digest, permissions, null, null, null);
   }

   public static PatchOperation upgrade(Path path, String digest, String permissions, Path newPath, String newDigest, String newPermissions) {
      return new PatchOperation(Action.UPGRADE, path, digest, permissions, newPath, newDigest, newPermissions);
   }

   public static PatchOperation replace(boolean soft, Path path, String digest, String permissions, String newDigest, String newPermissions) {
      return new PatchOperation(soft ? Action.SOFT_REPLACE : Action.HARD_REPLACE, path, digest, permissions, path, newDigest, newPermissions);
   }

   private PatchOperation(Action action, Path path, String digest, String permissions, Path newPath, String newDigest, String newPermissions) {
      this.action = action;
      this.path = path;
      this.digest = digest;
      this.permissions = permissions;
      this.newPath = newPath;
      this.newDigest = newDigest;
      this.newPermissions = newPermissions;
   }

   public Action getAction() {
      return action;
   }

   public Path getPath() {
      return path;
   }

   public String getDigest() {
      return digest;
   }

   public String getPermissions() {
      return permissions;
   }

   public Path getNewPath() {
      return newPath;
   }

   public String getNewDigest() {
      return newDigest;
   }

   public String getNewPermissions() {
      return newPermissions;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder(action.name());
      if (path != null) {
         sb.append(" ").append(path).append(" [").append(digest).append("] ").append(permissions);
      }
      if (newPath != null) {
         sb.append(" -> ").append(newPath).append(" [").append(newDigest).append("] ").append(newPermissions);
      }
      return sb.toString();
   }

   public static class PatchOperationSerializer extends StdSerializer<PatchOperation> {

      public PatchOperationSerializer() {
         this(null);
      }

      public PatchOperationSerializer(Class<PatchOperation> t) {
         super(t);
      }

      @Override
      public void serialize(PatchOperation operation, JsonGenerator json, SerializerProvider serializerProvider) throws IOException {
         json.writeStartObject();
         json.writeStringField(ACTION, operation.action.name());
         if (operation.path != null) {
            json.writeStringField(PATH, operation.path.toString());
            json.writeStringField(DIGEST, operation.digest);
            json.writeStringField(PERMISSIONS, operation.permissions);
         }
         if (operation.newPath != null) {
            json.writeStringField(NEW_PATH, operation.newPath.toString());
         }
         if (operation.newDigest != null) {
            json.writeStringField(NEW_DIGEST, operation.newDigest);
         }
         if (operation.newPermissions != null) {
            json.writeStringField(NEW_PERMISSIONS, operation.newPermissions);
         }
         json.writeEndObject();
      }
   }

   public static class PatchOperationDeserializer extends JsonDeserializer<PatchOperation> {

      @Override
      public PatchOperation deserialize(JsonParser parser, DeserializationContext ctx) throws IOException {
         JsonNode node = parser.getCodec().readTree(parser);
         Action action = Action.valueOf(node.get(ACTION).asText());
         switch (action) {
            case ADD:
               return add(Paths.get(node.get(NEW_PATH).asText()), node.get(NEW_DIGEST).asText(), node.get(NEW_PERMISSIONS).asText());
            case REMOVE:
               return remove(Paths.get(node.get(PATH).asText()), node.get(DIGEST).asText(), node.get(PERMISSIONS).asText());
            case HARD_REPLACE:
               return replace(false, Paths.get(node.get(PATH).asText()), node.get(DIGEST).asText(), node.get(PERMISSIONS).asText(), node.get(NEW_DIGEST).asText(), node.get(NEW_PERMISSIONS).asText());
            case SOFT_REPLACE:
               return replace(true, Paths.get(node.get(PATH).asText()), node.get(DIGEST).asText(), node.get(PERMISSIONS).asText(), node.get(NEW_DIGEST).asText(), node.get(NEW_PERMISSIONS).asText());
            case UPGRADE:
               return upgrade(Paths.get(node.get(PATH).asText()), node.get(DIGEST).asText(), node.get(PERMISSIONS).asText(), Paths.get(node.get(NEW_PATH).asText()), node.get(NEW_DIGEST).asText(), node.get(NEW_PERMISSIONS).asText());
            default:
               throw new IllegalArgumentException(action.name());
         }
      }
   }
}
