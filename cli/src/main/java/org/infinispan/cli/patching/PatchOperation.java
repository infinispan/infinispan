package org.infinispan.cli.patching;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class PatchOperation implements JsonSerialization {

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

   public static PatchOperation fromJson(Json json) {
      Action action = Action.valueOf(json.at(ACTION).asString());
      switch (action) {
         case ADD:
            return add(Paths.get(json.at(NEW_PATH).asString()), json.at(NEW_DIGEST).asString(), json.at(NEW_PERMISSIONS).asString());
         case REMOVE:
            return remove(Paths.get(json.at(PATH).asString()), json.at(DIGEST).asString(), json.at(PERMISSIONS).asString());
         case HARD_REPLACE:
            return replace(false, Paths.get(json.at(PATH).asString()), json.at(DIGEST).asString(), json.at(PERMISSIONS).asString(), json.at(NEW_DIGEST).asString(), json.at(NEW_PERMISSIONS).asString());
         case SOFT_REPLACE:
            return replace(true, Paths.get(json.at(PATH).asString()), json.at(DIGEST).asString(), json.at(PERMISSIONS).asString(), json.at(NEW_DIGEST).asString(), json.at(NEW_PERMISSIONS).asString());
         case UPGRADE:
            return upgrade(Paths.get(json.at(PATH).asString()), json.at(DIGEST).asString(), json.at(PERMISSIONS).asString(), Paths.get(json.at(NEW_PATH).asString()), json.at(NEW_DIGEST).asString(), json.at(NEW_PERMISSIONS).asString());
         default:
            throw new IllegalArgumentException(action.name());
      }
   }


   @Override
   public Json toJson() {
      Json result = Json.object().set(ACTION, action.name());
      if (path != null) {
         result.set(PATH, path.toString());
         result.set(DIGEST, digest);
         result.set(PERMISSIONS, permissions);
      }
      if (newPath != null) {
         result.set(NEW_PATH, newPath.toString());
      }
      if (newDigest != null) {
         result.set(NEW_DIGEST, newDigest);
      }
      if (newPermissions != null) {
         result.set(NEW_PERMISSIONS, newPermissions);
      }
      return result;
   }
}
