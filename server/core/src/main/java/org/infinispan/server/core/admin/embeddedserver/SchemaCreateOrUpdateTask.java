
package org.infinispan.server.core.admin.embeddedserver;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.internal.InternalCacheNames;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Creates or updates a ProtoStream schema in the server.
 *
 * Parameters are:
 * <ul>
 *    <li>name: schema name</li>
 *    <li>content: schema content</li>
 *    <li>op: operation type. 'c' create, 'u' update, 's' save</li>
 *    <li>force: bypass any version check in update or save</li>
 * </ul>
 *
 * Returns the operation done:
 * 'n' - Nothing has been done
 * 'c' - Created
 * 'u' - Updated
 *
 * The schema may have an error.
 * The output may be c;error in creation,
 * u;error in update and n;error if nothing has been done.
 *
 * @since 16.0
 */
public class SchemaCreateOrUpdateTask extends AdminServerTask<byte[]> {
   private static final String NAME = "name";
   private static final String CONTENT = "content";
   private static final String OP = "op";
   private static final String FORCE = "force";
   private static final String C = "c";
   private static final String U = "u";
   private static final String S = "s";
   private static final String N = "n";
   private static final String F = "f";
   private static final Set<String> PARAMETERS = Set.of(NAME, CONTENT, OP, FORCE);

   @Override
   public String getTaskContextName() {
      return "schemas";
   }

   @Override
   public String getTaskOperationName() {
      return "createOrUpdate";
   }

   @Override
   public Set<String> getParameters() {
      return PARAMETERS;
   }

   @Override
   protected byte[] execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      String schemaName = requireParameter(parameters, NAME);
      String schemaContent = requireParameter(parameters, CONTENT);
      String op = requireParameter(parameters, OP);
      boolean force = F.equals(getParameter(parameters, FORCE));
      AdvancedCache<String, String> schemasCache = cacheManager.<String, String>getCache(InternalCacheNames.PROTOBUF_METADATA_CACHE_NAME).getAdvancedCache();
      return (switch (op) {
         case C -> create(schemasCache, schemaName, schemaContent);
         case U -> update(schemasCache, schemaName, schemaContent, force);
         case S -> save(schemasCache, schemaName, schemaContent, force);
         default -> "";
      }).getBytes(StandardCharsets.UTF_8);
   }

   private String create(AdvancedCache<String, String> schemas, String schemaName, String schemaContent) {
      String prevValue = schemas.putIfAbsent(schemaName, schemaContent);

      // Nothing has been done, schema already exists
      if (prevValue != null) {
         return N;
      }

      // Return error if such exist
      return appendError(schemas, schemaName, C);
   }

   private String update(AdvancedCache<String, String> schemas,
                         String schemaName,
                         String schemaContent,
                         boolean force) {
      if (force) {
         if (schemas.replace(schemaName, schemaContent) == null) {
            // Nothing has been done, schema does not exist
            return N;
         }

         // Return error if such exist
         return appendError(schemas, schemaName, U);
      }

      return update(schemas, schemaName, schemaContent, schemas.getCacheEntry(schemaName));
   }

   private String update(AdvancedCache<String, String> schemas,
                         String schemaName,
                         String schemaContent,
                         CacheEntry<String, String> existingEntry) {
      String result = N;
      if (existingEntry == null) {
         return result;
      }

      // Replace if the actual value is still the value we had or if the content has changed.
      String actualSchemaContent = existingEntry.getValue();
         if (!actualSchemaContent.equals(schemaContent) && schemas.replace(schemaName, actualSchemaContent, schemaContent)) {
            result = U;
         }
      return appendError(schemas, schemaName, result);
   }

   private String save(AdvancedCache<String, String> schemas, String schemaName, String schemaContent, boolean force) {
      String result;

      if (force) {
         // Create or Update in every case
         String prevValue = schemas.put(schemaName, schemaContent);
         result = prevValue == null ? C : U;
         // Return error if such exist
         return appendError(schemas, schemaName, result);
      }

      CacheEntry<String, String> cacheEntry = schemas.getAdvancedCache().getCacheEntry(schemaName);
      if (cacheEntry == null) {
         // We create, and if someone else has created ot meanwhile, nothing happens
         return create(schemas, schemaName, schemaContent);
      }

      return update(schemas, schemaName, schemaContent, cacheEntry);
   }

   private static String appendError(AdvancedCache<String, String> schemas, String schemaName, String result) {
      String errorKey = schemaName + ERRORS_KEY_SUFFIX;
      String error = schemas.get(errorKey);
      return error == null ? result : result + ";" + error;
   }
}
