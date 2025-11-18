package org.infinispan.server.resp.json;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.security.actions.SecurityActions;

/**
 * A cache implementation for JSON data, providing various methods for interacting with and
 * manipulating JSON objects, arrays, and values. This class includes methods for setting,
 * retrieving, and querying JSON data in an embedded cache.
 *
 * <p>
 * Note: The implementation provides a set of functionalities for handling JSON objects, including
 * operations like recursively extracting values, checking types, and working with specific paths.
 * </p>
 *
 * @author Vittorio Rigamonti
 * @author Katia Aresti
 * @since 15.2
 */
public class EmbeddedJsonCache {
   public static final String ERR_KEY_CAN_T_BE_NULL = "key can't be null";
   public static final String ERR_VALUE_CAN_T_BE_NULL = "value can't be null";
   protected final FunctionalMap.ReadWriteMap<byte[], JsonBucket> readWriteMap;
   protected final AdvancedCache<byte[], JsonBucket> cache;
   protected final InternalEntryFactory entryFactory;

   public EmbeddedJsonCache(Cache<byte[], JsonBucket> cache) {
      this.cache = cache.getAdvancedCache();
      FunctionalMapImpl<byte[], JsonBucket> functionalMap = FunctionalMapImpl.create(this.cache);
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
      this.entryFactory = SecurityActions.getCacheComponentRegistry(this.cache).getInternalEntryFactory().running();
   }

   /**
    * Retrieves the JSON value at the specified paths within the given key. The resulting JSON
    * content can be formatted with the provided spacing, newline, and indentation settings.
    *
    * @param key
    *           The key from which the JSON value will be retrieved, represented as a byte array.
    * @param paths
    *           A list of JSON paths used to access specific values within the JSON, each
    *           represented as a byte array.
    * @param space
    *           The byte array used to represent spaces for formatting the JSON output.
    * @param newline
    *           The byte array used to represent newline characters for formatting the JSON output.
    * @param indent
    *           The byte array used to represent indentation characters for formatting the JSON
    *           output.
    * @return A {@link CompletionStage} containing the formatted JSON content as a byte array.
    */
   public CompletionStage<byte[]> get(byte[] key, List<byte[]> paths, byte[] space, byte[] newline, byte[] indent) {
      return readWriteMap.eval(key, new JsonGetFunction(paths, space, newline, indent));
   }

   /**
    * Sets a JSON value at the specified path in the given key.
    *
    * @param key
    *           The key in which the JSON value should be stored, represented as a byte array.
    * @param value
    *           The JSON value to set, represented as a byte array.
    * @param path
    *           The JSON path where the value should be inserted, represented as a byte array.
    * @param nx
    *           If {@code true}, the operation will only succeed if the key does not already exist
    *           (NX - "Not Exists").
    * @param xx
    *           If {@code true}, the operation will only succeed if the key already exists (XX -
    *           "Exists").
    * @return A {@link CompletionStage} containing the result of the operation as a {@link String}.
    */
   public CompletionStage<String> set(byte[] key, byte[] value, byte[] path, boolean nx, boolean xx) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(value, ERR_VALUE_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new JsonSetFunction(value, path, nx, xx));
   }

   /**
    * Retrieves the length of an array at the specified JSON path.
    * <p>
    * If the value at the path is an array, it returns the number of elements.
    * Returns {@code null} if the value is not an array.
    * </p>
    *
    * @param key  the key identifying the JSON document
    * @param path the JSON path to evaluate
    * @return a {@link CompletionStage} resolving to a {@link List} of array lengths, or {@code null} if the value is not an array
    */
   public CompletionStage<List<Long>> arrLen(byte[] key, byte[] path) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new JsonLenArrayFunction(path));
   }

   /**
    * Retrieves the length of a string at the specified JSON path.
    * <p>
    * If the value at the path is a string, it returns the number of characters.
    * Returns {@code null} if the value is not a string.
    * </p>
    *
    * @param key  the key identifying the JSON document
    * @param path the JSON path to evaluate
    * @return a {@link CompletionStage} resolving to a {@link List} of string lengths, or {@code null} if the value is not a string
    */
   public CompletionStage<List<Long>> srtLen(byte[] key, byte[] path) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new JsonLenStrFunction(path));
   }

   /**
    * Retrieves the number of key-value pairs in an object at the specified JSON path.
    * <p>
    * If the value at the path is an object, it returns the number of keys.
    * Returns {@code null} if the value is not an object.
    * </p>
    *
    * @param key  the key identifying the JSON document
    * @param path the JSON path to evaluate
    * @return a {@link CompletionStage} resolving to a {@link List} of object sizes, or {@code null} if the value is not an object
    */
   public CompletionStage<List<Long>> objLen(byte[] key, byte[] path) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new JsonLenObjFunction(path));
   }

   /**
    * Reports the type of the JSON value at the specified path within the given JSON. The result
    * will indicate the type of the value at each path in the list.
    *
    * @param key
    *           The key representing the JSON document, provided as a byte array.
    * @param path
    *           The JSON path at which the type of the value should be determined, provided as a
    *           byte array.
    * @return A {@link CompletionStage} containing a {@link List} of type strings, representing the
    *         type of the JSON value at each path (e.g., "object", "array", "string", etc.).
    */
   public CompletionStage<List<String>> type(byte[] key, byte[] path) {
      return readWriteMap.eval(key, new JsonTypeFunction(path));
   }

   /**
    * Deletes the value at the given path in the JSON document.
    *
    * @param key
    *           the key of the JSON document
    * @param path
    *           the path to the value to be deleted
    * @return a {@link CompletionStage} of the number of bytes deleted
    */
   public CompletionStage<Long> del(byte[] key, byte[] path) {
      return readWriteMap.eval(key, new JsonDelFunction(path));
   }

   /**
    * Appends the given values to the array at the specified paths in the JSON document associated
    * with the specified key. If the paths does not refer to an array, no changes are made to the
    * document.
    *
    * @param key
    *           The key of the JSON document to update.
    * @param path
    *           The JSON path of the array to append to.
    * @param values
    *           The values to append to the array.
    * @return A {@link CompletionStage} that will complete with the returning a list of the new
    *         lengths of the changed arrays. Null is returned for the matching paths that are not
    *         arrays.
    */
   public CompletionStage<List<Long>> arrAppend(byte[] key, byte[] path, List<byte[]> values) {
      return readWriteMap.eval(key, new JsonArrayAppendFunction(path, values));
   }

   /**
    * Appends the given value to the string at the specified paths in the JSON document associated
    * with the specified key. If the path exists but is not a string, no changes are made.
    * {@link IllegalArgumentException} is thrown.
    *
    * @param key
    *           the key identifying the JSON document
    * @param path
    *           the path to the array in the JSON document
    * @param value
    *           the value to append to the array
    * @return A {@link CompletionStage} that will complete with the returning a list of the new
    *         lengths of the changed string. Null is returned for the matching paths that are not
    *         string.
    */
   public CompletionStage<List<Long>> strAppend(byte[] key, byte[] path, byte[] value) {
      return readWriteMap.eval(key, new JsonStringAppendFunction(path, value));
   }

   /**
    * Toggles the boolean value at the specified JSON path in the stored JSON document.
    * If the value is `true`, it becomes `false`, and vice versa.
    * Non-boolean values result in `null`.
    *
    * @param key  The key identifying the JSON document in the Infinispan cache.
    * @param path The JSON path where the boolean value should be toggled.
    * @return A {@code CompletionStage} with a {@code List<Integer>} of results:
    *         <ul>
    *           <li>{@code 1} if toggled to {@code true}</li>
    *           <li>{@code 0} if toggled to {@code false}</li>
    *           <li>{@code null} if the value is not a boolean</li>
    *         </ul>
    */
   public CompletionStage<List<Integer>> toggle(byte[] key, byte[] path) {
      return readWriteMap.eval(key, new JsonToggleFunction(path));
   }

   /**
    * Retrieves the keys of the JSON object at the specified path within the given JSON document.
    *
    * @param key
    *           The key representing the JSON document, provided as a byte array.
    * @param path
    *           The JSON path at which the keys should be retrieved, provided as a byte array.
    * @return A {@link CompletionStage} containing a {@link List} of list of byte arrays, each representing
    *         a key in the JSON object or null if object is not a json object.
    */
   public CompletionStage<List<List<byte[]>>> objKeys(byte[] key, byte[] path) {
      return readWriteMap.eval(key, new JsonObjkeysFunction(path));
   }

   /**
    * Increments the number at the specified JSON path by the given value.
    *
    * @param key       the key identifying the JSON document
    * @param path      the JSON path to the number to be incremented
    * @param value     the value to operate by
    * @return a {@code CompletionStage} resolving to a list of updated numbers
    */
   public CompletionStage<List<Number>> numIncBy(byte[] key, byte[] path, byte[] value) {
      return readWriteMap.eval(key, new JsonNumIncrOpFunction(path, value));
   }

   /**
    * Multiply the number at the specified JSON path by the given value.
    *
    * @param key       the key identifying the JSON document
    * @param path      the JSON path to the number to be multiplied
    * @param value     the value to operate by
    * @return a {@code CompletionStage} resolving to a list of updated numbers
    */
   public CompletionStage<List<Number>> numMultBy(byte[] key, byte[] path, byte[] value) {
      return readWriteMap.eval(key, new JsonNumMultOpFunction(path, value));
   }

   /**
    * Clears container values (arrays/objects) and sets numeric values to 0 at the
    * specified JSON path for the given key.
    *
    * @param key The key identifying the data.
    * @param path The JSON path of the data to clear.
    * @return A CompletionStage indicating the result of the operation.
    */
   public CompletionStage<Integer> clear(byte[] key, byte[] path) {
      return readWriteMap.eval(key, new JsonClearFunction(path));
   }

   /**
    * Finds the first index of the specified value in the JSON array at the given path.
    * The search is performed within the specified range [start, stop].
    *
    * @param key       The key identifying the JSON document.
    * @param jsonPath  The JSON path to the array in which to search for the value.
    * @param value     The value to search for in the array.
    * @param start     The starting index of the range to search within.
    * @param stop      The ending index of the range to search within.
    * @param isLegacy  A boolean indicating whether to use legacy behavior.
    * @return A {@link CompletionStage} that will complete with a list of indices where the value is found.
    *         If the value is not found, the list will contain -1.
    */
   public CompletionStage<List<Integer>> arrIndex(byte[] key, byte[] jsonPath, byte[] value, int start, int stop, boolean isLegacy) {
      return readWriteMap.eval(key, new JsonArrindexFunction(jsonPath, value, start, stop, isLegacy));
}

   /**
    * Inserts the given values into the array at the specified index in the JSON document
    * identified by the given key.
    *
    * @param key The key identifying the JSON document.
    * @param jsonPath The JSON path specifying the array.
    * @param index The index at which to insert the values.
    * @param values The values to insert.
    * @return A {@link CompletionStage} that will complete with the list of the new lengths of the changed arrays.
    *         Null is returned for the matching paths that are not arrays.
    */
   public CompletionStage<List<Integer>> arrInsert(byte[] key, byte[] jsonPath, int index, List<byte[]> values) {
      return readWriteMap.eval(key, new JsonArrinsertFunction(jsonPath, index, values));
   }

   /**
    * Trims the elements in an array out of the specified range.
    *
    * @param key
    *           The key of the array to trim.
    * @param jsonPath
    *           The JSON path of the array to trim.
    * @param start
    *           The starting index of the range to keep (inclusive).
    * @param stop
    *           The ending index of the range to keep (inclusive).
    * @return A {@link CompletionStage} that will complete with a list of length of the trimmed array.
    */
   public CompletionStage<List<Integer>> arrTrim(byte[] key, byte[] jsonPath, int start, int stop) {
      return readWriteMap.eval(key, new JsonArrtrimFunction(jsonPath, start, stop));
   }

   /**
    * Removes the element at the specified index from the array at the specified JSON path in the
    * document associated with the specified key.
    *
    * @param key
    *           The key of the document.
    * @param jsonPath
    *           The JSON path to the array.
    * @param index
    *           The index of the element to remove.
    * @return A {@link CompletionStage} that will complete with the remove elements of all the
    *         matching paths.
    */
   public CompletionStage<List<byte[]>> arrpop(byte[] key, byte[] jsonPath, int index) {
      return readWriteMap.eval(key, new JsonArrpopFunction(jsonPath, index));
   }
   /**
    * Merges the given JSON value into the JSON document at the given JSON path in the map entry
    * with the given key. Merge is performed recursively (deep merge).
    *
    * @param key
    *           the key of the map entry
    * @param jsonPath
    *           the JSON path of the field to merge the value into
    * @param value
    *           the value to merge into the JSON document
    * @return a CompletionStage that completes with OK if the merge is successful
    */
   public CompletionStage<String> merge(byte[] key, byte[] jsonPath, byte[] value) {
      return readWriteMap.eval(key, new JsonMergeFunction(jsonPath, value));
   }

   /**
    * Returns a RESP representation of the json objects matching the jsonPath
    *
    * @param key
    *           The key of the document.
    * @param jsonPath
    *           The JSON path.
    * @return A {@link CompletionStage} that will complete with the RESP representation
    */
   public CompletionStage<List<Object>> resp(byte[] key, byte[] jsonPath) {
      return readWriteMap.eval(key, new JsonRespFunction(jsonPath));
   }

   /**
    * Returns a list of size in bytes for all the jsonpath matches
    *
    * @param key
    *           The key of the document.
    * @param jsonPath
    *           The JSON path.
    * @return A {@link CompletionStage} that will complete with a list of sizes
    */
   public CompletionStage<List<Long>> debug(byte[] key, byte[] jsonPath) {
      return readWriteMap.eval(key, new JsonDebugMemoryFunction(jsonPath));
   }
}
