package org.infinispan.loaders.cassandra;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.dataforte.cassandra.pool.ConnectionPool;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A persistent <code>CacheLoader</code> based on Apache Cassandra project. See
 * http://cassandra.apache.org/
 * 
 * @author Tristan Tarrant
 */
@CacheLoaderMetadata(configurationClass = CassandraCacheStoreConfig.class)
public class CassandraCacheStore extends AbstractCacheStore {

	private static final String ENTRY_KEY_PREFIX = "entry_";
	private static final String ENTRY_COLUMN_NAME = "entry";
	private static final String EXPIRATION_KEY = "expiration";
	private static final int SLICE_SIZE = 100;
	private static final Log log = LogFactory.getLog(CassandraCacheStore.class);
	private static final boolean trace = log.isTraceEnabled();

	private CassandraCacheStoreConfig config;

	private ConnectionPool pool;

	private ColumnPath entryColumnPath;
	private ColumnParent entryColumnParent;

	static private byte emptyByteArray[] = {};

	public Class<? extends CacheLoaderConfig> getConfigurationClass() {
		return CassandraCacheStoreConfig.class;
	}

	@Override
	public void init(CacheLoaderConfig clc, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
		super.init(clc, cache, m);
		this.config = (CassandraCacheStoreConfig) clc;
	}

	@Override
	public void start() throws CacheLoaderException {

		try {
			pool = new ConnectionPool(config.getPoolProperties());
			entryColumnPath = new ColumnPath(config.entryColumnFamily).setColumn(ENTRY_COLUMN_NAME.getBytes("UTF-8"));
			entryColumnParent = new ColumnParent(config.entryColumnFamily);
		} catch (Exception e) {
			throw new ConfigurationException(e);
		}

		log.debug("cleaning up expired entries...");
		purgeInternal();

		log.debug("started");
		super.start();
	}

	@Override
	public InternalCacheEntry load(Object key) throws CacheLoaderException {
		String hashKey = CassandraCacheStore.hashKey(key);
		Cassandra.Iface cassandraClient = null;
		try {
			cassandraClient = pool.getConnection();
			ColumnOrSuperColumn column = cassandraClient.get(config.keySpace, hashKey, entryColumnPath, ConsistencyLevel.ONE);
			InternalCacheEntry ice = unmarshall(column.getColumn().getValue(), key);
			if (ice != null && ice.isExpired()) {
				remove(key);
				return null;
			}
			return ice;
		} catch (NotFoundException nfe) {
			log.debug("Key '{0}' not found", hashKey);
			return null;
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			pool.release(cassandraClient);
		}
	}

	@Override
	public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
		return load(Integer.MAX_VALUE);
	}

	@Override
	public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
		Cassandra.Iface cassandraClient = null;
		try {
			cassandraClient = pool.getConnection();
			Set<InternalCacheEntry> s = new HashSet<InternalCacheEntry>();
			SlicePredicate slicePredicate = new SlicePredicate();
			slicePredicate.setSlice_range(new SliceRange(entryColumnPath.getColumn(), emptyByteArray, false, 1));
			String startKey = "";
			boolean complete = false;
			
			// Get the keys in SLICE_SIZE blocks
			int sliceSize = Math.min(SLICE_SIZE, numEntries);
			while (!complete) {
				KeyRange keyRange = new KeyRange(sliceSize);
				keyRange.setStart_token(startKey);
				keyRange.setEnd_token("");
				List<KeySlice> keySlices = cassandraClient.get_range_slices(config.keySpace, entryColumnParent, slicePredicate, keyRange, ConsistencyLevel.ONE);
				if (keySlices.size() < sliceSize) {
					// Cassandra has returned less keys than what we asked for. Assume we have finished
					complete = true;
				} else {
					// Cassandra has returned exactly the amount of keys we asked for. Assume we need to cycle again starting from the last returned key (excluded)
					startKey = keySlices.get(keySlices.size() - 1).getKey();
					sliceSize = Math.min(SLICE_SIZE, numEntries - s.size());
					if (sliceSize == 0) {
						complete = true;
					}
				}

				// Cycle through all the keys
				for (KeySlice keySlice : keySlices) {
					String key = unhashKey(keySlice.getKey());
					List<ColumnOrSuperColumn> columns = keySlice.getColumns();
					if (columns.size() > 0) {
						log.debug("COLUMN = " + new String(columns.get(0).getColumn().getName()));
						byte[] value = columns.get(0).getColumn().getValue();
						InternalCacheEntry ice = unmarshall(value, key);
						s.add(ice);
					}
				}
			}
			return s;
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			pool.release(cassandraClient);
		}
	}

	@Override
	public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
		Cassandra.Iface cassandraClient = null;
		try {
			cassandraClient = pool.getConnection();
			Set<Object> s = new HashSet<Object>();
			SlicePredicate slicePredicate = new SlicePredicate();
			slicePredicate.setSlice_range(new SliceRange(entryColumnPath.getColumn(), emptyByteArray, false, 1));
			String startKey = "";
			boolean complete = false;
			// Get the keys in SLICE_SIZE blocks
			while (!complete) {
				KeyRange keyRange = new KeyRange(SLICE_SIZE);
				keyRange.setStart_token(startKey);
				keyRange.setEnd_token("");
				List<KeySlice> keySlices = cassandraClient.get_range_slices(config.keySpace, entryColumnParent, slicePredicate, keyRange, ConsistencyLevel.ONE);
				if (keySlices.size() < SLICE_SIZE) {
					complete = true;
				} else {
					startKey = keySlices.get(keySlices.size() - 1).getKey();
				}

				for (KeySlice keySlice : keySlices) {
					String key = unhashKey(keySlice.getKey());
					if (keysToExclude == null || !keysToExclude.contains(key))
						s.add(key);
				}
			}
			return s;
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			pool.release(cassandraClient);
		}
	}

	/**
	 * Closes all databases, ignoring exceptions, and nulls references to all
	 * database related information.
	 */
	@Override
	public void stop() {
		pool.close();
	}

	@Override
	public void clear() throws CacheLoaderException {
		if (trace)
			log.trace("clear()");
		Cassandra.Iface cassandraClient = null;
		try {
			cassandraClient = pool.getConnection();
			SlicePredicate slicePredicate = new SlicePredicate();
			slicePredicate.setSlice_range(new SliceRange(entryColumnPath.getColumn(), emptyByteArray, false, 1));
			String startKey = "";
			boolean complete = false;
			// Get the keys in SLICE_SIZE blocks
			while (!complete) {
				KeyRange keyRange = new KeyRange(SLICE_SIZE);
				keyRange.setStart_token(startKey);
				keyRange.setEnd_token("");
				List<KeySlice> keySlices = cassandraClient.get_range_slices(config.keySpace, entryColumnParent, slicePredicate, keyRange, ConsistencyLevel.ONE);
				if (keySlices.size() < SLICE_SIZE) {
					complete = true;
				} else {
					startKey = keySlices.get(keySlices.size() - 1).getKey();
				}
				Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>();

				for (KeySlice keySlice : keySlices) {
					String cassandraKey = keySlice.getKey();
					remove0(cassandraKey, mutationMap);
				}
				cassandraClient.batch_mutate(config.keySpace, mutationMap, ConsistencyLevel.ALL);
			}
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			pool.release(cassandraClient);
		}

	}

	@Override
	public boolean remove(Object key) throws CacheLoaderException {
		if (trace)
			log.trace("remove() " + key);
		Cassandra.Iface cassandraClient = null;
		try {
			cassandraClient = pool.getConnection();
			Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>();
			remove0(CassandraCacheStore.hashKey(key), mutationMap);
			cassandraClient.batch_mutate(config.keySpace, mutationMap, ConsistencyLevel.ONE);
			return true;
		} catch (Exception e) {
			log.error("Exception while removing " + key, e);
			return false;
		} finally {
			pool.release(cassandraClient);
		}
	}

	private void remove0(String key, Map<String, Map<String, List<Mutation>>> mutationMap) {
		addMutation(mutationMap, key, config.entryColumnFamily, null, null);
		addMutation(mutationMap, key, config.expirationColumnFamily, null, null);
	}

	private byte[] marshall(InternalCacheEntry entry) throws IOException {
		return getMarshaller().objectToByteBuffer(entry.toInternalCacheValue());
	}

	private InternalCacheEntry unmarshall(Object o, Object key) throws IOException, ClassNotFoundException {
		if (o == null)
			return null;
		byte b[] = (byte[]) o;
		InternalCacheValue v = (InternalCacheValue) getMarshaller().objectFromByteBuffer(b);
		return v.toInternalCacheEntry(key);
	}

	public void store(InternalCacheEntry entry) throws CacheLoaderException {
		Cassandra.Iface cassandraClient = null;

		try {
			cassandraClient = pool.getConnection();
			Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>(2);
			store0(entry, mutationMap);
			cassandraClient.batch_mutate(config.keySpace, mutationMap, ConsistencyLevel.ONE);
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			pool.release(cassandraClient);
		}
	}

	private void store0(InternalCacheEntry entry, Map<String, Map<String, List<Mutation>>> mutationMap) throws IOException {
		Object key = entry.getKey();

		String cassandraKey = CassandraCacheStore.hashKey(key);
		addMutation(mutationMap, cassandraKey, config.entryColumnFamily, entryColumnPath.getColumn(), marshall(entry));
		if (entry.canExpire()) {
			addMutation(mutationMap, cassandraKey, config.expirationColumnFamily, longToBytes(entry.getExpiryTime()), emptyByteArray);

		}
	}

	/**
	 * Writes to a stream the number of entries (long) then the entries
	 * themselves.
	 */
	public void toStream(ObjectOutput out) throws CacheLoaderException {
		try {
			Set<InternalCacheEntry> loadAll = loadAll();
			int count = 0;
			for (InternalCacheEntry entry : loadAll) {
				getMarshaller().objectToObjectStream(entry, out);
				count++;
			}
			getMarshaller().objectToObjectStream(null, out);
		} catch (IOException e) {
			throw new CacheLoaderException(e);
		}
	}

	/**
	 * Reads from a stream the number of entries (long) then the entries
	 * themselves.
	 */
	public void fromStream(ObjectInput in) throws CacheLoaderException {
		try {
			int count = 0;
			while (true) {
				count++;
				InternalCacheEntry entry = (InternalCacheEntry) getMarshaller().objectFromObjectStream(in);
				if (entry == null)
					break;
				store(entry);
			}
		} catch (IOException e) {
			throw new CacheLoaderException(e);
		} catch (ClassNotFoundException e) {
			throw new CacheLoaderException(e);
		}
	}

	/**
	 * Purge expired entries.
	 */
	@Override
	protected void purgeInternal() throws CacheLoaderException {
		log.trace("purgeInternal");
		// TODO: implement

	}

	@Override
	protected void applyModifications(List<? extends Modification> mods) throws CacheLoaderException {
		Cassandra.Iface cassandraClient = null;

		try {
			cassandraClient = pool.getConnection();
			Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>();

			for (Modification m : mods) {
				switch (m.getType()) {
				case STORE:
					store0(((Store) m).getStoredEntry(), mutationMap);
					break;
				case CLEAR:
					clear();
					break;
				case REMOVE:
					remove0(hashKey(((Remove) m).getKey()), mutationMap);
					break;
				default:
					throw new AssertionError();
				}
			}

			cassandraClient.batch_mutate(config.keySpace, mutationMap, ConsistencyLevel.ONE);
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			pool.release(cassandraClient);
		}

	}

	@Override
	public String toString() {
		return "CassandraCacheStore";
	}

	public static String hashKey(Object key) {
		return ENTRY_KEY_PREFIX + key.toString();
	}

	public static String unhashKey(String key) {
		return key.substring(ENTRY_KEY_PREFIX.length());
	}

	public static String expirationColumn(long timestamp) {
		return String.format("expiration%013d", timestamp);
	}

	private static void addMutation(Map<String, Map<String, List<Mutation>>> mutationMap, String key, String columnFamily, byte[] column, byte[] value) {
		Map<String, List<Mutation>> keyMutations = mutationMap.get(key);
		// If the key doesn't exist yet, create the mutation holder
		if (keyMutations == null) {
			keyMutations = new HashMap<String, List<Mutation>>();
			mutationMap.put(key, keyMutations);
		}
		// If the columnfamily doesn't exist yet, create the mutation holder
		List<Mutation> columnFamilyMutations = keyMutations.get(columnFamily);
		if (columnFamilyMutations == null) {
			columnFamilyMutations = new ArrayList<Mutation>();
			keyMutations.put(columnFamily, columnFamilyMutations);
		}

		if (value == null) { // Delete
			Deletion deletion = new Deletion(System.currentTimeMillis());
			if (column != null) { // Single column delete
				deletion.setPredicate(new SlicePredicate().setColumn_names(Arrays.asList(new byte[][] { column })));
			} // else Delete entire column family
			columnFamilyMutations.add(new Mutation().setDeletion(deletion));
		} else { // Insert/update
			ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
			cosc.setColumn(new Column(column, value, System.currentTimeMillis()));
			columnFamilyMutations.add(new Mutation().setColumn_or_supercolumn(cosc));
		}
	}

	public static UUID getTimeBasedUUID(long timestamp) {
		long lsb = 0;
		long msb = 0;
		return new UUID(msb, lsb);
	}

	private static final byte[] longToBytes(long v) throws IOException {
		byte b[] = new byte[8];
		b[0] = (byte) (v >>> 56);
		b[1] = (byte) (v >>> 48);
		b[2] = (byte) (v >>> 40);
		b[3] = (byte) (v >>> 32);
		b[4] = (byte) (v >>> 24);
		b[5] = (byte) (v >>> 16);
		b[6] = (byte) (v >>> 8);
		b[7] = (byte) (v >>> 0);
		return b;
	}

}
