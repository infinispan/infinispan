package org.infinispan.loaders.cassandra;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.dataforte.cassandra.pool.DataSource;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CassandraThriftDataSource;
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
import org.apache.cassandra.thrift.SuperColumn;
import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.keymappers.TwoWayKey2StringMapper;
import org.infinispan.loaders.keymappers.UnsupportedKeyTypeException;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.Util;
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

	private CassandraThriftDataSource dataSource;

	private ConsistencyLevel readConsistencyLevel;
	private ConsistencyLevel writeConsistencyLevel;

	private String cacheName;
	private ColumnPath entryColumnPath;
	private ColumnParent entryColumnParent;
	private ColumnParent expirationColumnParent;
	private String entryKeyPrefix;
	private String expirationKey;
	private TwoWayKey2StringMapper keyMapper;

	static private byte emptyByteArray[] = {};

	public Class<? extends CacheLoaderConfig> getConfigurationClass() {
		return CassandraCacheStoreConfig.class;
	}

	@Override
	public void init(CacheLoaderConfig clc, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
		super.init(clc, cache, m);
		this.cacheName = cache.getName();
		this.config = (CassandraCacheStoreConfig) clc;
	}

	@Override
	public void start() throws CacheLoaderException {

		try {
			dataSource = new DataSource(config.getPoolProperties());
			readConsistencyLevel = ConsistencyLevel.valueOf(config.readConsistencyLevel);
			writeConsistencyLevel = ConsistencyLevel.valueOf(config.writeConsistencyLevel);
			entryColumnPath = new ColumnPath(config.entryColumnFamily).setColumn(ENTRY_COLUMN_NAME.getBytes("UTF-8"));
			entryColumnParent = new ColumnParent(config.entryColumnFamily);
			entryKeyPrefix = ENTRY_KEY_PREFIX + (config.isSharedKeyspace() ? cacheName + "_" : "");
			expirationColumnParent = new ColumnParent(config.expirationColumnFamily);
			expirationKey = EXPIRATION_KEY + (config.isSharedKeyspace() ? "_" + cacheName : "");
			keyMapper = (TwoWayKey2StringMapper) Util.getInstance(config.getKeyMapper());
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
		String hashKey = hashKey(key);
		Cassandra.Client cassandraClient = null;
		try {
			cassandraClient = dataSource.getConnection();
			ColumnOrSuperColumn column = cassandraClient.get(config.keySpace, hashKey, entryColumnPath, readConsistencyLevel);
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
			dataSource.releaseConnection(cassandraClient);
		}
	}

	@Override
	public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
		return load(Integer.MAX_VALUE);
	}

	@Override
	public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
		Cassandra.Client cassandraClient = null;
		try {
			cassandraClient = dataSource.getConnection();
			Set<InternalCacheEntry> s = new HashSet<InternalCacheEntry>();
			SlicePredicate slicePredicate = new SlicePredicate();
			slicePredicate.setSlice_range(new SliceRange(entryColumnPath.getColumn(), emptyByteArray, false, 1));
			String startKey = "";

			// Get the keys in SLICE_SIZE blocks
			int sliceSize = Math.min(SLICE_SIZE, numEntries);
			for (boolean complete = false; !complete;) {
				KeyRange keyRange = new KeyRange(sliceSize);
				keyRange.setStart_token(startKey);
				keyRange.setEnd_token("");
				List<KeySlice> keySlices = cassandraClient.get_range_slices(config.keySpace, entryColumnParent, slicePredicate, keyRange, readConsistencyLevel);

				// Cycle through all the keys
				for (KeySlice keySlice : keySlices) {
					Object key = unhashKey(keySlice.getKey());
					if (key == null) // Skip invalid keys
						continue;
					List<ColumnOrSuperColumn> columns = keySlice.getColumns();
					if (columns.size() > 0) {
						if (log.isDebugEnabled()) {
							log.debug("Loading {0}", key);
						}
						byte[] value = columns.get(0).getColumn().getValue();
						InternalCacheEntry ice = unmarshall(value, key);
						s.add(ice);
					} else if (log.isDebugEnabled()) {
						log.debug("Skipping empty key {0}", key);
					}
				}
				if (keySlices.size() < sliceSize) {
					// Cassandra has returned less keys than what we asked for.
					// Assume we have finished
					complete = true;
				} else {
					// Cassandra has returned exactly the amount of keys we
					// asked for. If we haven't reached the required quota yet,
					// assume we need to cycle again starting from
					// the last returned key (excluded)
					sliceSize = Math.min(SLICE_SIZE, numEntries - s.size());
					if (sliceSize == 0) {
						complete = true;
					} else {
						startKey = keySlices.get(keySlices.size() - 1).getKey();
					}
				}

			}
			return s;
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			dataSource.releaseConnection(cassandraClient);
		}
	}

	@Override
	public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
		Cassandra.Client cassandraClient = null;
		try {
			cassandraClient = dataSource.getConnection();
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
				List<KeySlice> keySlices = cassandraClient.get_range_slices(config.keySpace, entryColumnParent, slicePredicate, keyRange, readConsistencyLevel);
				if (keySlices.size() < SLICE_SIZE) {
					complete = true;
				} else {
					startKey = keySlices.get(keySlices.size() - 1).getKey();
				}

				for (KeySlice keySlice : keySlices) {
					if (keySlice.getColumnsSize() > 0) {
						Object key = unhashKey(keySlice.getKey());
						if (key != null && (keysToExclude == null || !keysToExclude.contains(key)))
							s.add(key);
					}
				}
			}
			return s;
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			dataSource.releaseConnection(cassandraClient);
		}
	}

	/**
	 * Closes all databases, ignoring exceptions, and nulls references to all
	 * database related information.
	 */
	@Override
	public void stop() {

	}

	@Override
	public void clear() throws CacheLoaderException {
		Cassandra.Client cassandraClient = null;
		try {
			cassandraClient = dataSource.getConnection();
			SlicePredicate slicePredicate = new SlicePredicate();
			slicePredicate.setSlice_range(new SliceRange(entryColumnPath.getColumn(), emptyByteArray, false, 1));
			String startKey = "";
			boolean complete = false;
			// Get the keys in SLICE_SIZE blocks
			while (!complete) {
				KeyRange keyRange = new KeyRange(SLICE_SIZE);
				keyRange.setStart_token(startKey);
				keyRange.setEnd_token("");
				List<KeySlice> keySlices = cassandraClient.get_range_slices(config.keySpace, entryColumnParent, slicePredicate, keyRange, readConsistencyLevel);
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
			dataSource.releaseConnection(cassandraClient);
		}

	}

	@Override
	public boolean remove(Object key) throws CacheLoaderException {
		if (trace)
			log.trace("remove(\"{0}\") ", key);
		Cassandra.Client cassandraClient = null;
		try {
			cassandraClient = dataSource.getConnection();
			Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>();
			remove0(hashKey(key), mutationMap);
			cassandraClient.batch_mutate(config.keySpace, mutationMap, writeConsistencyLevel);
			return true;
		} catch (Exception e) {
			log.error("Exception while removing " + key, e);
			return false;
		} finally {
			dataSource.releaseConnection(cassandraClient);
		}
	}

	private void remove0(String key, Map<String, Map<String, List<Mutation>>> mutationMap) {
		addMutation(mutationMap, key, config.entryColumnFamily, null, null);
	}

	private byte[] marshall(InternalCacheEntry entry) throws IOException, InterruptedException {
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
		Cassandra.Client cassandraClient = null;

		try {
			cassandraClient = dataSource.getConnection();
			Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>(2);
			store0(entry, mutationMap);

			cassandraClient.batch_mutate(config.keySpace, mutationMap, writeConsistencyLevel);
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			dataSource.releaseConnection(cassandraClient);
		}
	}

	private void store0(InternalCacheEntry entry, Map<String, Map<String, List<Mutation>>> mutationMap) throws IOException, UnsupportedKeyTypeException {
		Object key = entry.getKey();
		if (trace)
			log.trace("store(\"{0}\") ", key);
		String cassandraKey = hashKey(key);
		try {
			addMutation(mutationMap, cassandraKey, config.entryColumnFamily, entryColumnPath.getColumn(), marshall(entry));
			if (entry.canExpire()) {
				addExpiryEntry(cassandraKey, entry.getExpiryTime(), mutationMap);
			}
		} catch (InterruptedException ie) {
			if (trace)
				log.trace("Interrupted while trying to marshall entry");
			Thread.currentThread().interrupt();
		}
	}

	private void addExpiryEntry(String cassandraKey, long expiryTime, Map<String, Map<String, List<Mutation>>> mutationMap) {
		try {
			addMutation(mutationMap, expirationKey, config.expirationColumnFamily, longToBytes(expiryTime), cassandraKey.getBytes("UTF-8"), emptyByteArray);
		} catch (Exception e) {
			// Should not happen
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
		} catch (InterruptedException ie) {
			if (log.isTraceEnabled())
				log.trace("Interrupted while reading from stream");
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Purge expired entries. Expiration entries are stored in a single key
	 * (expirationKey) within a specific ColumnFamily (set by configuration).
	 * The entries are grouped by expiration timestamp in SuperColumns within
	 * which each entry's key is mapped to a column
	 */
	@Override
	protected void purgeInternal() throws CacheLoaderException {
		if (trace)
			log.trace("purgeInternal");
		Cassandra.Client cassandraClient = null;
		try {
			cassandraClient = dataSource.getConnection();
			// We need to get all supercolumns from the beginning of time until
			// now, in SLICE_SIZE chunks
			SlicePredicate predicate = new SlicePredicate();
			predicate.setSlice_range(new SliceRange(emptyByteArray, longToBytes(System.currentTimeMillis()), false, SLICE_SIZE));
			Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>();
			for (boolean complete = false; !complete;) {
				// Get all columns
				List<ColumnOrSuperColumn> slice = cassandraClient.get_slice(config.keySpace, expirationKey, expirationColumnParent, predicate, readConsistencyLevel);
				complete = slice.size() < SLICE_SIZE;
				// Delete all keys returned by the slice
				for (ColumnOrSuperColumn crumb : slice) {
					SuperColumn scol = crumb.getSuper_column();
					for (Iterator<Column> i = scol.getColumnsIterator(); i.hasNext();) {
						Column col = i.next();
						// Remove the entry row
						remove0(new String(col.getName(), "UTF-8"), mutationMap);
					}
					// Remove the expiration supercolumn
					addMutation(mutationMap, expirationKey, config.expirationColumnFamily, scol.getName(), null, null);
				}
			}
			cassandraClient.batch_mutate(config.keySpace, mutationMap, writeConsistencyLevel);
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			dataSource.releaseConnection(cassandraClient);
		}

	}

	@Override
	protected void applyModifications(List<? extends Modification> mods) throws CacheLoaderException {
		Cassandra.Client cassandraClient = null;

		try {
			cassandraClient = dataSource.getConnection();
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

			cassandraClient.batch_mutate(config.keySpace, mutationMap, writeConsistencyLevel);
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			dataSource.releaseConnection(cassandraClient);
		}

	}

	@Override
	public String toString() {
		return "CassandraCacheStore";
	}

	private String hashKey(Object key) throws UnsupportedKeyTypeException {
		if (!keyMapper.isSupportedType(key.getClass())) {
			throw new UnsupportedKeyTypeException(key);
		}

		return entryKeyPrefix + keyMapper.getStringMapping(key);
	}

	private Object unhashKey(String key) {
		if (key.startsWith(entryKeyPrefix))
			return keyMapper.getKeyMapping(key.substring(entryKeyPrefix.length()));
		else
			return null;
	}

	private static void addMutation(Map<String, Map<String, List<Mutation>>> mutationMap, String key, String columnFamily, byte[] column, byte[] value) {
		addMutation(mutationMap, key, columnFamily, null, column, value);
	}

	private static void addMutation(Map<String, Map<String, List<Mutation>>> mutationMap, String key, String columnFamily, byte[] superColumn, byte[] column, byte[] value) {
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
			if (superColumn != null) {
				deletion.setSuper_column(superColumn);
			}
			if (column != null) { // Single column delete
				deletion.setPredicate(new SlicePredicate().setColumn_names(Arrays.asList(new byte[][] { column })));
			} // else Delete entire column family or supercolumn
			columnFamilyMutations.add(new Mutation().setDeletion(deletion));
		} else { // Insert/update
			ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
			if (superColumn != null) {
				List<Column> columns = new ArrayList<Column>();
				columns.add(new Column(column, value, System.currentTimeMillis()));
				cosc.setSuper_column(new SuperColumn(superColumn, columns));
			} else {
				cosc.setColumn(new Column(column, value, System.currentTimeMillis()));
			}
			columnFamilyMutations.add(new Mutation().setColumn_or_supercolumn(cosc));
		}
	}

	private static final byte[] longToBytes(long v) {
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
