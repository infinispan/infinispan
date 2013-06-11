package org.infinispan.loaders.leveldb;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.concurrent.ThreadSafe;

import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.LockSupportCacheStore;
import org.infinispan.loaders.leveldb.logging.Log;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.logging.LogFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.impl.Iq80DBFactory;

@ThreadSafe
@CacheLoaderMetadata(configurationClass = LevelDBCacheStoreConfig.class)
public class LevelDBCacheStore extends LockSupportCacheStore<Integer> {
	private static final Log log = LogFactory.getLog(LevelDBCacheStore.class, Log.class);

	private LevelDBCacheStoreConfig config;
	private BlockingQueue<ExpiryEntry> expiryEntryQueue;
	private DB db;
	private DB expiredDb;

	@Override
	public Class<? extends CacheLoaderConfig> getConfigurationClass() {
		return LevelDBCacheStoreConfig.class;
	}

	@Override
	public void init(CacheLoaderConfig config, Cache<?, ?> cache,
			StreamingMarshaller m) throws CacheLoaderException {
		super.init(config, cache, m);

		this.config = (LevelDBCacheStoreConfig) config;
	}

	@Override
	public void start() throws CacheLoaderException {
		expiryEntryQueue = new LinkedBlockingQueue<ExpiryEntry>(
				config.getExpiryQueueSize());

		try {
			db = openDatabase(config.getLocation(), config.getDataDbOptions());
			expiredDb = openDatabase(config.getExpiredLocation(),
					config.getExpiredDbOptions());
		} catch (IOException e) {
			throw new ConfigurationException("Unable to open database", e);
		}

		super.start();
	}

	/**
	 * Creates database if it doesn't exist.
	 * 
	 * @return database at location
	 * @throws IOException
	 */
	protected DB openDatabase(String location, Options options)
			throws IOException {
		return Iq80DBFactory.factory.open(new File(location), options);
	}

	protected void destroyDatabase(String location) throws IOException {
		File dir = new File(location);

		Iq80DBFactory.factory.destroy(dir, null);
	}

	protected DB reinitDatabase(String location, Options options)
			throws IOException {
		destroyDatabase(location);
		return openDatabase(location, options);
	}

	protected void reinitAllDatabases() throws IOException {
		db = reinitDatabase(config.getLocation(), config.getDataDbOptions());
		expiredDb = reinitDatabase(config.getExpiredLocation(),
				config.getExpiredDbOptions());
	}

	@Override
	public void stop() throws CacheLoaderException {
	   try {
	      db.close();
	   } catch (IOException e) {
	      log.warnUnableToCloseDb(e);
	   }
	   
	   try {
         expiredDb.close();
      } catch (IOException e) {
         log.warnUnableToCloseExpiredDb(e);
      }

		super.stop();
	}

	@Override
	protected void clearLockSafe() throws CacheLoaderException {
		long count = 0;
		DBIterator it = db.iterator(new ReadOptions().fillCache(false));
		boolean destroyDatabase = false;

		if (config.getClearThreshold() <= 0) {
			try {
				for (it.seekToFirst(); it.hasNext();) {
					Map.Entry<byte[], byte[]> entry = it.next();
					db.delete(entry.getKey());
					count++;

					if (count > config.clearThreshold) {
						destroyDatabase = true;
						break;
					}
				}
			} finally {
				try {
               it.close();
            } catch (IOException e) {
               log.warnUnableToCloseDbIterator(e);
            }
			}
		} else {
			destroyDatabase = true;
		}

		if (destroyDatabase) {
			try {
				reinitAllDatabases();
			} catch (IOException e) {
				throw new CacheLoaderException(e);
			}
		}
	}

	@Override
	protected Set<InternalCacheEntry> loadAllLockSafe()
			throws CacheLoaderException {
		Set<InternalCacheEntry> entries = new HashSet<InternalCacheEntry>();

		DBIterator it = db.iterator(new ReadOptions().fillCache(false));
		try {
			for (it.seekToFirst(); it.hasNext();) {
				Map.Entry<byte[], byte[]> entry = it.next();
				entries.add(unmarshall(entry));
			}
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			try {
            it.close();
         } catch (IOException e) {
            log.warnUnableToCloseDbIterator(e);
         }
		}

		return entries;
	}

	@Override
	protected Set<InternalCacheEntry> loadLockSafe(int maxEntries)
			throws CacheLoaderException {
		if (maxEntries <= 0)
			return Collections.emptySet();

		Set<InternalCacheEntry> entries = new HashSet<InternalCacheEntry>();

		DBIterator it = db.iterator(new ReadOptions().fillCache(false));
		try {
			it.seekToFirst();
			for (int i = 0; it.hasNext() && i < maxEntries; i++) {
				Map.Entry<byte[], byte[]> entry = it.next();
				entries.add(unmarshall(entry));
			}
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			try {
            it.close();
         } catch (IOException e) {
            log.warnUnableToCloseDbIterator(e);
         }
		}

		return entries;
	}

	@Override
	protected Set<Object> loadAllKeysLockSafe(Set<Object> keysToExclude)
			throws CacheLoaderException {
		Set<Object> keys = new HashSet<Object>();

		DBIterator it = db.iterator(new ReadOptions().fillCache(false));
		try {
			for (it.seekToFirst(); it.hasNext();) {
				Map.Entry<byte[], byte[]> entry = it.next();
				Object key = unmarshall(entry.getKey());
				if (keysToExclude == null || keysToExclude.isEmpty()
						|| !keysToExclude.contains(key))
					keys.add(key);
			}

			return keys;
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			try {
            it.close();
         } catch (IOException e) {
            log.warnUnableToCloseDbIterator(e);
         }
		}
	}

	@Override
	protected void toStreamLockSafe(ObjectOutput oos)
			throws CacheLoaderException {
		DBIterator it = db.iterator(new ReadOptions().fillCache(false));
		try {

			for (it.seekToFirst(); it.hasNext();) {
				Map.Entry<byte[], byte[]> entry = it.next();
				InternalCacheEntry ice = unmarshall(entry);
				getMarshaller().objectToObjectStream(ice, oos);
			}
			getMarshaller().objectToObjectStream(null, oos);
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		} finally {
			try {
            it.close();
         } catch (IOException e) {
            log.warnUnableToCloseDbIterator(e);
         }
		}
	}

	@Override
	protected void fromStreamLockSafe(ObjectInput ois)
			throws CacheLoaderException {
		try {
			while (true) {
				InternalCacheEntry entry = (InternalCacheEntry) getMarshaller()
						.objectFromObjectStream(ois);
				if (entry == null)
					break;

				db.put(marshall(entry.getKey()), marshall(entry));
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		}

	}

	@Override
	protected boolean removeLockSafe(Object key, Integer lockingKey)
			throws CacheLoaderException {
		try {
			byte[] keyBytes = marshall(key);
			if (db.get(keyBytes) == null) {
				return false;
			}
			db.delete(keyBytes);
			return true;
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		}
	}

	@Override
	protected void storeLockSafe(InternalCacheEntry ed, Integer lockingKey)
			throws CacheLoaderException {
		try {
			db.put(marshall(ed.getKey()), marshall(ed));
			if (ed.canExpire()) {
				addNewExpiry(ed);
			}
		} catch (Exception e) {
			throw new DBException(e);
		}
	}

	@Override
	protected InternalCacheEntry loadLockSafe(Object key, Integer lockingKey)
			throws CacheLoaderException {
		try {
			InternalCacheEntry ice = unmarshall(
					db.get(marshall(key)), key);
			if (ice != null && ice.isExpired(System.currentTimeMillis())) {
				removeLockSafe(key, lockingKey);
				return null;
			}
			return ice;
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		}
	}

	@Override
	protected Integer getLockFromKey(Object key) throws CacheLoaderException {
		return key.hashCode();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void purgeInternal() throws CacheLoaderException {
		try {
			// Drain queue and update expiry tree
			List<ExpiryEntry> entries = new ArrayList<ExpiryEntry>();
			expiryEntryQueue.drainTo(entries);
			for (ExpiryEntry entry : entries) {
				final byte[] expiryBytes = marshall(entry.expiry);
				final byte[] keyBytes = marshall(entry.key);
				final byte[] existingBytes = expiredDb.get(expiryBytes);

				if (existingBytes != null) {
					// in the case of collision make the key a List ...
					final Object existing = unmarshall(existingBytes);
					if (existing instanceof List) {
						((List<Object>) existing).add(entry.key);
						expiredDb.put(expiryBytes, marshall(existing));
					} else {
						List<Object> al = new ArrayList<Object>(2);
						al.add(existing);
						al.add(entry.key);
						expiredDb.put(expiryBytes, marshall(al));
					}
				} else {
					expiredDb.put(marshall(expiryBytes), keyBytes);
				}
			}

			List<Long> times = new ArrayList<Long>();
			List<Object> keys = new ArrayList<Object>();
			DBIterator it = expiredDb.iterator(new ReadOptions()
					.fillCache(false));
			try {
				for (it.seekToFirst(); it.hasNext();) {
					Map.Entry<byte[], byte[]> entry = it.next();

					Long time = (Long) unmarshall(entry.getKey());
					if (time > System.currentTimeMillis())
						break;
					times.add(time);
					Object key = unmarshall(entry.getValue());
					if (key instanceof List)
						keys.addAll((List<?>) key);
					else
						keys.add(key);
				}

				for (Long time : times) {
					expiredDb.delete(marshall(time));
				}

				if (!keys.isEmpty())
					log.debugf("purge (up to) %d entries", keys.size());
				int count = 0;
				long currentTimeMillis = System.currentTimeMillis();
				for (Object key : keys) {
					byte[] keyBytes = marshall(key);
					
					byte[] b = db.get(keyBytes);
					if (b == null)
						continue;
					InternalCacheValue ice = (InternalCacheValue) getMarshaller()
							.objectFromByteBuffer(b);
					if (ice.isExpired(currentTimeMillis)) {
						// somewhat inefficient to FIND then REMOVE...
						db.delete(keyBytes);
						count++;
					}
				}
				if (count != 0)
					log.debugf("purged %d entries", count);
			} catch (Exception e) {
				throw new CacheLoaderException(e);
			} finally {
			   try {
			      it.close();
			   } catch (IOException e) {
			      log.warnUnableToCloseDbIterator(e);
			   }
			}
		} catch (Exception e) {
			throw new CacheLoaderException(e);
		}
	}

	private byte[] marshall(InternalCacheEntry entry) throws IOException,
			InterruptedException {
		return marshall(entry.toInternalCacheValue());
	}

	private byte[] marshall(Object entry) throws IOException,
			InterruptedException {
		return getMarshaller().objectToByteBuffer(entry);
	}

	private Object unmarshall(byte[] bytes) throws IOException,
			ClassNotFoundException {
		if (bytes == null)
			return null;

		return getMarshaller().objectFromByteBuffer(bytes);
	}

	private InternalCacheEntry unmarshall(Map.Entry<byte[], byte[]> entry)
			throws IOException, ClassNotFoundException {
		if (entry == null || entry.getValue() == null)
			return null;

		InternalCacheValue v = (InternalCacheValue) unmarshall(entry.getValue());
		Object k = unmarshall(entry.getKey());
		return v.toInternalCacheEntry(k);
	}

	private InternalCacheEntry unmarshall(byte[] value, Object key)
			throws IOException, ClassNotFoundException {
		if (value == null)
			return null;

		InternalCacheValue v = (InternalCacheValue) unmarshall(value);
		return v.toInternalCacheEntry(key);
	}

	private void addNewExpiry(InternalCacheEntry entry) throws IOException {
		long expiry = entry.getExpiryTime();
		if (entry.getMaxIdle() > 0) {
			// Coding getExpiryTime() for transient entries has the risk of
			// being a moving target
			// which could lead to unexpected results, hence, InternalCacheEntry
			// calls are required
			expiry = entry.getMaxIdle() + System.currentTimeMillis();
		}
		Long at = expiry;
		Object key = entry.getKey();

		try {
			expiryEntryQueue.put(new ExpiryEntry(at, key));
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt(); // Restore interruption status
		}
	}

	private static final class ExpiryEntry {
		private final Long expiry;
		private final Object key;

		private ExpiryEntry(long expiry, Object key) {
			this.expiry = expiry;
			this.key = key;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ExpiryEntry other = (ExpiryEntry) obj;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			return true;
		}

	}

}
