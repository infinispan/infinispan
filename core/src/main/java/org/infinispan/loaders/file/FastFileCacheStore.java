/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.file;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.*;
import org.infinispan.loaders.*;
import org.infinispan.marshall.StreamingMarshaller;

/**
 * A filesystem-based implementation of a {@link CacheStore}. This file store
 * stores cache values in a single file
 * <tt>&lt;location&gt;/&lt;cache name&gt;.dat</tt>, keys and file positions are
 * kept in memory.
 * <p/>
 * Note: this CacheStore implementation keeps keys and file positions in memory!
 * The current implementation needs about 100 bytes per cache entry, plus the
 * memory for the key objects. Use the maxEntries parameter or cache entries
 * that can expire (with purge) to prevent the cache store from growing
 * indefinitely and causing OutOfMemoryExceptions.
 * <p/>
 * This class is fully thread safe, yet allows for concurrent load / store of
 * individual cache entries.
 * 
 * @author Karsten Blees
 */
@CacheLoaderMetadata(configurationClass = FastFileCacheStoreConfig.class)
public class FastFileCacheStore extends AbstractCacheStore implements
	Externalizable
{
	private static final byte[] MAGIC = new byte[]
	{ 'F', 'C', 'S', '1' };

	// file header is MAGIC + long + int
	private static final int HEADER_SIZE = MAGIC.length + 8 + 4;

	private FastFileCacheStoreConfig config;

	private RandomAccessFile file;

	private Map<Object, FileEntry> entries;

	private SortedSet<FileEntry> freeList;

	private long filePos = HEADER_SIZE;

	/** {@inheritDoc} */
	@Override
	public Class<? extends CacheLoaderConfig> getConfigurationClass()
	{
		return FastFileCacheStoreConfig.class;
	}

	/** {@inheritDoc} */
	@Override
	public void init(CacheLoaderConfig config, Cache<?, ?> cache,
		StreamingMarshaller m) throws CacheLoaderException
	{
		super.init(config, cache, m);
		this.config = (FastFileCacheStoreConfig) config;
	}

	/** {@inheritDoc} */
	@Override
	public void start() throws CacheLoaderException
	{
		super.start();
		try
		{
			// open the data file
			String location = config.getLocation();
			if (location == null || location.trim().length() == 0)
				location = "Infinispan-FileCacheStore";
			File dir = new File(location);
			if (!dir.exists() && !dir.mkdirs())
				throw new ConfigurationException("Directory " + dir.getAbsolutePath()
					+ " does not exist and cannot be created!");

			File f = new File(location, cache.getName() + ".dat");
			file = new RandomAccessFile(f, "rw");

			// initialize data structures
			// only use LinkedHashMap (LRU) for entries when cache store is bounded
			final Map entryMap;
			if (config.getMaxEntries() > 0)
				entryMap = new LinkedHashMap<Object, FileEntry>(16, 0.75f, true);
			else
				entryMap = new HashMap();
			entries = Collections.synchronizedMap(entryMap);
			freeList = Collections.synchronizedSortedSet(new TreeSet<FileEntry>());

			// read the index from file if enabled, otherwise reset file pointer
			if (file.length() > HEADER_SIZE
				&& Boolean.TRUE.equals(config.isFetchPersistentState()))
				readIndex();
			else
				file.setLength(0);

			// index is now in memory, invalidate index on disk
			writeHeader(null);
		}
		catch (Exception e)
		{
			throw new CacheLoaderException(e);
		}
	}

	/** {@inheritDoc} */
	@Override
	public void stop() throws CacheLoaderException
	{
		try
		{
			if (file != null)
			{
				// write in-memory index to the file if persistent caching is enabled
				if (Boolean.TRUE.equals(config.isFetchPersistentState()))
					writeIndex();

				// reset state
				file.close();
				file = null;
				entries = null;
				freeList = null;
				filePos = HEADER_SIZE;
			}
		}
		catch (Exception e)
		{
			throw new CacheLoaderException(e);
		}
		super.stop();
	}

	/**
	 * Read state of the index from the cache file if enabled.
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void readIndex() throws IOException, ClassNotFoundException
	{
		// read file header
		ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE);
		file.getChannel().read(buf);
		buf.flip();

		// don't read index if the file header / version doesn't match
		byte[] magic = new byte[4];
		buf.get(magic);
		if (!Arrays.equals(MAGIC, magic))
			return;

		// get offset + length of index
		FileEntry fe = new FileEntry();
		fe.offset = buf.getLong();
		if (fe.offset <= 0)
			return;

		// read and deserialize index
		fe.len = buf.getInt();
		byte[] data = new byte[fe.len];
		file.getChannel().read(ByteBuffer.wrap(data), fe.offset);
		ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(data));
		readExternal(in);
		in.close();
	}

	/**
	 * Writes the file header.
	 * 
	 * @param index optional file position and length of serialized index
	 * @throws IOException
	 */
	private void writeHeader(FileEntry index) throws IOException
	{
		ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE);
		buf.put(MAGIC);
		buf.putLong(index == null ? 0 : index.offset);
		buf.putInt(index == null ? 0 : index.len);
		buf.flip();
		file.getChannel().write(buf, 0);
	}

	/**
	 * Write the current state of the index to the cache file.
	 * 
	 * @throws IOException
	 */
	private void writeIndex() throws IOException
	{
		// serializes current state
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		writeExternal(oos);
		oos.flush();
		oos.close();
		byte[] data = baos.toByteArray();

		// store index in a free section of the file
		FileEntry index = allocate(data.length);
		file.getChannel().write(ByteBuffer.wrap(data), index.offset);

		// store offset + length of the index in the header
		writeHeader(index);
	}

	/**
	 * {@inheritDoc}
	 * <p/>
	 * The base class implementation calls {@link #load(Object)} for this, we can
	 * do better because we keep all keys in memory.
	 */
	@Override
	public boolean containsKey(Object key) throws CacheLoaderException
	{
		return entries.containsKey(key);
	}

	/**
	 * Allocates the requested space in the file.
	 * 
	 * @param len requested space
	 * @return allocated file position and length as FileEntry object
	 */
	private FileEntry allocate(int len)
	{
		FileEntry fe = new FileEntry();
		fe.len = fe.size = len;
		synchronized (freeList)
		{
			// lookup a free entry of sufficient size
			SortedSet<FileEntry> candidates = freeList.tailSet(fe);
			for (Iterator<FileEntry> it = candidates.iterator(); it.hasNext();)
			{
				FileEntry free = it.next();
				// ignore entries that are still in use by concurrent readers
				if (free.isLocked())
					continue;

				// found one, remove from freeList and initialize requested length (may
				// be smaller than size)
				it.remove();
				free.len = len;
				return free;
			}

			// no appropriate free section available, append at end of file
			fe.offset = filePos;
			filePos += fe.size;
		}
		return fe;
	}

	/**
	 * Frees the space of the specified file entry (for reuse by allocate).
	 * 
	 * @param fe FileEntry to free
	 */
	private void free(FileEntry fe)
	{
		if (fe != null)
			freeList.add(fe);
	}

	/** {@inheritDoc} */
	@Override
	public void store(InternalCacheEntry entry) throws CacheLoaderException
	{
		FileEntry fe = null;
		try
		{
			// serialize cache value
			byte[] data = getMarshaller().objectToByteBuffer(
				entry.toInternalCacheValue());

			// allocate file entry and store in cache file
			fe = allocate(data.length);
			fe.expiryTime = entry.getExpiryTime();
			file.getChannel().write(ByteBuffer.wrap(data), fe.offset);

			// add the new entry to in-memory index
			fe = entries.put(entry.getKey(), fe);

			// if we added an entry, check if we need to evict something
			if (fe == null)
				fe = evict();
		}
		catch (Exception e)
		{
			throw new CacheLoaderException(e);
		}
		finally
		{
			// in case we replaced or evicted an entry, add to freeList
			free(fe);
		}
	}

	/**
	 * Try to evict an entry if the capacity of the cache store is reached.
	 * 
	 * @return FileEntry to evict, or null (if unbounded or capacity is not yet
	 *         reached)
	 */
	private FileEntry evict()
	{
		if (config.getMaxEntries() > 0)
			synchronized (entries)
			{
				if (entries.size() > config.getMaxEntries())
				{
					Iterator<FileEntry> it = entries.values().iterator();
					FileEntry fe = it.next();
					it.remove();
					return fe;
				}
			}
		return null;
	}

	/** {@inheritDoc} */
	@Override
	public void clear() throws CacheLoaderException
	{
		try
		{
			synchronized (entries)
			{
				synchronized (freeList)
				{
					// wait until all readers are done reading file entries
					for (FileEntry fe : entries.values())
						fe.waitUnlocked();
					for (FileEntry fe : freeList)
						fe.waitUnlocked();

					// clear in-memory state
					entries.clear();
					freeList.clear();
					filePos = HEADER_SIZE;
				}
			}
		}
		catch (Exception e)
		{
			throw new CacheLoaderException(e);
		}
	}

	/** {@inheritDoc} */
	@Override
	public boolean remove(Object key) throws CacheLoaderException
	{
		FileEntry fe = entries.remove(key);
		free(fe);
		return fe != null;
	}

	/** {@inheritDoc} */
	@Override
	public InternalCacheEntry load(Object key) throws CacheLoaderException
	{
		try
		{
			final FileEntry fe;
			synchronized (entries)
			{
				// lookup FileEntry of the key
				fe = entries.get(key);
				if (fe == null)
					return null;

				// lock entry for reading before releasing entries monitor
				fe.lock();
			}

			final byte[] data;
			try
			{
				// check if entry is expired
				if (fe.expiryTime > 0 && fe.expiryTime < System.currentTimeMillis())
				{
					free(fe);
					return null;
				}

				// load serialized data from disk
				data = new byte[fe.len];
				file.getChannel().read(ByteBuffer.wrap(data), fe.offset);
			}
			finally
			{
				// no need to keep the lock for deserialization
				fe.unlock();
			}

			// deserialize data and recreate InternalCacheEntry
			return ((InternalCacheValue) getMarshaller().objectFromByteBuffer(data))
				.toInternalCacheEntry(key);
		}
		catch (Exception e)
		{
			throw new CacheLoaderException(e);
		}
	}

	/** {@inheritDoc} */
	@Override
	public Set<InternalCacheEntry> loadAll() throws CacheLoaderException
	{
		return load(Integer.MAX_VALUE);
	}

	/** {@inheritDoc} */
	@Override
	public Set<InternalCacheEntry> load(int numEntries)
		throws CacheLoaderException
	{
		Set<Object> keys = loadAllKeys(null);
		Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>();
		for (Object key : keys)
		{
			InternalCacheEntry ice = load(key);
			if (ice != null)
			{
				result.add(ice);
				if (result.size() > numEntries)
					return result;
			}
		}
		return result;
	}

	/** {@inheritDoc} */
	@Override
	public Set<Object> loadAllKeys(Set<Object> keysToExclude)
		throws CacheLoaderException
	{
		Set<Object> result;
		synchronized (entries)
		{
			result = new HashSet(entries.keySet());
		}
		if (keysToExclude != null)
			result.removeAll(keysToExclude);
		return result;
	}

	/** {@inheritDoc} */
	@Override
	protected void purgeInternal() throws CacheLoaderException
	{
		long now = System.currentTimeMillis();
		synchronized (entries)
		{
			for (Iterator<FileEntry> it = entries.values().iterator(); it.hasNext();)
			{
				FileEntry fe = it.next();
				if (fe.expiryTime > 0 && fe.expiryTime < now)
				{
					it.remove();
					free(fe);
				}
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public void writeExternal(ObjectOutput out) throws IOException
	{
		synchronized (entries)
		{
			synchronized (freeList)
			{
				out.writeLong(filePos);
				// write entries map
				out.writeInt(entries.size());
				for (Map.Entry<Object, FileEntry> me : entries.entrySet())
				{
					out.writeObject(me.getKey());
					me.getValue().writeExternal(out);
				}
				// write free list
				out.writeInt(freeList.size());
				for (FileEntry fe : freeList)
					fe.writeExternal(out);
			}
		}
	}

	/** {@inheritDoc} */
	@Override
	public void readExternal(ObjectInput in) throws IOException,
		ClassNotFoundException
	{
		synchronized (entries)
		{
			synchronized (freeList)
			{
				filePos = in.readLong();
				// read entries map
				int sz = in.readInt();
				for (int i = 0; i < sz; i++)
				{
					Object key = in.readObject();
					FileEntry fe = new FileEntry();
					fe.readExternal(in);
					entries.put(key, fe);
				}
				// read free list
				sz = in.readInt();
				for (int i = 0; i < sz; i++)
				{
					FileEntry fe = new FileEntry();
					fe.readExternal(in);
				}
			}
		}
	}

	/** {@inheritDoc} */
	public void fromStream(ObjectInput inputStream) throws CacheLoaderException
	{
		// seems that this is never called by Infinispan (except by decorators)
		throw new UnsupportedOperationException();
	}

	/** {@inheritDoc} */
	public void toStream(ObjectOutput outputStream) throws CacheLoaderException
	{
		// seems that this is never called by Infinispan (except by decorators)
		throw new UnsupportedOperationException();
	}

	/**
	 * Helper class to represent a section of the cache file.
	 */
	private static class FileEntry implements Comparable, Externalizable
	{
		// file offset of this block (never changes, not final for readExternal)
		private long offset;

		// total size of this block (never changes, not final for readExternal)
		private int size;

		// used size of this block
		private int len;

		// number of current readers
		private int readers = 0;

		// timestamp when the entry will expire (i.e. will be collected by purge)
		private long expiryTime = -1;

		private synchronized boolean isLocked()
		{
			return readers > 0;
		}

		private synchronized void lock()
		{
			readers++;
		}

		private synchronized void unlock()
		{
			readers--;
			if (readers == 0)
				notifyAll();
		}

		private synchronized void waitUnlocked()
		{
			while (readers > 0)
			{
				try
				{
					wait();
				}
				catch (InterruptedException e)
				{
					// ignore, we don't expect anyone to interrupt us
				}
			}
		}

		/** {@inheritDoc} */
		public void writeExternal(ObjectOutput out) throws IOException
		{
			out.writeLong(offset);
			out.writeInt(size);
			out.writeInt(len);
			out.writeLong(expiryTime);
		}

		/** {@inheritDoc} */
		public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException
		{
			offset = in.readLong();
			size = in.readInt();
			len = in.readInt();
			expiryTime = in.readLong();
			readers = 0;
		}

		/** {@inheritDoc} */
		public int compareTo(Object o)
		{
			if (!(o instanceof FileEntry))
				throw new ClassCastException();
			if (this == o)
				return 0;
			FileEntry fe = (FileEntry) o;
			int diff = size - fe.size;
			return (diff != 0) ? diff : offset > fe.offset ? 1 : -1;
		}
	}
}
