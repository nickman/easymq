// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package com.heliosapm.easymq.cache;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.heliosapm.easymq.MQConfig;
import com.heliosapm.easymq.json.JSONOps;
import com.heliosapm.easymq.pool.PoolKey;

/**
 * <p>Title: CacheService</p>
 * <p>Description: Configures and manages MQ artifact caches</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.cache.CacheService</code></p>
 */

public class CacheService {
	/** The singleton instance */
	private static volatile CacheService instance = null;
	/** The singleton instance ctor lock */
	private static Object lock = new Object();
		
	/** Instance logger */
	protected Logger log = LoggerFactory.getLogger(getClass());
	/** The caches keyed by cache name within a map keyed by MQ instance key */
	protected final ConcurrentHashMap<String, ConcurrentHashMap<String, Cache<?, ?>>> caches = new ConcurrentHashMap<String, ConcurrentHashMap<String, Cache<?, ?>>>(128, 0.75f, Runtime.getRuntime().availableProcessors()); 
	
	/**
	 * Acquires the CacheService singleton instance
	 * @return the CacheService singleton
	 */
	public static CacheService getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {					
					instance = new CacheService();
				}
			}
		}
		return instance;
	}

	
	/**
	 * Stores a value to a named instance cache
	 * @param poolKey The MQ instance pool key
	 * @param cacheName The name of the cache to insert into
	 * @param key The cache put key
	 * @param value The cache put value
	 */
	public void put(final String poolKey, final String cacheName, final Object key, final Object value) {
		@SuppressWarnings("unchecked")
		final Cache<Object,Object> cache = (Cache<Object, Object>) getNamedInstanceCache(poolKey, cacheName);
		cache.put(key, value);
	}

	/**
	 * Retrieves a value from an instance cache
	 * @param poolKey The pool key of the MQ instance
	 * @param cacheName The instance cache name
	 * @param key The cache key
	 * @param loader The loader to fetch the value if it is not in cache
	 * @return the value or null if not found
	 */
	public Object get(final String poolKey, final String cacheName, final Object key, final Callable<Object> loader) {
		@SuppressWarnings("unchecked")
		final Cache<Object, Object> cache = (Cache<Object, Object>)getNamedInstanceCache(poolKey, cacheName);
		try {
			// Map<String, Map<QueueAttribute, Object>>> fetchQueues = new Callable<Map<String, Map<QueueAttribute, Object>>>() {
			return loader==null ? cache.asMap().get(key) : cache.get(key, loader);
		} catch (Exception ex) {
			throw new RuntimeException(ex.getMessage(), ex);
		}
	}
	
	/**
	 * Retrieves all values from an instance cache
	 * @param poolKey The pool key of the MQ instance
	 * @param cacheName The instance cache name
	 * @param loader The loader to fetch the value if it is not in cache
	 * @return the value or null if not found
	 */
	@SuppressWarnings("unchecked")
	public <K,V> Map<K,V> get(final String poolKey, final String cacheName, final Callable<Map<?,?>> loader) {
		@SuppressWarnings("unchecked")
		final Cache<K, V> cache = (Cache<K, V>)getNamedInstanceCache(poolKey, cacheName);
		if(cache.asMap().isEmpty()) {
			final Map<K,V> loaded;
			try {
				loaded = (Map<K, V>) loader.call();
			} catch (Exception ex) {
				log.error("Failed to load cache [{}/{}] with loader [{}]", poolKey, cacheName, loader, ex);
				throw new RuntimeException(ex.getMessage(),ex);
			}
			loaded.entrySet().parallelStream().forEach(entry -> {
				cache.put(entry.getKey(), entry.getValue());
			});			
		}
		return new HashMap<K,V>(cache.asMap());
	}
	
	
	/**
	 * Retrieves a value from an instance cache
	 * @param poolKey The pool key of the MQ instance
	 * @param cacheName The instance cache name
	 * @param key The cache key
	 * @return the value or null if not found
	 */
	public Object get(final String poolKey, final String cacheName, final Object key) {
		return get(poolKey, cacheName, key, null);
	}
	
	
	public static void main(String[] args) {
		System.out.println(defaultCacheSpec(null, true));
	}
	
	/** The default cache spec */
	protected final String defaultCacheSpec;
	/** Flag indicating if caches should register a JMX management interface */
	protected final boolean enableJmx;
	/** The default instance cache specs */
	protected final Map<String, String> instanceSpecs = new HashMap<String, String>();
	/** An empty node const */
	private static final JsonNode EMPTY_NODE = JSONOps.getNodeFactory().nullNode();
	/** The default concurrency level for caches */
	public static final String DEFAULT_CONCURRENCY = "concurrencyLevel=" + Runtime.getRuntime().availableProcessors();
	/** The default cache spec */
	public static final String DEFAULT_SPEC = new StringBuilder(DEFAULT_CONCURRENCY).append(",")
		.append("initialCapacity=").append(1024).append(",")
		.append("maximumSize=").append(8192).append(",")
		.append("expireAfterWrite=").append("2m")		
		.toString();
	
	
//	"defaultConfig" : "",
//	"jmx" : true,
//	"caches" : {
//		"queuenames" : ""
//	}
	
	
	private CacheService() {
		final JsonNode cacheNode = MQConfig.getInstance().getConfigNode("cacheconfig");
		enableJmx = cacheNode.get("jmx").asBoolean(true);
		defaultCacheSpec = defaultCacheSpec(
				nvl(cacheNode.get("defaultCacheSpec")).asText(""),
				enableJmx
		);
		final JsonNode cacheInstancesNode = nvl(cacheNode.get("caches"));		
		if(cacheInstancesNode!=EMPTY_NODE) {
			int specCount = 0;
			for(final Iterator<Entry<String, JsonNode>> iter = cacheInstancesNode.fields(); iter.hasNext();) {
				final Entry<String, JsonNode> entry = iter.next();
				instanceSpecs.put(entry.getKey(), defaultCacheSpec(entry.getValue().textValue(), enableJmx));
				specCount++;
			}
			log.info("Loaded [{}] cache specs", specCount);
		}
		
		
	}
	
	/**
	 * Returns (creating if necessary) instance caches for the passed MQ instance key
	 * @param key The instance key
	 * @return the instance caches map
	 */
	public ConcurrentHashMap<String, Cache<?,?>> getCachesForMQInstance(final String key) {
		ConcurrentHashMap<String, Cache<?,?>> allInstanceCaches = caches.get(key);
		if(allInstanceCaches==null) {
			synchronized(caches) {
				allInstanceCaches = caches.get(key);
				if(allInstanceCaches==null) {				
					final StringBuilder b = new StringBuilder("===Created instance caches for [").append(key).append("]:");
					allInstanceCaches = new ConcurrentHashMap<String, Cache<?,?>>(64, 0.75f, Runtime.getRuntime().availableProcessors());
					for(final Map.Entry<String, String> entry: instanceSpecs.entrySet()) {
						final DelegatingRemovalListener<?, ?> listener = new DelegatingRemovalListener<Object, Object> (); 
						final Cache<?,?> cache = CacheBuilder
								.from(entry.getValue())
								.removalListener(listener)
								.build();
						registerCacheMBean(key,entry.getKey(), cache, listener);
						allInstanceCaches.put(entry.getKey(), cache);
						b.append("\n\t").append(entry.getKey());					
					}
					b.append("\n===");
					caches.put(key, allInstanceCaches);
					log.info(b.toString());
				}
			}
		}		
		return allInstanceCaches;
	}
	
	protected void registerCacheMBean(final String poolKey, final String cacheName, final Cache<?,?> cache, final DelegatingRemovalListener<?, ?> listener) {
		if(enableJmx) {
			try {
				final PoolKey pk = PoolKey.poolKey(poolKey);
				final ObjectName on = new ObjectName(pk.objectName() + ",cacheName=" + cacheName);
				final CacheStatistics cs = new CacheStatistics(cache, on, listener);
				ManagementFactory.getPlatformMBeanServer().registerMBean(cs, on);
				
			} catch (Exception ex) {
				log.warn("Failed to create CacheMBean for [{}/{}]", ex);
			}
		}
	}
	
	/**
	 * Returns (creating if necessary) the named instance cache for the passed MQ instance key and cache name
	 * @param poolKey The instance key
	 * @param cacheName The cache name
	 * @return the named cache
	 */
	public Cache<?,?> getNamedInstanceCache(final String poolKey, final String cacheName) {
		final ConcurrentHashMap<String, Cache<?,?>> instanceCache = getCachesForMQInstance(poolKey);
		Cache<?,?> cache = instanceCache.get(cacheName);
		if(cache==null) {
			synchronized(instanceCache) {
				cache = instanceCache.get(cacheName);
				if(cache==null) {
					final DelegatingRemovalListener<?, ?> listener = new DelegatingRemovalListener<Object, Object> (); 
					
					cache = CacheBuilder
						.from(defaultCacheSpec(null, enableJmx))
						.removalListener(listener)
						.build();
					registerCacheMBean(poolKey, cacheName, cache, listener);
					instanceCache.put(cacheName, cache);					
				}
			}
		}
		return cache;		
	}
	

	
	protected static String defaultCacheSpec(final String baseSpec, final boolean enableStats) {
		if(baseSpec==null || baseSpec.trim().isEmpty()) {
			return DEFAULT_SPEC + (enableStats ? ",recordStats" : "");
		}
		return baseSpec + (enableStats ? ",recordStats" : "");
	}
	
	protected static JsonNode nvl(final JsonNode node) {
		return node==null ? EMPTY_NODE : node;
	}


}
