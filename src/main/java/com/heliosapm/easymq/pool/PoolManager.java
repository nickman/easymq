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
package com.heliosapm.easymq.pool;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.heliosapm.easymq.MQConfig;
import com.heliosapm.easymq.cache.CacheService;
import com.heliosapm.easymq.json.JSONOps;

/**
 * <p>Title: PoolManager</p>
 * <p>Description: Configured and manages pcf connection pools</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.pool.PoolManager</code></p>
 */

public class PoolManager {
	/** The singleton instance */
	private static volatile PoolManager instance = null;
	/** The singleton instance ctor lock */
	private static Object lock = new Object();
	
	
	/** Instance logger */
	protected Logger log = LoggerFactory.getLogger(getClass());
	/** The pool configuration */
	protected final GenericKeyedObjectPoolConfig poolConfig;
	/** The pool */
	protected final GenericKeyedObjectPool<PoolKey, PCFMessageAgentWrapper> pool;
	/** A set of installed pool keys */
	protected final Set<String> poolKeys = new CopyOnWriteArraySet<String>();
	/** A map of pool keys keyed by the pool name */
	protected final Map<String, String> poolNameKeys = new ConcurrentHashMap<String, String>(32, 0.75f, Runtime.getRuntime().availableProcessors());
	/** A map of pool names keyed by the pool key */
	protected final Map<String, String> poolKeyNames = new ConcurrentHashMap<String, String>(32, 0.75f, Runtime.getRuntime().availableProcessors());
	
	
	/** Instance logger */
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	/**
	 * Acquires the pool manager singleton instance
	 * @return the PoolManager singleton
	 */
	public static PoolManager getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {					
					instance = new PoolManager();
				}
			}
		}
		return instance;
	}
	
	public static void main(String[] args) {
		getInstance();
	}
	
	private PoolManager() {
		final JsonNode rootNode = MQConfig.getInstance().getRootNode();
		poolConfig = JSONOps.parseToObject(rootNode.get("poolconfig"), GenericKeyedObjectPoolConfig.class);
		pool = new GenericKeyedObjectPool<PoolKey, PCFMessageAgentWrapper>(PCFAgentPooledObjectFactory.INSTANCE, poolConfig);
		log.info("PCFMessageAgent Pool Started");
		final JsonNode poolDefs = rootNode.get("pools");
		for(JsonNode poolDef: poolDefs) {
			final SubPool subPool = JSONOps.parseToObject(poolDef, SubPool.class);
			installSubPool(subPool);
		}
		Runtime.getRuntime().addShutdownHook(new Thread("PoolManagerShutdown"){
			@Override
			public void run() {
				log.info("Closing PCF Pool...");
				pool.close();
			}
		});
	}
	
	/**
	 * Acquires a connection for the passed key
	 * @param poolKey The pool key
	 * @return the connection
	 */
	public PCFMessageAgentWrapper getConnection(final String poolKey) {
		if(poolKey==null || poolKey.trim().isEmpty()) throw new IllegalArgumentException("The key was null or empty");
		final PoolKey _key = PoolKey.poolKey(poolKey.trim());
		if(!poolKeys.contains(_key)) {
			synchronized(poolKeys) {
				if(!poolKeys.contains(_key)) {
					installSubPool(_key);
				}
			}
		}
		try {
			return pool.borrowObject(_key);
		} catch (Exception ex) {
			log.error("Failed to acquire connection for key [{}]", _key, ex);
			throw new RuntimeException("Failed to acquire connection for key [" + _key + "]", ex);
		}
	}

	
	/**
	 * Installs a new sub pool
	 * @param poolName The assigned pool name
	 * @param host The MQ endpoint host
	 * @param channel The MQ endpoint channel name
	 * @param port The MQ endpoint listening port
	 * @param pcfWait The pcf response message wait time in seconds
	 * @param pcfExpiry The pcf response message expiration time in seconds
	 * @return true if the pool was installed, false otherwise
	 */
	public boolean installSubPool(final String poolName,  final String host, 
			final String channel, final int port, 
			final int pcfWait, final int pcfExpiry) {
		return installSubPool(new SubPool(poolName, host, channel, port, pcfWait, pcfExpiry));
	}
	
	/**
	 * Installs a new sub pool from a pool key
	 * @param key The pool key
	 * @return true if the pool was installed, false otherwise
	 */
	public boolean installSubPool(final String key) {
		return installSubPool(SubPool.fromKey(key));
	}
	
	/**
	 * Installs a new sub pool from a pool key
	 * @param key The pool key
	 * @return true if the pool was installed, false otherwise
	 */
	public boolean installSubPool(final PoolKey key) {
		return installSubPool(SubPool.fromKey(key));
	}
	
	
	/**
	 * Installs a new sub pool
	 * @param subPool The sub pool to install
	 * @return true if the pool was installed, false otherwise
	 */
	public boolean installSubPool(final SubPool subPool) {
		if(subPool==null) throw new IllegalArgumentException("The passed sub pool was null");
		final String pk = subPool.key.toString();
		if(poolKeys.add(pk)) {
			log.info("Installing sub pool [{}]...", subPool.poolName);
			try {
				pool.preparePool(subPool.key);
				poolNameKeys.put(subPool.poolName, pk);
				poolKeyNames.put(pk, subPool.poolName);
				log.info("SubPool [{}] installed", subPool.poolName);
				CacheService.getInstance().getCachesForMQInstance(subPool.key.toString());
				return true;
			} catch (Exception ex) {
				poolKeys.remove(subPool.key);
				log.error("Failed to install pool [{}]", subPool.poolName, ex);				
			}
		} else {
			log.debug("SubPool with key [{}] and name [{}] is already installed", subPool.key, subPool.poolName);
		}
		return false;
	}
	
	/**
	 * Returns the key for the passed pool name
	 * @param poolName The pool name
	 * @return the key or null if not found
	 */
	public String getKeyForName(final String poolName) {
		if(poolName==null || poolName.trim().isEmpty()) throw new IllegalArgumentException("The poolName was null or empty");
		return poolNameKeys.get(poolName.trim());
	}
	
	/**
	 * Returns the name for the passed pool key.
	 * This will be null unless the pool was installed from the config.
	 * @param poolKey The pool key
	 * @return the pool name or null if not found
	 */
	public String getNameForKey(final String poolKey) {
		if(poolKey==null || poolKey.trim().isEmpty()) throw new IllegalArgumentException("The pool key was null or empty");
		return poolKeyNames.get(poolKey.trim());
	}
	
	
}
