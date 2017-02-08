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
package com.heliosapm.easymq;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.heliosapm.easymq.json.JSONOps;
import com.heliosapm.easymq.pool.PoolManager;

/**
 * <p>Title: MQConfig</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.MQConfig</code></p>
 */

public class MQConfig {
	/** The singleton instance */
	private static volatile MQConfig instance = null;
	/** The singleton instance ctor lock */
	private static Object lock = new Object();

	/** The system property key for the easymq json configuration source */
	public static final String POOL_CONFIG_PROP = "com.heliosapm.easymq.config";
	/** The environmental variable key for the easymq json configuration source */
	public static final String POOL_CONFIG_ENV = "EASYMQ_CONFIG";
	/** The default pool configuration */
	public static final String DEFAULT_CONFIG = "config/config.json";
	
	/** The root config node */
	private final JsonNode rootNode;
	/** The json config nodes keyed by name */
	private final Map<String, JsonNode> configNodes = new HashMap<String, JsonNode>();
	/** Instance logger */
	protected Logger log = LoggerFactory.getLogger(getClass());

	
	/**
	 * Acquires the MQConfig singleton instance
	 * @return the MQConfig singleton
	 */
	public static MQConfig getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {					
					instance = new MQConfig();
				}
			}
		}
		return instance;
	}
	
	private MQConfig() {
		final String conf = System.getProperty(POOL_CONFIG_PROP, System.getenv(POOL_CONFIG_ENV));
		final URL url = getConfig(conf);
		log.info("Loading config from [{}]", url);
		rootNode = JSONOps.parseToNode(url);
		for(final Iterator<Entry<String, JsonNode>> iter = rootNode.fields(); iter.hasNext();) {
			final Entry<String, JsonNode> entry = iter.next();
			configNodes.put(entry.getKey(), entry.getValue());
			log.info("Loaded JsonNode [{}]", entry.getKey());
		}
		if(configNodes.containsKey("sysprops")) {
			final JsonNode sysPropsNode = configNodes.get("sysprops");
			int propsSet = 0;
			for(final Iterator<Entry<String, JsonNode>> iter = sysPropsNode.fields(); iter.hasNext();) {
				final Entry<String, JsonNode> entry = iter.next();
				System.setProperty(entry.getKey(), entry.getValue().asText());
				propsSet++;
			}
			log.info("Set {} system properties", propsSet);
		}
	}
	
	/**
	 * Returns the configuration root node
	 * @return the configuration root node
	 */
	public JsonNode getRootNode() {
		return rootNode;
	}
	
	/**
	 * Returns the config node for the passed key
	 * @param key The config node key
	 * @return The config node or null if not found
	 */
	public JsonNode getConfigNode(final String key) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed config key was null or empty");
		return configNodes.get(key.trim());
	}
	
	/**
	 * Locates and returns the URL of the configuration json
	 * @param url The configured URL 
	 * @return the actual URL
	 */
	public static URL getConfig(final String url) {
		URL configUrl = fromString(url);
		return configUrl!=null ? configUrl : PoolManager.class.getClassLoader().getResource(DEFAULT_CONFIG);
	}
	
	/**
	 * Attempts to convert the passed string to a {@link URL} in the following order:<ol>
	 * 	<li>Direct conversion using {@link URL#URL(String)}</li>
	 *  <li>If the supplied string evaluates to a readable file, the <b><code>file:/</code></b> protocol URL of the file is returned</li>
	 *  <li>Attempts to read as a resource URL from the classloader</li>
	 * </ol>
	 * If all fail, returns null.
	 * @param url The string to build a URL from
	 * @return the built URL or null if one could not be built
	 */
	public static URL fromString(final String url) {
		URL resolvedUrl = null;
		
		if(url==null || url.trim().isEmpty()) {
			return null;
		}
		try {
			resolvedUrl = new URL(url.trim());
			return resolvedUrl;
		} catch (MalformedURLException e) {/* No Op */}
		File f = new File(url.trim());
		if(f.canRead()) {
			try {				
				return f.getAbsoluteFile().toURI().toURL();
			} catch (MalformedURLException e) {/* No Op */}
		}
		final ClassLoader cl = PoolManager.class.getClassLoader();
		return cl.getResource(url.trim());
	}
	

	
}
