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

import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>Title: PoolKey</p>
 * <p>Description: Parser and builder for pool keys which are key in the form <b><code>&lt;channel&gt;@&lt;host&gt;:&lt;port&gt;</code></b>.
 * e.g. <b><code>SYSTEM.DEF.SVRCONN@wmq-server-05:1414</code></b>.</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.pool.PoolKey</code></p>
 */

public class PoolKey {
	/** The MQ instance host name or ip address */
	public final String host;
	/** The MQ instance channel to connect to */
	public final String channel;
	/** The MQ instance listening port */
	public final int port;
	/** The string key */
	public final String key;
	
	/** The key parsing regex */
	public static final Pattern KEY_PATTERN = Pattern.compile("(.*?)@(.*?):(\\d+)");

	/** A cache of pool keys */
	private static final ConcurrentHashMap<String, PoolKey> poolKeys = new ConcurrentHashMap<String, PoolKey>(128, 0.75f, Runtime.getRuntime().availableProcessors()); 
	
	/**
	 * Returns the PoolKey for the passed host channel and port
	 * @param host The MQ instance host name or ip address
	 * @param channel The MQ instance channel to connect to
	 * @param port The MQ instance listening port
	 * @return the pool key
	 */
	public static PoolKey poolKey(final String host, final String channel, final int port) {
		final String key = poolKeyString(host, channel, port);
		PoolKey pk = poolKeys.get(key);
		if(pk==null) {
			synchronized(poolKeys) {
				pk = poolKeys.get(key);
				if(pk==null) {
					pk = new PoolKey(host, channel, port, key);
					poolKeys.put(key, pk);
				}
			}
		}
		return pk;
	}
	
	/**
	 * Returns the PoolKey for the passed string key
	 * @param poolKey The pool key string
	 * @return the PoolKey
	 */
	public static PoolKey poolKey(final String poolKey) {
		if(poolKey==null || poolKey.trim().isEmpty()) throw new IllegalArgumentException("The passed pool key was null or empty");
		final String _pk = poolKey.trim();
		final Matcher m = matcher(_pk);
		if(m==null) throw new IllegalArgumentException("The passed pool key [" + poolKey + "] could not be parsed");
		return poolKey(m.group(2), m.group(1), Integer.parseInt(m.group(3)));
	}
	
	/**
	 * Creates a pool key string
	 * @param host The MQ instance host name or ip address
	 * @param channel The MQ instance channel to connect to
	 * @param port The MQ instance listening port
	 * @return the pool key string
	 */
	public static String poolKeyString(final String host, final String channel, final int port) {
		if(host==null || host.trim().isEmpty()) throw new IllegalArgumentException("The passed host was null or empty");
		if(channel==null || channel.trim().isEmpty()) throw new IllegalArgumentException("The passed channel was null or empty");
		if(port < 1 || port > 65535) throw new IllegalArgumentException("Invalid port:" + port);
		return new StringBuilder(channel.trim()).append("@").append(host.trim()).append(":").append(port).toString().intern();
	}
	
	private PoolKey(final String host, final String channel, final int port, final String key) {
		this.host = host.trim();
		this.channel = channel.trim();
		this.port = port;
		this.key = key.trim();
	}

	/**
	 * Indicates if the passed stringy looks like a pool key
	 * @param value The value to test
	 * @return true for match, false otherwise
	 */
	public static boolean matches(final CharSequence value) {
		return matcher(value)==null ? false : true;
	}


	/**
	 * Creates a regex matcher for the passed value
	 * @param value The value to test
	 * @return the matcher if the value matches a pool key string, null otherwise
	 */
	static Matcher matcher(final CharSequence value) {
		if(value==null) return null;
		final Matcher matcher = KEY_PATTERN.matcher(value.toString().trim());
		return matcher.matches() ? matcher : null;
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return key;
	}
	
	
	/**
	 * Returns the pool key as a json string
	 * @return the pool key json
	 */
	public String toJson() {
		return String.format("{\"host\":\"%s\",\"port\":\"%s\",\"channel\":\"%s\"}", host, port, channel);
	}
	
	/**
	 * Returns the base ObjectName for this pool key
	 * @return the base ObjectName for this pool key
	 */
	public String objectName() {
		return new StringBuilder("com.heliosapm.easymq.cache:host=")
			.append(host).append(",")
			.append("port=").append(port).append(",")
			.append("channel=").append(channel)			
			.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + channel.hashCode();
		result = prime * result + host.hashCode();
		result = prime * result + port;
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
		PoolKey other = (PoolKey) obj;
		if (channel == null) {
			if (other.channel != null)
				return false;
		} else if (!channel.equals(other.channel))
			return false;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (port != other.port)
			return false;
		return true;
	}
	
	
}
