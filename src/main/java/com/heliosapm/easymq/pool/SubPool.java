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

import java.util.regex.Matcher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>Title: SubPool</p>
 * <p>Description: Defines a MQ endpoint to create a PCF agent sub pool for</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.pool.SubPool</code></p>
 */

public class SubPool {
	/** The assigned pool name */
	final String poolName;
	/** The MQ endpoint host */
	final String host;
	/** The MQ endpoint channel */
	final String channel;
	/** The MQ endpoint port */
	final int port;
	/** The pcf response message wait time in seconds */
	final int pcfWait;
	/** The pcf response message expiration time in seconds */
	final int pcfExpiry;
	/** The pool key for this sub pool */
	final PoolKey key;
	
	
	/**
	 * Creates a new SubPool
	 * @param poolName The assigned pool name
	 * @param host The MQ endpoint host
	 * @param channel The MQ endpoint channel name
	 * @param port The MQ endpoint listening port
	 * @param pcfWait The pcf response message wait time in seconds
	 * @param pcfExpiry The pcf response message expiration time in seconds
	 */
	@JsonCreator
	public SubPool(@JsonProperty("poolName") final String poolName, @JsonProperty("host") final String host, 
			@JsonProperty("channel") final String channel, @JsonProperty("port") final int port, 
			@JsonProperty("pcfWait") final int pcfWait, @JsonProperty("pcfExpiry") final int pcfExpiry) {
		if(poolName==null || poolName.trim().isEmpty()) throw new IllegalArgumentException("The pool name was null or empty");
		if(host==null || host.trim().isEmpty()) throw new IllegalArgumentException("The host was null or empty");
		if(channel==null || channel.trim().isEmpty()) throw new IllegalArgumentException("The channel was null or empty");
		this.poolName = poolName;
		this.host = host;
		this.channel = channel;
		this.port = port;
		this.pcfWait = pcfWait;
		this.pcfExpiry = pcfExpiry;
		key = PoolKey.poolKey(host, channel, port);
	}
	
	/**
	 * Creates a sub pool from a pool key
	 * @param key The key
	 * @return the sub pool
	 */
	public static SubPool fromKey(final String key) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The key was null or empty");
		final PoolKey p = PoolKey.poolKey(key.trim());
		return new SubPool(p.toString(), p.host, p.channel, p.port, PCFMessageAgentWrapper.DEFAULT_PCF_WAIT, PCFMessageAgentWrapper.DEFAULT_PCF_EXPIRY);
	}
	
	/**
	 * Creates a sub pool from a PCFMessageAgentWrapper key
	 * @param key The key
	 * @return the sub pool
	 */
	public static SubPool fromKey(final PoolKey p) {
		if(p==null) throw new IllegalArgumentException("The key was null");
		return new SubPool(p.toString(), p.host, p.channel, p.port, PCFMessageAgentWrapper.DEFAULT_PCF_WAIT, PCFMessageAgentWrapper.DEFAULT_PCF_EXPIRY);
	}

	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SubPool:" + key.toString();
	}

	public String getPoolName() {
		return poolName;
	}

	public String getHost() {
		return host;
	}

	public String getChannel() {
		return channel;
	}

	public int getPort() {
		return port;
	}

	public int getPcfWait() {
		return pcfWait;
	}

	public int getPcfExpiry() {
		return pcfExpiry;
	}

	public PoolKey getPoolKey() {
		return key;
	}
	
}


//"name" : "mq8",
//"host" : "192.168.1.13",
//"port" : 1414,
//"channel" : "SYSTEM.DEF.SVRCONN",
//"pcfwait" : 5,
//"pcfexpiry" : 5			
