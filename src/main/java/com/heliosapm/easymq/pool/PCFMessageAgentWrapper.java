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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.mq.MQException;
import com.ibm.mq.pcf.PCFException;
import com.ibm.mq.pcf.PCFMessage;
import com.ibm.mq.pcf.PCFMessageAgent;

/**
 * <p>Title: PCFMessageAgentWrapper</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.PCFMessageAgentWrapper</code></p>
 */

public class PCFMessageAgentWrapper implements Closeable {
	/** The wrapped message agent */
	private final PCFMessageAgent pcf;
	/** The queue manager name */
	private final String queueManagerName;
	/** The wrapper key */
	public final String key;
	/** Indicates if this wrapper is pooled */
	private final boolean pooled;
	/** The key parsing regex */
	public static final Pattern KEY_PATTERN = Pattern.compile("(.*?)@(.*?):(\\d+)");
	/** Flag indicating if the pcf expiry has been set */
	private static final AtomicBoolean expirySet = new AtomicBoolean(false);
	/** The system property to enable or disable pcf message expiry */
	public static final String PCF_EXPIRY_PROP = "com.ibm.mq.pcf.enablePCFResponseExpiry";
	
	/** The default pcf response message expiry in seconds */
	public static final int DEFAULT_PCF_EXPIRY = 30;
	/** The default pcf response message wait time in seconds */
	public static final int DEFAULT_PCF_WAIT = 30;
	
	/**
	 * Creates a new PCFMessageAgentWrapper
	 * @param host the hostname or IP address where the queue manager resides
	 * @param port the port on which the queue manager listens for incoming channel connections
	 * @param channel the client channel to use for the connection
	 * @param pooled true if this agent is pooled, false otherwise
	 */
	public PCFMessageAgentWrapper(final String host, final int port, final String channel, final boolean pooled) {
		key = key(host, port, channel);
		try {
			pcf = new PCFMessageAgent(host, port, channel);
			queueManagerName = pcf.getQManagerName();
			this.pooled = pooled;
		} catch (MQException mqex) {
			throw new RuntimeException(mqex.getMessage(), mqex);
		}
	}
	
	/**
	 * Creates a new PCFMessageAgentWrapper
	 * @param host the hostname or IP address where the queue manager resides
	 * @param port the port on which the queue manager listens for incoming channel connections
	 * @param channel the client channel to use for the connection
	 */
	public PCFMessageAgentWrapper(final String host, final int port, final String channel) {
		this(host, port, channel, false);
	}
	
	
	/**
	 * Creates a new message agent from the passed key
	 * @param key The message agent key
	 * @param pooled true if this agent is pooled, false otherwise
	 * @return The created and connected message agent
	 */
	public static PCFMessageAgentWrapper fromKey(final String key, final boolean pooled) {
		if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed key was null or empty");
		final Matcher m = KEY_PATTERN.matcher(key.trim());
		if(!m.matches()) throw new IllegalArgumentException("Cannot parse the key [" + key + "]");
		final String host = m.group(2);
		final String channel = m.group(1);
		final int port = Integer.parseInt(m.group(3));
		return new PCFMessageAgentWrapper(host, port, channel, pooled);
	}
	
	/**
	 * Creates a new message agent from the passed key
	 * @param key The message agent key
	 * @param poolFactory The optional pool factory this message agent will be managed by
	 * @return The created and connected message agent
	 */
	public static PCFMessageAgentWrapper fromKey(final String key) {
		return fromKey(key, false);
	}
	
	
	
	
	/**
	 * Generates a message agent key in the form <b><code>&lt;channel&gt;@&lt;host&gt;:&lt;port&gt;</code></b>.
	 * e.g. <b><code>SYSTEM.DEF.SVRCONN@wmq-server-05:1414</code></b>. 
	 * @param host the hostname or IP address where the queue manager resides
	 * @param port the port on which the queue manager listens for incoming channel connections
	 * @param channel the client channel to use for the connection
	 * @return the key
	 */
	public static String key(final String host, final int port, final String channel) {
		if(host==null || host.trim().isEmpty()) throw new IllegalArgumentException("The passed host was null or empty");		
		if(channel==null || channel.trim().isEmpty()) throw new IllegalArgumentException("The passed channel was null or empty");
		if(port < 1 || port > 65535) throw new IllegalArgumentException("Invalid port:" + port);
		return new StringBuilder(channel.trim()).append("@").append(host.trim()).append(":").append(port).toString();
	}
		
	/**
	 * Generates a message agent key in JSON form.
	 * @param host the hostname or IP address where the queue manager resides
	 * @param port the port on which the queue manager listens for incoming channel connections
	 * @param channel the client channel to use for the connection
	 * @return the key in json format
	 */
	public static String json(final String host, final int port, final String channel) {
		return String.format("{\"host\":\"%s\",\"port\":\"%s\",\"channel\":\"%s\"}", host, port, channel);
	}
	/**
	 * Indicates if the system property to enable pcf message expiry is set to false
	 * @return true if the expiry property is set to false
	 */
	public static boolean isExpiryDisabled() {
		return "false".equals(System.getProperty(PCF_EXPIRY_PROP));
	}
	
	/**
	 * Indicates if the system property to enable pcf message expiry is set to true
	 * @return true if the expiry property is enabled
	 */
	public static boolean isExpiryEnabled() {
		return "true".equals(System.getProperty(PCF_EXPIRY_PROP));
	}
	
	
	
	/**
	 * Passivates the message agent if pooled, otherwise calls a hard disconnect
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	public void close() throws IOException {
		if(pooled) {
			PoolManager.getInstance().pool.returnObject(key, this);
		} else {
			try {
				pcf.disconnect();
			} catch (Exception x) {/* No Op */}			
		}
	}

	/**
	 * Frees queue manager resources, and drops the current queue manager connection.
	 * @throws MQException if there was a problem with reading or writing
	 */
	public void disconnect() throws MQException {
		pcf.disconnect();
	}

	/**
	 * Returns the name of the queue manager (if connected).
	 * @return the queue manager
	 */
	public String getQManagerName() {
		return queueManagerName;
	}
	
	/**
	 * Returns the raw agent
	 * @return the raw agent
	 */
	PCFMessageAgent getRawAgent() {
		return pcf;
	}

	/**
	 * Returns the wait interval in seconds.
	 * @return the wait interval
	 */
	public int getWaitInterval() {
		return pcf.getWaitInterval();
	}

	/**
	 * Sends a PCF request to the connected queue manager and returns the responses.
	 * @param pcfMessage the request message
	 * @return an array of PCF response messages. A single PCF request can generate multiple replies.
	 * @throws PCFException if the response indicates an error in PCF processing
	 * @throws MQException if there is a problem with the request or response
	 * @throws IOException if there is a problem with reading or writing
	 */
	public PCFMessage[] send(final PCFMessage pcfMessage) throws PCFException, MQException, IOException {
		return pcf.send(pcfMessage);
	}

	/**
	 * Sets the wait interval and message expiry in seconds
	 * @param waitInterval the wait interval
	 * @param expiry the expiry
	 */
	public void setWaitInterval(final int waitInterval, final int expiry) {
		pcf.setWaitInterval(waitInterval, expiry);
		if(expiry > 0) {  
			if(!isExpiryDisabled() && expirySet.compareAndSet(false, true)) {
				System.setProperty(PCF_EXPIRY_PROP, "true");
			}
		}
	}
	
	
	/**
	 * Returns the pcf message expiry in seconds or zero if not enabled.
	 * @return the expiry
	 */
	public int getExpiry() {
		return isExpiryEnabled() ? pcf.getExpiry() : 0; 
	}
	
	
	/**
	 * Sets the message expiry in seconds
	 * @param expiry the expiry
	 */
	public void setExpiry(final int expiry) {
		setWaitInterval(pcf.getWaitInterval(), expiry);
		
	}

	/**
	 * Sets the wait interval in seconds
	 * @param waitInterval the wait interval
	 */
	public void setWaitInterval(final int waitInterval) {
		pcf.setWaitInterval(waitInterval);
		
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
	 * {@inheritDoc}
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		return pcf.equals(obj);
	}

	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return pcf.hashCode();
	}
	

}
