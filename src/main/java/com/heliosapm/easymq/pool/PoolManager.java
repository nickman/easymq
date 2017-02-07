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

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;

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
	
	protected final GenericKeyedObjectPoolConfig poolConfig
	 		= new  GenericKeyedObjectPoolConfig();
	
	protected final ObjectMapper jsonMapper = new ObjectMapper();
	
	
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
		jsonMapper.setDefaultPrettyPrinter(new DefaultPrettyPrinter());
		try {
			System.out.println(jsonMapper.writeValueAsString(poolConfig));
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
	}
}
