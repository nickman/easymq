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

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.ibm.mq.pcf.PCFMessageAgent;

/**
 * <p>Title: PCFAgentPooledObjectFactory</p>
 * <p>Description: Pooled object factory to fuel pcf message agent pools</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.PCFAgentPooledObjectFactory</code></p>
 */

public class PCFAgentPooledObjectFactory extends BaseKeyedPooledObjectFactory<String, PCFMessageAgentWrapper> {
	/** Shareable instance */
	public static final PCFAgentPooledObjectFactory INSTANCE = new PCFAgentPooledObjectFactory();
	
	private PCFAgentPooledObjectFactory() {}
	
	/**
	 * {@inheritDoc}
	 * @see org.apache.commons.pool2.BaseKeyedPooledObjectFactory#create(java.lang.Object)
	 */
	@Override
	public PCFMessageAgentWrapper create(final String key) throws Exception {
		return PCFMessageAgentWrapper.fromKey(key, true);
	}

	/**
	 * {@inheritDoc}
	 * @see org.apache.commons.pool2.BaseKeyedPooledObjectFactory#wrap(java.lang.Object)
	 */
	@Override
	public PooledObject<PCFMessageAgentWrapper> wrap(final PCFMessageAgentWrapper value) {		
		return new DefaultPooledObject<PCFMessageAgentWrapper>(value);
	}
	
	@Override
	public boolean validateObject(final String key, final PooledObject<PCFMessageAgentWrapper> p) {
		try {
			final PCFMessageAgentWrapper wrapper = p.getObject();
			final PCFMessageAgent agent = wrapper.getRawAgent();
			return agent.getQManagerName().equals(wrapper.getQManagerName());
		} catch (Exception ex) {
			return false;
		}
	}

}
