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

import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * <p>Title: DelegatingRemovalListener</p>
 * <p>Description: Removal listener which can delegate to other listeners</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.cache.DelegatingRemovalListener</code></p>
 */

public class DelegatingRemovalListener<K, V> implements RemovalListener<K, V> {
	/** The delegate listeners */
	private final Set<RemovalListener<K, V>> delegates = new LinkedHashSet<RemovalListener<K, V>>();
	
	/**
	 * Creates a new DelegatingRemovalListener
	 * @param listeners An optional array of delegates
	 */
	@SafeVarargs
	public DelegatingRemovalListener(final RemovalListener<K, V>...listeners) {
		addDelegates(listeners);
	}
	
	
	/**
	 * {@inheritDoc}
	 * @see com.google.common.cache.RemovalListener#onRemoval(com.google.common.cache.RemovalNotification)
	 */
	@Override
	public void onRemoval(final RemovalNotification<K, V> rn) {
		if(!delegates.isEmpty()) {
			delegates.parallelStream().forEach(l -> l.onRemoval(rn));
		}
	}
	
	
	/**
	 * Adds the passed delegates
	 * @param listeners The delegates to add
	 */
	@SafeVarargs
	public final void addDelegates(final RemovalListener<K, V>...listeners) {
		if(listeners!=null) {
			for(RemovalListener<K, V> rl : listeners) {
				if(rl!=null) {
					delegates.add(rl);
				}
			}
		}		
	}

}
