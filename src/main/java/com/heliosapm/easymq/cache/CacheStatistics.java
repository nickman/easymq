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

import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import javax.management.ObjectName;

import com.codahale.metrics.CachedGauge;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * <p>Title: CacheStatistics</p>
 * <p>Description: Guava cache JMX managed stats</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.cache.CacheStatistics</code></p>
 */
@SuppressWarnings("rawtypes")
public class CacheStatistics implements CacheStatisticsMBean, RemovalListener {
	/** The wrapped cache instance */
	protected final Cache<?, ?> cache;	
	/** The cache stats JMX object name */
	protected final ObjectName objectName;
	/** Removal cause counters */
	protected final EnumMap<RemovalCause, LongAdder> removalCauseCounts = new EnumMap<RemovalCause, LongAdder>(RemovalCause.class);
	
	private static final RemovalCause[] REMOVAL_CAUSES = RemovalCause.values(); 
	
	/** A cache stats gauge */
	protected final CachedGauge<CacheStats> cacheStats = new CachedGauge<CacheStats>(2, TimeUnit.SECONDS) {
		@Override
		protected CacheStats loadValue() {			
			return cache.stats();
		}
	};

	/**
	 * Creates a new CacheStatistics
	 * @param cache The guava cache instance to wrap
	 * @param objectName The assigned JMX ObjectName for this cache
	 */
	@SuppressWarnings("unchecked")
	public CacheStatistics(final Cache<?, ?> cache, final ObjectName objectName, final DelegatingRemovalListener<?,?> listener) {
		this.cache = cache;
		this.objectName = objectName;		
		listener.addDelegates(this);
		for(RemovalCause rc: REMOVAL_CAUSES) {
			removalCauseCounts.put(rc, new LongAdder());			
		}
	}

	/**
	 * {@inheritDoc}
	 * @see com.google.common.cache.RemovalListener#onRemoval(com.google.common.cache.RemovalNotification)
	 */
	@Override
	public void onRemoval(final RemovalNotification rc) {
		removalCauseCounts.get(rc.getCause()).increment();		
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.easymq.cache.CacheStatisticsMBean#getRemovalCauseCounts()
	 */
	@Override
	public Map<String, Long> getRemovalCauseCounts() {
		final Map<String, Long> map = new LinkedHashMap<String, Long>(REMOVAL_CAUSES.length);
		for(Map.Entry<RemovalCause, LongAdder> entry : removalCauseCounts.entrySet()) {
			map.put(entry.getKey().name(), entry.getValue().longValue());
		}
		return map;
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#invalidateAll()
	 */
	@Override
	public void invalidateAll() {
		cache.invalidateAll();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#cleanup()
	 */
	@Override
	public void cleanup() {
		cache.cleanUp();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getSize()
	 */
	@Override
	public long getSize() {
		return cache.size();
	}
	
	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getRequestCount()
	 */
	@Override
	public long getRequestCount() {
		return cacheStats.getValue().requestCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getHitCount()
	 */
	@Override
	public long getHitCount() {
		return cacheStats.getValue().hitCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getHitRate()
	 */
	@Override
	public double getHitRate() {
		return cacheStats.getValue().hitRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getMissCount()
	 */
	@Override
	public long getMissCount() {
		return cacheStats.getValue().missCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getMissRate()
	 */
	@Override
	public double getMissRate() {
		return cacheStats.getValue().missRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getLoadCount()
	 */
	@Override
	public long getLoadCount() {
		return cacheStats.getValue().loadCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getLoadSuccessCount()
	 */
	@Override
	public long getLoadSuccessCount() {
		return cacheStats.getValue().loadSuccessCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getLoadExceptionCount()
	 */
	@Override
	public long getLoadExceptionCount() {
		return cacheStats.getValue().loadExceptionCount();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getLoadExceptionRate()
	 */
	@Override
	public double getLoadExceptionRate() {
		return cacheStats.getValue().loadExceptionRate();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getTotalLoadTime()
	 */
	@Override
	public long getTotalLoadTime() {
		return cacheStats.getValue().totalLoadTime();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getAverageLoadPenalty()
	 */
	@Override
	public double getAverageLoadPenalty() {
		return cacheStats.getValue().averageLoadPenalty();
	}

	/**
	 * {@inheritDoc}
	 * @see com.heliosapm.tsdbex.CacheStatisticsMBean.CacheStatisticsMXBean#getEvictionCount()
	 */
	@Override
	public long getEvictionCount() {
		return cacheStats.getValue().evictionCount();
	}

}
