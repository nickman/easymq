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

import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.servlet.http.HttpSessionBindingEvent;
import javax.servlet.http.HttpSessionBindingListener;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Gauge;
import com.heliosapm.easymq.cache.CacheService;
import com.heliosapm.easymq.commands.QueueAttribute;
import com.heliosapm.easymq.commands.SubscriptionAttribute;
import com.heliosapm.easymq.commands.TopicAttribute;
import com.heliosapm.easymq.http.HttpServer;
import com.heliosapm.easymq.pool.PCFMessageAgentWrapper;
import com.heliosapm.easymq.pool.PoolKey;
import com.heliosapm.easymq.pool.PoolManager;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.CMQCFC;
import com.ibm.mq.pcf.MQCFBS;
import com.ibm.mq.pcf.MQCFIL;
import com.ibm.mq.pcf.MQCFIN;
import com.ibm.mq.pcf.MQCFST;
import com.ibm.mq.pcf.PCFMessage;
import com.ibm.mq.pcf.PCFParameter;

/**
 * <p>Title: MQ</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.MQ</code></p>
 */

public class MQ implements MessageListener, HttpSessionBindingListener {
	/** The pcf pool key */
	protected final PoolKey poolKey;
	/** The pcf pool key as json */
	protected final String poolKeyJson;	
	/** The pool name */
	protected final String poolName;
	
	/** The queue manager name */
	protected final String queueManager;
	/** The MQ host name or ip address */
	protected final String host;
	/** The connection channel name */
	protected final String channel;
	/** The MQ host listening port */
	protected final int port;
	/** A reference to the pool manager */
	protected final PoolManager poolManager;
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	/** The cache service */
	protected final CacheService cache;
	
	/** A serial number for auto generated pool names */
	private static final AtomicLong autoPoolNameSerial = new AtomicLong(0L);
	
	/** The number of CPUs available to this JVM */
	private static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
	
	/** All MQ instances keyed by key */
	private static final ConcurrentHashMap<String, MQ> instances = new ConcurrentHashMap<String, MQ>(32, 0.75f, Runtime.getRuntime().availableProcessors()); 
	
	/** Thread pool for dispatching async and parallel tasks across all MQ instances */
	private static final ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
		final AtomicInteger serial = new AtomicInteger();
		@Override
		public Thread newThread(final Runnable r) {
			final Thread t = new Thread(r, "MQAsyncTask#" + serial.incrementAndGet());
			t.setDaemon(true);
			return t;
		}
	});

	/** The pattern for admin queue names */
	public static final Pattern NON_ADMIN_QUEUES = Pattern.compile("SYSTEM\\..*||AMQ\\..*", Pattern.CASE_INSENSITIVE);
	/** The pattern for admin topic names */
	public static final Pattern NON_ADMIN_TOPICS = Pattern.compile("SYSTEM\\..*", Pattern.CASE_INSENSITIVE);
	
	
	public static void main(String[] args) {
		log("MQTest");
		HttpServer.getInstance();
		final MQ mq1 = MQ.getInstance("192.168.1.13", 1414, "SYSTEM.DEF.SVRCONN");
		final MQ mq = MQ.getInstance("mq8");
		log("Same:" + (mq1==mq));
		
		final Map<String, String> queueNames = mq.getQueueNames(NON_ADMIN_QUEUES, null); 
		log("Queues:" + queueNames.keySet());
		final Map<String, String> topicNames = mq.getTopicNames(NON_ADMIN_TOPICS, null); 
		log("Topics:" + topicNames.keySet());
		for(String name: topicNames.values()) {
			final Map<String, byte[]> subInfo = mq.getTopicSubscriptions(name.trim());
			log("Subs for [" + name.trim() + "]:" + subInfo);
			if(subInfo.isEmpty()) continue;
			for(Map.Entry<String, byte[]> entry : subInfo.entrySet()) {
				final String sname = entry.getKey();
				final byte[] subId = entry.getValue();
				try {
					log("Sub Status for [" + sname.trim() + "]:" + printSubscriptionAttributes(mq.subscriptionAttrs(subId)));
				} catch (Exception x) {
					x.printStackTrace(System.err);
				}
			}			
		}
		final Map<QueueAttribute, Object> qattrs = mq.queueAttrs("PRICE.FEED.QUEUE");
		try { Thread.currentThread().join(); } catch (Exception x) {}
	}
	
	public static void log(final Object msg) {
		System.out.println(msg);
	}
	
	/**
	 * Acquires the MQ instance for the passed MQ endpoint
	 * @param host The MQ host
	 * @param port The MQ listening port
	 * @param channel The MQ command channel
	 * @return the MQ instance
	 */
	public static MQ getInstance(final String host, final int port, final String channel) {
		final String key = PoolKey.poolKeyString(host, channel, port);
		return getInstanceByKey(key);
	}
	
	/**
	 * Acquires the MQ instance for the passed key
	 * @param key The MQ pcf pool key
	 * @return the MQ instance
	 */
	public static MQ getInstanceByKey(final String key) {
		MQ mq = instances.get(key);
		if(mq==null) {
			synchronized(instances) {
				if(mq==null) {
					final PoolKey p = PoolKey.poolKey(key);
					mq = new MQ(p.host, p.port, p.channel);
					instances.put(key, mq);
				}
			}
		}
		return mq;		
	}
	
	
	/**
	 * Acquires the MQ instance for the named MQ pcf pool.
	 * If the pool name matches the key pattern, the lookup will be by pool key,
	 * otherwise it will lookup by pool name
	 * @param poolName The MQ pcf pool look up value (a key or name)
	 * @param nullIfNotFound true to return null if instance is not found, false to throw an exception
	 * @return the MQ instance
	 */
	public static MQ getInstance(final String poolName, final boolean nullIfNotFound) {
		if(poolName==null || poolName.trim().isEmpty()) throw new IllegalArgumentException("The poolName was null or empty");
		final MQ mq;
		if(PoolKey.matches(poolName.trim())) {
			mq = getInstanceByKey(poolName.trim());
		} else {
			final String key = PoolManager.getInstance().getKeyForName(poolName);
			if(key==null) {
				if(nullIfNotFound) return null;
				throw new RuntimeException("No pool named [" + poolName + "] found");
			}
			mq = getInstanceByKey(key);
		}
		if(mq==null) {
			if(nullIfNotFound) return null;
			throw new RuntimeException("Failed to locate MQ instance for value [" + poolName + "]");
		}
		return mq;		
	}
	
	/**
	 * Acquires the MQ instance for the named MQ pcf pool.
	 * If the pool name matches the key pattern, the lookup will be by pool key,
	 * otherwise it will lookup by pool name
	 * @param poolName The MQ pcf pool look up value (a key or name)
	 * @return the MQ instance
	 */
	public static MQ getInstance(final String poolName) {
		return getInstance(poolName, false);
	}
	
	
	/**
	 * Creates a new connected MQ instance 
	 * @param host The MQSeries host
	 * @param port The MQSeries listening port
	 * @param channel The MQSeries channel
	 */
	private MQ(final String host, final int port, final String channel) {
		this.host = host;
		this.channel = channel;
		this.port = port;
		poolKey = PoolKey.poolKey(host, channel, port);
		poolKeyJson = poolKey.toJson();		 
		poolManager = PoolManager.getInstance();
		final String pk = poolKey.toString();
		final String tmpPool = poolManager.getNameForKey(pk);
		poolName = tmpPool!=null ? tmpPool : "MQPCFPool#" + autoPoolNameSerial.incrementAndGet(); 
		PCFMessageAgentWrapper conn = null;
		try {
			conn = poolManager.getConnection(pk);
			queueManager = conn.getQManagerName();
		} finally {
			if(conn!=null) try { conn.close(); } catch (Exception x) {/* No Op */}
		}	
		cache = CacheService.getInstance();
		initializeCaches();
		//cache.put(poolKey.toString(), "queuenames", key, value);
	}
	
	protected void initializeCaches() {
		// Load queue attributes
		threadPool.submit(new Runnable(){			
			public void run() {
				cache.get(poolKey.toString(), "queues", fetchQueues);
			}
		});
		// Load topic attributes
		threadPool.submit(new Runnable(){			
			public void run() {
				cache.get(poolKey.toString(), "topics", fetchTopics);
			}
		});
		
		
	}
	
	/**
	 * Returns the pool key
	 * @return the pool key
	 */
	public PoolKey key() {
		return poolKey;
	}
	
	/**
	 * Returns the pool key json
	 * @return the pool key json
	 */
	public String keyJson() {
		return poolKeyJson;
	}
	
	/**
	 * Returns the pool name
	 * @return the pool name
	 */
	public String poolName() {
		return poolName;
	}
	
	
	
	/**
	 * {@inheritDoc}
	 * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
	 */
	@Override
	public void onMessage(final Message message) {
	}
	
	
	/** Callable to return all queue info */
	private final Callable<Map<?, ?>> fetchQueues = new Callable<Map<?, ?>>() {
		@Override
		public Map<String, Map<QueueAttribute, Object>> call() throws Exception {
			final long startTime = System.currentTimeMillis();
			try {
				final PCFMessage[] qAttrPcfs = pcfList(CMQCFC.MQCMD_INQUIRE_Q_STATUS, 
						new MQCFST(CMQC.MQCA_Q_NAME, "*"),
						new MQCFIN(CMQC.MQIA_Q_TYPE, CMQC.MQQT_LOCAL)
					);
				final Map<String, Map<QueueAttribute, Object>> qAttrs = new ConcurrentHashMap<String, Map<QueueAttribute, Object>>(qAttrPcfs.length, 0.75f, CORES); 
				Arrays.stream(qAttrPcfs).parallel().forEach(q -> {
					final Map<QueueAttribute, Object> attrMap = QueueAttribute.extractQueueAttributes(MQ.this,  q);
					final String queueName = (String)attrMap.get(QueueAttribute.NAME); 
					qAttrs.put(queueName, attrMap);
					cache.put(poolKey.toString(), "queues", queueName, attrMap);
				});
				
				final int size = qAttrs.size();
				final long elapsed = System.currentTimeMillis() - startTime;
				log.info("Loaded Queue Cache, Size: {}, Elapsed: {}", size, elapsed);
				return qAttrs;
			} catch (Exception ex) {
				log.error("Failed to initialize queue cache on [{}]", poolKey, ex);
				throw ex;
			}
		}
	};
	
	/** Callable to return all topic info */
	private final Callable<Map<?, ?>> fetchTopics = new Callable<Map<?, ?>>() {
		@Override
		public Map<String, Map<TopicAttribute, Object>> call() throws Exception {
			final long startTime = System.currentTimeMillis();
			final PCFMessage topicInfo = new PCFMessage(CMQCFC.MQCMD_INQUIRE_TOPIC);
			final PCFMessage topicStatus = new PCFMessage(CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS);
			final PCFMessage topicSub = new PCFMessage(CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS);
			final PCFMessage topicPub = new PCFMessage(CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS);
			try {
				topicStatus.addParameter(new MQCFST(CMQC.MQCA_TOPIC_STRING, "#"));
				topicStatus.addParameter(new MQCFIN(CMQCFC.MQIACF_TOPIC_STATUS_TYPE, CMQCFC.MQIACF_TOPIC_STATUS));
				final PCFMessage[] topicInfos = pcfList(topicStatus);
				
				final Map<String, Map<TopicAttribute, Object>> topicAttrs = new ConcurrentHashMap<String, Map<TopicAttribute, Object>>(topicInfos.length, 0.75f, CORES);
				Arrays.stream(topicInfos).parallel().forEach(tinfo -> {
					final StringBuffer b = new StringBuffer();
					final Map<TopicAttribute, Object> attrMap = 
							TopicAttribute.extractTopicAttributes(MQ.this, CMQCFC.MQIACF_TOPIC_STATUS, tinfo);
					for(Map.Entry<TopicAttribute, Object> entry: attrMap.entrySet()) {
						b.append("\n\t").append(entry.getKey()).append(":[").append(entry.getValue()).append("]");
					}
					b.append("\n\tMsg:[").append(tinfo).append("]");
					b.append("\n\t=========");
					log.info(b.toString());

				});
				
//				topicInfo.addParameter(new MQCFST(CMQC.MQCA_TOPIC_NAME, "*"));
//				final PCFMessage[] topicInfos = pcfList(topicInfo);
//				final Map<String, Map<TopicAttribute, Object>> topicAttrs = new ConcurrentHashMap<String, Map<TopicAttribute, Object>>(topicInfos.length, 0.75f, CORES);
//				Arrays.stream(topicInfos).forEach(tinfo -> {
//					final StringBuffer b = new StringBuffer();
//					final Map<TopicAttribute, Object> attrMap = TopicAttribute.extractTopicAttributes(MQ.this, CMQCFC.MQCMD_INQUIRE_TOPIC, tinfo);
//					final String topicName = (String)attrMap.get(TopicAttribute.NAME);
//					final String topicString = (String)attrMap.get(TopicAttribute.TSTRING);
//					b.append("TOPIC:  name:[").append(topicName).append("], string:[").append(topicString).append("]");
//					for(Map.Entry<TopicAttribute, Object> entry: attrMap.entrySet()) {
//						b.append("\n\t").append(entry.getKey()).append(":[").append(entry.getValue()).append("]");
//					}
//					
//					try {
//						final PCFMessage topicStatus = new PCFMessage(CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS);
//						topicStatus.addParameter(new MQCFST(CMQC.MQCA_TOPIC_STRING, topicString.isEmpty() ? padName(topicName) : topicString));
//						topicStatus.addParameter(new MQCFIN(CMQCFC.MQIACF_TOPIC_STATUS_TYPE, CMQCFC.MQIACF_TOPIC_STATUS));
//						final PCFMessage[] topicStatusResponse = pcfList(topicStatus);
//						final Map<TopicAttribute, Object> statusMap = TopicAttribute.extractTopicAttributes(
//								MQ.this, CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS, topicStatusResponse);
//						for(Map.Entry<TopicAttribute, Object> entry: statusMap.entrySet()) {
//							b.append("\n\t").append(entry.getKey()).append(":[").append(entry.getValue()).append("]");
//						}
//					} catch (Exception x) {}
//					log.info(b.toString());
//
//				});
				
				
				
//				//--
//				topicStatus.addParameter(new MQCFST(CMQC.MQCA_TOPIC_STRING, "#"));
//				topicStatus.addParameter(new MQCFIN(CMQCFC.MQIACF_TOPIC_STATUS_TYPE, CMQCFC.MQIACF_TOPIC_STATUS));
//				//--
//				topicSub.addParameter(new MQCFST(CMQC.MQCA_TOPIC_STRING, "#"));
//				topicSub.addParameter(new MQCFIN(CMQCFC.MQIACF_TOPIC_STATUS_TYPE, CMQCFC.MQIACF_TOPIC_SUB));
//				//--
//				topicPub.addParameter(new MQCFST(CMQC.MQCA_TOPIC_STRING, "#"));
//				topicPub.addParameter(new MQCFIN(CMQCFC.MQIACF_TOPIC_STATUS_TYPE, CMQCFC.MQIACF_TOPIC_PUB));
//				
//				
//				
//				final Map<Integer, PCFMessage[]> responses = new ConcurrentHashMap<Integer, PCFMessage[]>(4);
//				final PCFMessage[] requests = new PCFMessage[] {topicInfo, topicStatus, topicSub, topicPub};
//				
//				final Map<String, String> topicStrings = new HashMap<String, String>(128);
//				final CountDownLatch latch = new CountDownLatch(1);
//				IntStream.range(0, requests.length).parallel().forEach(idx -> {
//					final PCFMessage request = requests[idx];					
//					final PCFMessage[] response = pcfList(requests[idx]);
//					if(request.getCommand()==CMQCFC.MQCMD_INQUIRE_TOPIC) {
//						final Map<TopicAttribute, Object> attrMap = TopicAttribute.extractTopicAttributes(MQ.this, CMQCFC.MQCMD_INQUIRE_TOPIC, response);
//						final String topicName = (String)attrMap.get(TopicAttribute.TSTRING); 
//						topicAttrs.put(topicName, attrMap);
//						cache.put(poolKey.toString(), "topics", topicName, attrMap);
//						latch.countDown();
//					} else {
//						int statusType = idx==1 ? CMQCFC.MQIACF_TOPIC_STATUS : idx==2 ? CMQCFC.MQIACF_TOPIC_SUB : CMQCFC.MQIACF_TOPIC_PUB;
//						final Map<TopicAttribute, Object> attrMap = TopicAttribute.extractTopicAttributes(MQ.this, statusType, response);
//						try {
//							if(!latch.await(10, TimeUnit.SECONDS)) throw new RuntimeException("Timed out waiting for topic name lookup on [" + poolKey + "]");
//						} catch (InterruptedException e) {
//							throw new RuntimeException("Interrupted while waiting for topic name lookup on [" + poolKey + "]");
//						}
//						
//						final String topicName = (String)attrMap.get(TopicAttribute.TSTRING); 
//						topicAttrs.get(topicName).putAll(attrMap);
//						
//					}
//				});
//				logger.info("[{}] commands completed in [{}] ms.", responses.size(), System.currentTimeMillis() - startTime);
//				final int size = topicAttrs.size();
//				final long elapsed = System.currentTimeMillis() - startTime;
//				logger.info("Loaded Topic Cache, Size: {}, Elapsed: {}", size, elapsed);
				return topicAttrs;
			} catch (Exception ex) {
				log.error("Failed to initialize topic cache on [{}]", poolKey, ex);
				throw ex;
			}
		}
	};
	
	
	protected PCFMessage[] pcfList(final int commandType, final PCFParameter...params) {
		PCFMessageAgentWrapper conn = null;
		try {
			conn = poolManager.getConnection(poolKey.toString());
			final PCFMessage request = new PCFMessage(commandType);
			for(PCFParameter p: params) {
				request.addParameter(p);
			}
			return conn.send(request);
		} catch (Exception ex) {
			throw new RuntimeException("PCF Exception", ex);
		} finally {
			if(conn!=null) try { conn.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	protected PCFMessage[] pcfList(final PCFMessage request) {
		PCFMessageAgentWrapper conn = null;
		try {
			conn = poolManager.getConnection(poolKey.toString());
			return conn.send(request);
		} catch (Exception ex) {
			throw new RuntimeException("PCF Exception", ex);
		} finally {
			if(conn!=null) try { conn.close(); } catch (Exception x) {/* No Op */}
		}
		
	}
	
	/**
	 * Returns the queue depth for the named queue
	 * @param queueName The queue name
	 * @return The queue depth
	 */
	public int queueDepth(final String queueName) {
		final PCFMessage p = pcfList(CMQCFC.MQCMD_INQUIRE_Q_STATUS, 
				new MQCFST(CMQC.MQCA_Q_NAME, queueName)
			)[0];
		try {
			return p.getIntParameterValue(CMQC.MQIA_CURRENT_Q_DEPTH);
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			throw new RuntimeException("Failed to get queue depth for [" + queueName + "]", ex);
		}
	}
	
	public static final String DATE_FORMAT = "yyyy-MM-dd HH.mm.ss";
	public static final int DATE_LENGTH = DATE_FORMAT.length();
	static final ThreadLocal<WeakReference<SimpleDateFormat>> SDF = new ThreadLocal<WeakReference<SimpleDateFormat>>() {
		@Override
		protected WeakReference<SimpleDateFormat> initialValue() {
			return new WeakReference<SimpleDateFormat>(new SimpleDateFormat(DATE_FORMAT));
		}
	};
	
	/**
	 * Converts a pcf standard date format to a java date
	 * @param cs The string date
	 * @return The java date
	 */
	public static Date fromStringy(final CharSequence stringy) {
		final String s = stringy.toString().trim();
		if(s.isEmpty()) return null;
		WeakReference<SimpleDateFormat> sdfRef = SDF.get();		
		SimpleDateFormat sdf = sdfRef.get();
		if(sdf==null) {
			SDF.remove();
			sdf = SDF.get().get();
		}
		try {
			return sdf.parse(s.toString().trim());
		} catch (ParseException pe) {
			throw new RuntimeException("Failed to parse date [" + stringy + "]");
		}
	}
	
	
	public Map<String, byte[]> getTopicSubscriptions(final String topicName) {
		try {
			final Map<TopicAttribute, Object> topicAttrs;
			try {
				topicAttrs = topicAttrs(topicName.trim());
			} catch (Exception ex) {
				return Collections.emptyMap();
			}
			if(((Integer)topicAttrs.get(TopicAttribute.SUBSCRIBER_COUNT)).intValue()==0) {
				return Collections.emptyMap();
			}
			@SuppressWarnings("unchecked")
			final Map<String, byte[]> subIds = (Map<String, byte[]>)topicAttrs.get(TopicAttribute.SUB_SUBSCRIPTION_ID_BYTES); 
			
			final Map<String, byte[]> map = new HashMap<String, byte[]>(subIds.size());			
			for(byte[] id: subIds.values()) {
				final PCFMessage[] p = pcfList(CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION,
					new MQCFBS(CMQCFC.MQBACF_SUB_ID, id));
					final String subName = p[0].getStringParameterValue(CMQCFC.MQCACF_SUB_NAME);
					if(subName==null || subName.trim().isEmpty()) {
						map.put(DatatypeConverter.printBase64Binary(id), id);
					} else {
						map.put(subName, id);
					}
					
			}
			return map;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to get Subscriptions for Topic [" + topicName + "]", ex);
		}			
	}
	
	
	/**
	 * Returns the queue attributes for the named queue
	 * @param queueName The queue name
	 * @return The queue attributes in a name/value map
	 */
	public Map<QueueAttribute, Object> queueAttrs(final String queueName) {
		final PCFMessage[] p = pcfList(CMQCFC.MQCMD_INQUIRE_Q_STATUS, 
				new MQCFST(CMQC.MQCA_Q_NAME, padName(queueName))
			);
		try {
			return QueueAttribute.extractQueueAttributes(this, p);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to get queue attributes for [" + queueName.trim() + "]", ex);
		}		
	}
	
	/**
	 * Returns the topic attributes for the named topic
	 * @param topicName The topic name
	 * @return The topic attributes in a name/value map
	 */
	public Map<TopicAttribute, Object> topicAttrs(final String topicName) {
		final PCFMessage[] p = pcfList(CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS, 
				new MQCFST(CMQC.MQCA_TOPIC_STRING, topicName), 
				new MQCFIN(CMQCFC.MQIACF_TOPIC_STATUS_TYPE, CMQCFC.MQIACF_TOPIC_STATUS)
			);
		try {
			final Map<TopicAttribute, Object> attrMap = TopicAttribute.extractTopicAttributes(this, CMQCFC.MQIACF_TOPIC_STATUS, p);
			final Integer subs = (Integer)attrMap.get(TopicAttribute.SUBSCRIBER_COUNT);
			final Integer pubs = (Integer)attrMap.get(TopicAttribute.PUBLISHER_COUNT);
			if(pubs > 0) {
				attrMap.putAll(
						TopicAttribute.extractTopicAttributes(
							this,
							CMQCFC.MQIACF_TOPIC_PUB,
							pcfList(CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS, 
								new MQCFST(CMQC.MQCA_TOPIC_STRING, topicName), 
								new MQCFIN(CMQCFC.MQIACF_TOPIC_STATUS_TYPE, CMQCFC.MQIACF_TOPIC_PUB)								
						)
				));
			}
			if(subs > 0) {
				attrMap.putAll(
						TopicAttribute.extractTopicAttributes(
							this, 
							CMQCFC.MQIACF_TOPIC_SUB,
							pcfList(CMQCFC.MQCMD_INQUIRE_TOPIC_STATUS, 
								new MQCFST(CMQC.MQCA_TOPIC_STRING, topicName), 
								new MQCFIN(CMQCFC.MQIACF_TOPIC_STATUS_TYPE, CMQCFC.MQIACF_TOPIC_SUB)								
						)
				));
			}
			return attrMap;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to get topic attributes for [" + topicName.trim() + "]", ex);
		}		
	}
	
	/**
	 * Returns the subscription attributes for the named subscription
	 * @param subName The subscription name
	 * @return The subscription attributes in a name/value map
	 */
	public Map<SubscriptionAttribute, Object> subscriptionAttrs(final String subName) {
		try {
			PCFMessage[] p = pcfList(CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION, 
					new MQCFST(CMQCFC.MQCACF_SUB_NAME, subName) 
			);
			final Map<SubscriptionAttribute, Object> attrMap = SubscriptionAttribute.extractSubscriptionAttributes(this, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION, p);
			attrMap.putAll(
					SubscriptionAttribute.extractSubscriptionAttributes(
						this, 
						CMQCFC.MQCMD_INQUIRE_SUB_STATUS, 
						pcfList(CMQCFC.MQCMD_INQUIRE_SUB_STATUS, new MQCFST(CMQCFC.MQCACF_SUB_NAME, subName)
					)
			));
			return attrMap;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to get subscription attributes for [" + subName.trim() + "]", ex);
		}		
	}
	
	/**
	 * Returns the subscription attributes for the named subscription
	 * @param subName The subscription name
	 * @return The subscription attributes in a name/value map
	 */
	public Map<SubscriptionAttribute, Object> subscriptionAttrs(final byte[] subId) {
		try {
			PCFMessage[] p = pcfList(CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION, 
					new MQCFBS(CMQCFC.MQBACF_SUB_ID, subId) 
			);
			final Map<SubscriptionAttribute, Object> attrMap = SubscriptionAttribute.extractSubscriptionAttributes(this, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION, p);
			final String subName = (String)attrMap.get(SubscriptionAttribute.NAME);
			attrMap.putAll(
					SubscriptionAttribute.extractSubscriptionAttributes(
						this, 
						CMQCFC.MQCMD_INQUIRE_SUB_STATUS, 
						pcfList(CMQCFC.MQCMD_INQUIRE_SUB_STATUS, new MQCFST(CMQCFC.MQCACF_SUB_NAME, subName)
					)
			));
			return attrMap;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to get subscription attributes for [" + DatatypeConverter.printHexBinary(subId) + "]", ex);
		}		
	}
	

	
	/**
	 * Stringifies the passed queue attributes in one line 
	 * @param qattrs The attributes
	 * @return the string
	 */
	public static final String printQueueAttributes(final Map<QueueAttribute, Object> qattrs) {
		final StringBuilder b = new StringBuilder();
		for(QueueAttribute qa : QueueAttribute.VALUE_SET) {
			if(b.length()>0) {
				b.append(", ");
			}
			b.append(qa.name()).append(":");
			final Object o = qattrs.get(qa);
			if(o==null) {
				b.append("null");
			} else {
				if(qa.type.isArray()) {
					b.append(Arrays.toString((int[])o));
				} else {
					b.append(o);
				}
			}
		}
		return b.toString();
	}
	
	/**
	 * Stringifies the passed subscription attributes in one line 
	 * @param qattrs The attributes
	 * @return the string
	 */
	public static final String printSubscriptionAttributes(final Map<SubscriptionAttribute, Object> qattrs) {
		final StringBuilder b = new StringBuilder();
		for(SubscriptionAttribute qa : SubscriptionAttribute.VALUE_SET) {
			final Object o = qattrs.get(qa);
			if(o==null) continue;
			if(b.length()>0) {
				b.append(", ");
			}
			b.append(qa.name()).append(":");
			b.append(o.toString().trim());
		}
		return b.toString();
	}
	
	
	/**
	 * Stringifies the passed topic attributes in as few lines as possible 
	 * @param tattrs The attributes
	 * @return the string
	 */
	public static final String printTopicAttributes(final Map<TopicAttribute, Object> tattrs) {
		final StringBuilder b = new StringBuilder();
		for(TopicAttribute ta : TopicAttribute.VALUE_SET) {
			if(!tattrs.containsKey(ta)) continue;
			if(b.length()>0) {
				b.append(", ");
			}
			if(ta.type==Map.class) {
				b.append("\n\t").append(ta.name()).append(":");
			} else {
				b.append(ta.name()).append(":");
			}
			
			final Object o = tattrs.get(ta);
			if(o==null) {
				b.append("null");
			} else {
				if(ta.type.isArray()) {
					b.append(Arrays.toString((Object[])o));
				} else if(ta.type==Map.class) {
					@SuppressWarnings("unchecked")
					Map<Object, Object> map = (Map<Object, Object>)o; 
					for(Map.Entry<Object, Object> entry: map.entrySet()) {
						b.append("\n\t\t").append(entry.getKey()).append(":").append(entry.getValue());
					}
					b.append("\n");
				} else {
					b.append(o);
				}
			}
		}
		return b.toString();
	}
	
	
	
//	public void browseQueue(final String queueName, final MessageHandler handler) {
//		Connection conn = null;
//		Session session = null;
//		QueueBrowser browser = null;
//		try {
//			conn = connectionFactory.createConnection();
//			session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//			browser = session.createBrowser(session.createQueue(queueName));
//			for(@SuppressWarnings("unchecked")
//			Enumeration<Message> menum = browser.getEnumeration(); menum.hasMoreElements();) {
//				final Message m = menum.nextElement();
//				if(!handler.onMessage(m)) break;
//			}
//		} catch (Exception ex) {
//			throw new RuntimeException("Failed to browse queue [" + (queueName==null ? "null" : queueName.trim()) + "]", ex);
//		} finally {
//			if(browser!=null) try { browser.close(); } catch (Exception x) {/* No Op */}
//			if(session!=null) try { session.close(); } catch (Exception x) {/* No Op */}
//			if(conn!=null) try { conn.close(); } catch (Exception x) {/* No Op */}
//		}
//	}
	
	public Map<String, String> getQueueNames() {
		return getQueueNames(null, null);
	}
	
	public Map<String, String> getNonAdminQueueNames() {
		return getQueueNames(null, null);
	}

	public Map<String, String> getTopicNames() {
		return getTopicNames(null, null);
	}
	
	public Gauge<Integer> queueDepthGauge(final String queueName, final long cachePeriodSecs) {
		return new CachedGauge<Integer>(cachePeriodSecs, TimeUnit.SECONDS) {
			@Override
			protected Integer loadValue() {
				return queueDepth(queueName);
			}
		};
	}
	
	public Gauge<Integer> subDepthGauge(final String subName, final long cachePeriodSecs) {
		final String queueName = (String)subscriptionAttrs(subName).get(SubscriptionAttribute.DESTINATION);
		return new CachedGauge<Integer>(cachePeriodSecs, TimeUnit.SECONDS) {
			@Override
			protected Integer loadValue() {
				return queueDepth(queueName);
			}
		};
	}
	
	
	
	public Map<String, String> getQueueNames(final Pattern excludeFilter, final Pattern includeFilter) {		
		PCFMessage p = pcfList(CMQCFC.MQCMD_INQUIRE_Q_NAMES, 
				new MQCFST(CMQC.MQCA_Q_NAME, "*"),
				new MQCFIN(CMQC.MQIA_Q_TYPE, CMQC.MQQT_LOCAL)
			)[0];		
		try {
			final String[] queueNames = p.getStringListParameterValue(CMQCFC.MQCACF_Q_NAMES);
			final Map<String, String> map = new HashMap<String, String>(queueNames.length);
			for(String q: queueNames) {
				final String tq = q.trim();
				if(excludeFilter!=null) {
					if(excludeFilter.matcher(tq).matches()) continue;
				}
				if(includeFilter!=null) {
					if(!includeFilter.matcher(tq).matches()) continue;
				}				
				map.put(tq, q);
			}
			return map;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to extract queue names", ex);
		}
	}
	
	/**
	 * Determines if this MQ instance has the named topic
	 * @param topicName The topic name to test for
	 * @return true if there are one or more topics matching the passed name, false otherwise
	 */
	public boolean topicExists(final String topicName) {
		PCFMessage[] p = pcfList(CMQCFC.MQCMD_INQUIRE_TOPIC, 
				new MQCFST(CMQC.MQCA_TOPIC_NAME, topicName),
				new MQCFIL(CMQCFC.MQIACF_TOPIC_ATTRS, new int[]{CMQC.MQCA_TOPIC_STRING})
			);
		return p.length != 0;		
	}
	
	public Map<String, String> getTopicNames(final Pattern excludeFilter, final Pattern includeFilter) {
		PCFMessage[] p = pcfList(CMQCFC.MQCMD_INQUIRE_TOPIC, 
				new MQCFST(CMQC.MQCA_TOPIC_NAME, "*"),
				new MQCFIL(CMQCFC.MQIACF_TOPIC_ATTRS, new int[]{CMQC.MQCA_TOPIC_STRING})
//				new MQCFIL(CMQC.MQIA_TOPIC_TYPE, new int[]{CMQC.MQTOPT_LOCAL})
			);
		try {
			final Map<String, String> map = new HashMap<String, String>(p.length);
			for(PCFMessage t: p) {
				final String topicName = t.getStringParameterValue(CMQC.MQCA_TOPIC_NAME).trim();
				final String topicString = t.getStringParameterValue(CMQC.MQCA_TOPIC_STRING);
				if(topicString==null || topicString.trim().isEmpty()) continue;
				if(excludeFilter!=null) {
					if(excludeFilter.matcher(topicName).matches()) continue;
				}				
				if(includeFilter!=null) {
					if(!includeFilter.matcher(topicName).matches()) continue;
				}								
				map.put(topicName, t.getStringParameterValue(CMQC.MQCA_TOPIC_STRING));
			}
			return map;
		} catch (Exception ex) {
			throw new RuntimeException("Failed to extract topic names", ex);
		}
	}
	
	/**
	 * Pads the passed name out to the max size of a queue name,
	 * unless the name is wildcarded, in which case the trimmed value is returned.
	 * @param queueName The name to pad
	 * @return the padded queue name 
	 */
	public static String padName(final String queueName) {
		if(queueName==null || queueName.trim().isEmpty()) throw new IllegalArgumentException("The passed queue name was null or empty");		
		final StringBuilder q = new StringBuilder(queueName.trim());
		
		if(q.charAt(q.length()-1)=='*') return q.toString();
		final int toPad = CMQC.MQ_Q_NAME_LENGTH - queueName.length();
		final char[] padding = new char[toPad];
		Arrays.fill(padding, ' ');
		return q.append(padding).toString();
	}
	
	/**
	 * Sends a JMS text message
	 * @param body An object which is rendered to a string using {@link #toString()}
	 * @param headers A map of headers applied to the message
	 * @return the sent message
	 */
//	public synchronized Message sendMessage(final Object body, final Map<String, String> headers) {
//		try {
//			final TextMessage msg = session.createTextMessage(body.toString());
//			
//			if(headers!=null && !headers.isEmpty()) {
//				for(Map.Entry<String, String> entry: headers.entrySet()) {
//					msg.setObjectProperty(entry.getKey().trim(), entry.getValue().trim());
//				}
//			}
//			producer.send(msg);
//			session.commit();
//			return msg;
//		} catch (Exception ex) {
//			Logger.error(ex, "Failed to send message");
//			return null;
//		}
//	}
	
	
	protected static String config(final Properties p, final String key, final String defaultValue, final String msg) {
		final String value = p.getProperty(key, defaultValue);
		if(value==null) throw new IllegalArgumentException("Invalid value for config item [" + msg + "] : [" + value + "]");
		p.setProperty(key, value.trim());
		return value.trim();
	}
	
	protected static int config(final Properties p, final String key, final int defaultValue, final String msg) {
		final String value = p.getProperty(key, "" + defaultValue);
		if(value==null) throw new IllegalArgumentException("Invalid value for config item [" + msg + "] : [" + value + "]");
		int v = -1;
		try { v = Integer.parseInt(value.trim()); } catch (Exception x) { v = -1; }
		if(v==-1) throw new IllegalArgumentException("Invalid value for config item [" + msg + "] : [" + value + "]");
		p.setProperty(key, "" + v);
		return v;
	}
	
	protected static boolean config(final Properties p, final String key, final boolean defaultValue, final String msg) {
		final String value = p.getProperty(key, "" + defaultValue);
		if(value==null) throw new IllegalArgumentException("Invalid value for config item [" + msg + "] : [" + value + "]");
		boolean b = value.trim().equalsIgnoreCase("true");
		p.setProperty(key, "" + b);
		return b;
	}

	/**
	 * 
	 * @see javax.servlet.http.HttpSessionBindingListener#valueBound(javax.servlet.http.HttpSessionBindingEvent)
	 */
	@Override
	public void valueBound(final HttpSessionBindingEvent event) {
		/* No Op */
	}

	/**
	 * 
	 * @see javax.servlet.http.HttpSessionBindingListener#valueUnbound(javax.servlet.http.HttpSessionBindingEvent)
	 */
	@Override
	public void valueUnbound(final HttpSessionBindingEvent event) {
		try { log("MQ Closed on session unbind"); } catch (Exception x) {/* No Op */}
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return poolKey + "[" + queueManager + "]";
	}

}
