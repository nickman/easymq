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

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.servlet.http.HttpSessionBindingEvent;
import javax.servlet.http.HttpSessionBindingListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Gauge;
import com.heliosapm.easymq.commands.QueueAttribute;
import com.heliosapm.easymq.commands.SubscriptionAttribute;
import com.heliosapm.easymq.commands.TopicAttribute;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.CMQCFC;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.pcf.MQCFBS;
import com.ibm.mq.pcf.MQCFIL;
import com.ibm.mq.pcf.MQCFIN;
import com.ibm.mq.pcf.MQCFST;
import com.ibm.mq.pcf.PCFMessage;
import com.ibm.mq.pcf.PCFMessageAgent;
import com.ibm.mq.pcf.PCFParameter;

/**
 * <p>Title: MQ</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.MQ</code></p>
 */

public class MQ implements Closeable, MessageListener, HttpSessionBindingListener {
	/** The MQ PCF agent */
	protected final PCFMessageAgent pcf;
	/** The MQ QueueManager */
	protected final MQQueueManager qmanager;
	
	
	/** The listener MQ session */
	protected final Session listenerSession;
	/** The MQ message consumer */
	protected final MessageConsumer consumer;
	
	/** The queue manager name */
	protected final String queueManager;
	/** The MQ host name or ip address */
	protected final String host;
	/** The connection channel name */
	protected final String channel;
	/** The MQ host listening port */
	protected final int port;
	/** Indicates if we should lisen on the outbound port */
	protected final boolean listen;
	
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	
	
	/** The default MQ Queue Manager name */
	public static final String DEFAULT_QUEUE_MAN_NAME = "MQMGR";
	/** The config key for the connection channel name */
	public static final String CONFIG_CHANNEL_NAME = "channel";
	/** The default connection channel name */
	public static final String DEFAULT_CHANNEL_NAME = "SVRCONN";	
	/** The config key for the MQ server host name */
	public static final String CONFIG_MQ_HOST = "server.host";
	/** The config key for the MQ server port */
	public static final String CONFIG_MQ_PORT = "server.port";
	/** The default MQ server port */
	public static final int DEFAULT_MQ_PORT = 1430;
	
	/** The config key for the connection channel name */
	public static final String CONFIG_LISTEN = "listen";
	/** The default connection channel name */
	public static final boolean DEFAULT_LISTEN = false;	

	/** The pattern for admin queue names */
	public static final Pattern NON_ADMIN_QUEUES = Pattern.compile("SYSTEM\\..*||AMQ\\..*", Pattern.CASE_INSENSITIVE);
	/** The pattern for admin topic names */
	public static final Pattern NON_ADMIN_TOPICS = Pattern.compile("SYSTEM\\..*", Pattern.CASE_INSENSITIVE);
	
	
	public static void main(String[] args) {
		log("MQTest");
		System.setProperty("mq.config", "mq.properties");
//		final MQ mq = new MQ(new File("./src/test/resources/working"));
//		final Map<String, String> queueNames = mq.getQueueNames(NON_ADMIN_QUEUES, null); 
//		log("Queues:" + queueNames.keySet());
//		final Map<String, String> topicNames = mq.getTopicNames(NON_ADMIN_TOPICS, null); 
//		log("Topics:" + topicNames.keySet());
//		for(String name: topicNames.values()) {
//			final Map<String, byte[]> subInfo = mq.getTopicSubscriptions(name.trim());
//			log("Subs for [" + name.trim() + "]:" + subInfo);
//			if(subInfo.isEmpty()) continue;
//			for(String sname : subInfo.keySet()) {
//				try {
//					log("Sub Status for [" + sname.trim() + "]:" + printSubscriptionAttributes(mq.subscriptionAttrs(sname)));
//				} catch (Exception x) {
//					x.printStackTrace(System.err);
//				}
//			}
//		}
	}
	
	public static void log(final Object msg) {
		System.out.println(msg);
	}
	
	/**
	 * Creates a new connected MQ instance for admin only
	 * @param host The MQSeries host
	 * @param port The MQSeries listening port
	 * @param channel The MQSeries channel
	 * @param qmName The MQSeries queue manager name
	 */
	public MQ(final String host, final int port, final String channel, final String qmName) {
		listenerSession = null;
		listen = false;
		consumer = null;
		this.queueManager = qmName;
		this.host = host;			
		this.channel = channel;
		this.port = port;		
//		try {
//			connectionFactory.setAppName(getClass().getName());
//			connectionFactory.setDescription("MQ Admin API");
//			connectionFactory.setHostName(host);
//			connectionFactory.setPort(port);
//			connectionFactory.setQueueManager(queueManager);
//			connectionFactory.setChannel(channel);
//			connectionFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);			
//		} catch (Exception ex) {
//			throw new RuntimeException("Failed to initialize MQ Connection Factory", ex);
//		}
		try {
			Hashtable<String, Object> env = new Hashtable<String, Object>();
			env.put(MQConstants.HOST_NAME_PROPERTY, host);
			env.put(MQConstants.PORT_PROPERTY, port);
			env.put(MQConstants.CHANNEL_PROPERTY, channel);
			qmanager = new MQQueueManager(queueManager, env);
			pcf = new PCFMessageAgent();
			pcf.connect(qmanager);
			logger.info("PCFMessageAgent connected to {}@{}:{}", queueManager, host, port);
		} catch (Exception ex) {
			throw new RuntimeException("Failed to initialize MQ PCF Message Agent", ex);
		}
//		try {
//			connection = connectionFactory.createConnection();			
//			session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
//			producer = session.createProducer(inQueue);
//			connection.start();
//			Logger.info("MQ Producer Initialized");
//		} catch (Exception ex) {
//			try { close(); } catch (Exception x) {/* No Op */}
//			throw new RuntimeException("Failed to initialize MQ message producer", ex);
//		}
	}
	
	
	
	
	
	/**
	 * {@inheritDoc}
	 * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
	 */
	@Override
	public void onMessage(final Message message) {
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
//		try { producer.close(); } catch (Exception x) {/* No Op */}
//		try { session.close(); } catch (Exception x) {/* No Op */}
//		try { connection.stop(); } catch (Exception x) {/* No Op */}
//		try { connection.close(); } catch (Exception x) {/* No Op */}
		try { pcf.disconnect(); }  catch (Exception x) {/* No Op */}		
		try { qmanager.disconnect(); }  catch (Exception x) {/* No Op */}
		logger.info("MQ Instance Closed");
	}
	
	
	protected synchronized PCFMessage[] pcfList(final int commandType, final PCFParameter...params) {
		try {
			final PCFMessage request = new PCFMessage(commandType);
			for(PCFParameter p: params) {
				request.addParameter(p);
			}
			return pcf.send(request);
		} catch (Exception ex) {
			throw new RuntimeException("PCF Exception", ex);
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
	
	public static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
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
	public static Date fromStringy(final CharSequence cs) {
		WeakReference<SimpleDateFormat> sdfRef = SDF.get();		
		SimpleDateFormat sdf = sdfRef.get();
		if(sdf==null) {
			SDF.remove();
			sdf = SDF.get().get();
		}
		try {
			return sdf.parse(cs.toString().trim());
		} catch (ParseException pe) {
			throw new RuntimeException("Failed to parse date [" + cs + "]");
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
					map.put(p[0].getStringParameterValue(CMQCFC.MQCACF_SUB_NAME), id);
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
				new MQCFST(CMQC.MQCA_Q_NAME, queueName)
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
		try { close(); log("MQ Closed on session unbind"); } catch (Exception x) {/* No Op */}
	}
	

}
