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
package com.heliosapm.easymq.commands;

import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

import com.heliosapm.easymq.MQ;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.CMQCFC;
import com.ibm.mq.pcf.PCFException;
import com.ibm.mq.pcf.PCFMessage;

/**
 * <p>Title: TopicAttribute</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.commands.TopicAttribute</code></p>
 */

public enum TopicAttribute implements AttributeExtractor {
	PUBLISHER_COUNT(Integer.class, CMQCFC.MQIACF_TOPIC_STATUS){
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			return messages[0].getIntParameterValue(CMQC.MQIA_PUB_COUNT);
		}
	},
	SUBSCRIBER_COUNT(Integer.class, CMQCFC.MQIACF_TOPIC_STATUS){
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			return messages[0].getIntParameterValue(CMQC.MQIA_SUB_COUNT);
		}
	},
	LAST_PUB_DATES(Map.class, CMQCFC.MQIACF_TOPIC_PUB) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final Map<String, Date> map = new HashMap<String, Date>(messages.length);
			for(PCFMessage message: messages) {					
				final String connId = message.getStringParameterValue(CMQCFC.MQBACF_CONNECTION_ID);
				final StringBuilder b = new StringBuilder(MQ.DATE_LENGTH);
				b.append(message.getStringParameterValue(CMQCFC.MQCACF_LAST_PUB_DATE))
					.append(" ")
					.append(message.getStringParameterValue(CMQCFC.MQCACF_LAST_PUB_TIME));
				map.put(connId, b.length() >= MQ.DATE_LENGTH ? MQ.fromStringy(b) : null);
			}
			return map;
		}			
	},
	PUB_MSG_COUNTS(Map.class, CMQCFC.MQIACF_TOPIC_PUB) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final Map<String, Integer> map = new HashMap<String, Integer>(messages.length);
			for(PCFMessage message: messages) {					
				final String connId = message.getStringParameterValue(CMQCFC.MQBACF_CONNECTION_ID);
				map.put(connId, message.getIntParameterValue(CMQCFC.MQIACF_PUBLISH_COUNT));
			}
			return map;				
		}
	},
	PUB_CONNECTION_ID(String[].class, CMQCFC.MQIACF_TOPIC_PUB) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final int len = messages.length;
			final String[] conns = new String[len];
			for(int i = 0; i < len; i++) {
				conns[i] = messages[i].getStringParameterValue(CMQCFC.MQBACF_CONNECTION_ID);
			}
			return conns;
		}
	},
	SUB_COMM_INFO(String[].class, CMQCFC.MQIACF_TOPIC_STATUS) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final int len = messages.length;
			final String[] conns = new String[len];
			for(int i = 0; i < len; i++) {
				conns[i] = messages[i].getStringParameterValue(CMQC.MQCA_COMM_INFO_NAME);
			}
			return conns;				
		}
	},
	SUB_RESUME_DATE(Map.class, CMQCFC.MQIACF_TOPIC_SUB) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final Map<String, Date> map = new HashMap<String, Date>(messages.length);
			for(PCFMessage message: messages) {					
				final String subId = DatatypeConverter.printHexBinary(message.getBytesParameterValue(CMQCFC.MQBACF_SUB_ID)); 
				final StringBuilder b = new StringBuilder(MQ.DATE_LENGTH);
				b.append(message.getStringParameterValue(CMQC.MQCA_RESUME_DATE))
					.append(" ")
					.append(message.getStringParameterValue(CMQC.MQCA_RESUME_TIME));
				map.put(subId, b.length() >= MQ.DATE_LENGTH ? MQ.fromStringy(b) : null);
			}
			return map;
		}			
	},
	SUB_LAST_MESSAGE_DATE(Map.class, CMQCFC.MQIACF_TOPIC_SUB) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final Map<String, Date> map = new HashMap<String, Date>(messages.length);
			for(PCFMessage message: messages) {					
				final String subId = DatatypeConverter.printHexBinary(message.getBytesParameterValue(CMQCFC.MQBACF_SUB_ID));
				final StringBuilder b = new StringBuilder(MQ.DATE_LENGTH);
				b.append(message.getStringParameterValue(CMQCFC.MQCACF_LAST_MSG_DATE))
					.append(" ")
					.append(message.getStringParameterValue(CMQCFC.MQCACF_LAST_MSG_TIME));
				final String dt = b.toString().trim();
				if(!dt.isEmpty()) {
					map.put(subId, MQ.fromStringy(b));
				}				
			}
			return map;
		}			
	},
	SUB_MSG_COUNTS(Map.class, CMQCFC.MQIACF_TOPIC_SUB) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final Map<String, Integer> map = new HashMap<String, Integer>(messages.length);
			for(PCFMessage message: messages) {					
				final String subId = DatatypeConverter.printHexBinary(message.getBytesParameterValue(CMQCFC.MQBACF_SUB_ID));
				map.put(subId, message.getIntParameterValue(CMQCFC.MQIACF_MESSAGE_COUNT));
			}
			return map;				
		}
	},
	SUB_SUBSCRIPTION_ID(String[].class, CMQCFC.MQIACF_TOPIC_SUB) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final int len = messages.length;
			final String[] conns = new String[len];
			for(int i = 0; i < len; i++) {
				conns[i] = DatatypeConverter.printHexBinary(messages[i].getBytesParameterValue(CMQCFC.MQBACF_SUB_ID));
			}
			return conns;
		}
	},
	SUB_SUBSCRIPTION_ID_BYTES(Map.class, CMQCFC.MQIACF_TOPIC_SUB) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final Map<String, byte[]> map = new HashMap<String, byte[]>(messages.length);
			for(PCFMessage message: messages) {					
				final byte[] idBytes = message.getBytesParameterValue(CMQCFC.MQBACF_SUB_ID);
				final String subId = DatatypeConverter.printHexBinary(idBytes);
				map.put(subId, idBytes);
			}
			return map;				
		}
	};
	
	private static final TopicAttribute[] values = values();
	public static final Set<TopicAttribute> VALUE_SET = Collections.unmodifiableSet(EnumSet.allOf(TopicAttribute.class));
	
	
	public static Map<TopicAttribute, Object> extractTopicAttributes(final MQ mq, final int statusType, final PCFMessage...messages) {
		final EnumMap<TopicAttribute, Object> map = new EnumMap<TopicAttribute, Object>(TopicAttribute.class);
		for(final TopicAttribute ta : values) {
			if(ta.statusType != statusType) continue;
			try {
				map.put(ta, ta.extract(mq, messages));
			} catch (PCFException pex) {
				pex.printStackTrace(System.err);
			}
		}
		return map;
	}
	
	private TopicAttribute(final Class<?> type, final int statusType) {
		this.type = type;
		this.statusType = statusType;
	}
	
	public final Class<?> type;
	public final int statusType;

}
