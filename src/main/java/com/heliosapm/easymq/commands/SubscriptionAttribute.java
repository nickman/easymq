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

import java.text.AttributedCharacterIterator;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import com.heliosapm.easymq.MQ;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.CMQCFC;
import com.ibm.mq.pcf.PCFException;
import com.ibm.mq.pcf.PCFMessage;

/**
 * <p>Title: SubscriptionAttribute</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.commands.SubscriptionAttribute</code></p>
 */

public enum SubscriptionAttribute implements AttributeExtractor {
	DESTINATION(String.class, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION){
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			try {
				return messages[0].getStringParameterValue(CMQCFC.MQCACF_DESTINATION);
			} catch (Exception ex) {
				return null;
			}
		}
	},
	TOPIC(String.class, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION){
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			return messages[0].getStringParameterValue(CMQC.MQCA_TOPIC_STRING);
		}
	},
	USER_DATA(String.class, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION){
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			return messages[0].getStringParameterValue(CMQCFC.MQCACF_SUB_USER_DATA);
		}
	},		
	
	MANAGED(Boolean.class, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION){ // true means a managed destination, false means a provided destination
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final int man = messages[0].getIntParameterValue(CMQCFC.MQIACF_DESTINATION_CLASS);
			return man==CMQC.MQDC_MANAGED;
		}
	},
	SCOPE_ALL(Boolean.class, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION){ // true means the subscription is forwarded to all queue managers directly connected through a publish/subscribe collective or hierarchy. 
		// false means the subscription only forwards messages published on the topic within this queue manager.
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final int man = messages[0].getIntParameterValue(CMQCFC.MQIACF_SUBSCRIPTION_SCOPE);
			return man==CMQC.MQTSCOPE_ALL;
		}
	},  		
	
	DURABLE(Boolean.class, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION){
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final int dur = messages[0].getIntParameterValue(CMQCFC.MQIACF_DURABLE_SUBSCRIPTION);
			return dur==CMQC.MQSUB_DURABLE_YES;
		}
	},  
	LAST_MESSAGE_SENT(Date.class, CMQCFC.MQCMD_INQUIRE_SUB_STATUS) { // The date that a message was last sent to the destination specified by the subscription
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final StringBuilder b = new StringBuilder(MQ.DATE_LENGTH);
			b.append(messages[0].getStringParameterValue(CMQCFC.MQCACF_LAST_MSG_DATE))
				.append(" ")
				.append(messages[0].getStringParameterValue(CMQCFC.MQCACF_LAST_MSG_TIME));
			return b.length() >= MQ.DATE_LENGTH ? MQ.fromStringy(b) : null;
		}			
	},
	LAST_RESUME(Date.class, CMQCFC.MQCMD_INQUIRE_SUB_STATUS) { // The date of the most recent MQSUB API call that connected to the subscription 
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final StringBuilder b = new StringBuilder(MQ.DATE_LENGTH);
			b.append(messages[0].getStringParameterValue(CMQC.MQCA_RESUME_DATE))
				.append(" ")
				.append(messages[0].getStringParameterValue(CMQC.MQCA_RESUME_TIME));
			return b.length() >= MQ.DATE_LENGTH ? MQ.fromStringy(b) : null;
		}			
	},		
	MESSAGES_SENT(Integer.class, CMQCFC.MQCMD_INQUIRE_SUB_STATUS) { // The number of messages put to the destination specified by this subscription
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			return messages[0].getIntParameterValue(CMQCFC.MQIACF_MESSAGE_COUNT);
		}			
	},
	QUEUE_MGR(String.class, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION) { // The queue manager hosting the subscription's queue
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final String qmgr = messages[0].getStringParameterValue(CMQCFC.MQCACF_DESTINATION_Q_MGR);
			return qmgr;
		}			
	},		
	UNDELIVERED_MESSAGES(Integer.class, CMQCFC.MQCMD_INQUIRE_SUBSCRIPTION) { // The number of messages still in the subscription's queue
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			try {
				final String queueName = messages[0].getStringParameterValue(CMQCFC.MQCACF_DESTINATION);
				return mq.queueDepth(queueName);
			} catch (Exception ex) {
				return -1;
			}
		}			
	};		
	
	
	
	
	private static final SubscriptionAttribute[] values = values();
	public static final Set<SubscriptionAttribute> VALUE_SET = Collections.unmodifiableSet(EnumSet.allOf(SubscriptionAttribute.class));
	
	
	public static Map<SubscriptionAttribute, Object> extractSubscriptionAttributes(final MQ mq, final int attrType, final PCFMessage...messages) {
		final EnumMap<SubscriptionAttribute, Object> map = new EnumMap<SubscriptionAttribute, Object>(SubscriptionAttribute.class);
		for(final SubscriptionAttribute qa : values) {
			if(qa.attrType != attrType) continue;
			try {
				map.put(qa, qa.extract(mq, messages));
			} catch (PCFException pex) {
				pex.printStackTrace(System.err);
			}
		}
		return map;
	}
	
	private SubscriptionAttribute(final Class<?> type, final int attrType) {
		this.type = type;
		this.attrType = attrType; 
	}
	
	public final Class<?> type;
	public final int attrType;

}
