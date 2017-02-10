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
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.heliosapm.easymq.MQ;

import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.CMQCFC;
import com.ibm.mq.pcf.PCFException;
import com.ibm.mq.pcf.PCFMessage;

/**
 * <p>Title: QueueAttribute</p>
 * <p>Description: Functional enumeration of MQ queue attributes</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.commands.QueueAttribute</code></p>
 */

public enum QueueAttribute implements AttributeExtractor {
	NAME(String.class) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			return messages[0].getStringParameterValue(CMQC.MQCA_Q_NAME).trim();
		}
	},
	ADMIN(boolean.class) {
		final Pattern NON_ADMIN_QUEUES = Pattern.compile("SYSTEM\\..*||AMQ\\..*", Pattern.CASE_INSENSITIVE);
		@Override
		public Object extract(MQ mq, PCFMessage... messages) throws PCFException {
			return !NON_ADMIN_QUEUES.matcher(messages[0].getStringParameterValue(CMQC.MQCA_Q_NAME)).matches();
		}
	},
	QUEUE_DEPTH(Integer.class){
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			return messages[0].getIntParameterValue(CMQC.MQIA_CURRENT_Q_DEPTH);
		}
	},
	LAST_GET(Date.class) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final StringBuilder b = new StringBuilder(MQ.DATE_LENGTH);
			b.append(messages[0].getStringParameterValue(CMQCFC.MQCACF_LAST_GET_DATE))
				.append(" ")
				.append(messages[0].getStringParameterValue(CMQCFC.MQCACF_LAST_GET_TIME));
			return b.length() >= MQ.DATE_LENGTH ? MQ.fromStringy(b) : null;
		}			
	},
	LAST_PUT(Date.class) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			final StringBuilder b = new StringBuilder(MQ.DATE_LENGTH);
			b.append(messages[0].getStringParameterValue(CMQCFC.MQCACF_LAST_PUT_DATE))
				.append(" ")
				.append(messages[0].getStringParameterValue(CMQCFC.MQCACF_LAST_PUT_TIME));
			return b.length() >= MQ.DATE_LENGTH ? MQ.fromStringy(b) : null;
		}			
	},
	OLDEST_MSG_AGE(Integer.class) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			return messages[0].getIntParameterValue(CMQCFC.MQIACF_OLDEST_MSG_AGE);
		}			
	},
	ON_Q_TIME(int[].class) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			return messages[0].getIntListParameterValue(CMQCFC.MQIACF_Q_TIME_INDICATOR);
		}			
	},
	OPEN_INPUTS(Integer.class) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			return messages[0].getIntParameterValue(CMQC.MQIA_OPEN_INPUT_COUNT);
		}			
	},
	OPEN_OUTPUTS(Integer.class) {
		@Override
		public Object extract(final MQ mq, final PCFMessage... messages) throws PCFException {
			return messages[0].getIntParameterValue(CMQC.MQIA_OPEN_OUTPUT_COUNT);
		}			
	};
	
	private static final QueueAttribute[] values = values();
	public static final Set<QueueAttribute> VALUE_SET = Collections.unmodifiableSet(EnumSet.allOf(QueueAttribute.class));
	
	
	public static Map<QueueAttribute, Object> extractQueueAttributes(final MQ mq, final PCFMessage...messages) {
		final EnumMap<QueueAttribute, Object> map = new EnumMap<QueueAttribute, Object>(QueueAttribute.class);
		for(final QueueAttribute qa : values) {
			try {
				map.put(qa, qa.extract(mq, messages));
			} catch (PCFException pex) {
				pex.printStackTrace(System.err);
			}
		}
		return map;
	}
	
	private QueueAttribute(final Class<?> type) {
		this.type = type;
	}
	
	public final Class<?> type;

}
