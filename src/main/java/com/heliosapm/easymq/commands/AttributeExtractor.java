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

import com.heliosapm.easymq.MQ;
import com.ibm.mq.pcf.PCFException;
import com.ibm.mq.pcf.PCFMessage;

/**
 * <p>Title: AttributeExtractor</p>
 * <p>Description: Defines the procedure to extract a target attribute from a PCF response message</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.commands.AttributeExtractor</code></p>
 */

public interface AttributeExtractor {
	/**
	 * Extracts a traget attribute from the passed pcf response message array
	 * @param mq A reference to the MQ
	 * @param messages the pcf response message array
	 * @return the extracted attribute value
	 * @throws PCFException thrown on errors processing the pcf messages
	 */
	public Object extract(final MQ mq, final PCFMessage...messages) throws PCFException;
}
