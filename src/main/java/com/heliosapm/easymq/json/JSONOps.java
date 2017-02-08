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
package com.heliosapm.easymq.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;



/**
 * <p>Title: JSONOps</p>
 * <p>Description: JSON utilities</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.json.JSONOps</code></p>
 */

public class JSONOps {
	private static final ObjectMapper jsonMapper = new ObjectMapper();
	private static final JsonFactory jsonFactory = jsonMapper.getFactory();
	static {
		jsonMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
	}
	
	/**
	 * Returns a reference to the static ObjectMapper
	 * @return The ObjectMapper
	 */
	public final static ObjectMapper getMapper() {
		return jsonMapper;
	}

	/**
	 * Returns a reference to the JsonFactory for streaming creation
	 * @return The JsonFactory object
	 */
	public final static JsonFactory getFactory() {
		return jsonFactory;
	}
	
	/**
	 * Returns a shareable node factory
	 * @return a json node factory
	 */
	public static JsonNodeFactory getNodeFactory() {
		return JsonNodeFactory.instance;
	}
	
	/**
	 * Reads and parses the data from the passed input stream into a JsonNode
	 * @param is The input stream to read from
	 * @return the parsed JsonNode
	 */
	public static JsonNode parseToNode(final InputStream is) {
		if (is == null) throw new IllegalArgumentException("InputStream was null");
		try {
			return jsonMapper.readTree(is);
		} catch (Exception e) {
			throw new JSONException(e);
		}
	}
	
	/**
	 * Reads and parses the data from the passed URL into a JsonNode
	 * @param url The url to read from
	 * @return the parsed JsonNode
	 */
	public static JsonNode parseToNode(final URL url) {
		if (url == null) throw new IllegalArgumentException("URL was null");
		InputStream is = null;
		try {
			is = url.openStream();
			return parseToNode(is);
		} catch (Exception e) {
			throw new JSONException(e);
		} finally {
			if(is!=null) try { is.close(); } catch (Exception x) {/* No Op */}
		}
	}
	
	/**
	 * Deserializes a JSON node to a specific class type
	 * <b>Note:</b> If you get mapping exceptions you may need to provide a 
	 * TypeReference
	 * @param json The node to deserialize
	 * @param pojo The class type of the object used for deserialization
	 * @return An object of the {@code pojo} type
	 * @throws IllegalArgumentException if the data or class was null or parsing 
	 * failed
	 * @throws JSONException if the data could not be parsed
	 */
	public static final <T> T parseToObject(final JsonNode json, final Class<T> pojo) {
		if (json == null)
			throw new IllegalArgumentException("Incoming data was null or empty");
		if (pojo == null)
			throw new IllegalArgumentException("Missing class type");
		return jsonMapper.convertValue(json, pojo);		
	}
	
	/**
	 * Deserializes a JSON formatted string to a specific class type
	 * <b>Note:</b> If you get mapping exceptions you may need to provide a 
	 * TypeReference
	 * @param json The string to deserialize
	 * @param pojo The class type of the object used for deserialization
	 * @return An object of the {@code pojo} type
	 * @throws IllegalArgumentException if the data or class was null or parsing 
	 * failed
	 * @throws JSONException if the data could not be parsed
	 */
	public static final <T> T parseToObject(final JsonNode json, final TypeReference<T> pojo) {
		if (json == null)
			throw new IllegalArgumentException("Incoming data was null or empty");
		if (pojo == null)
			throw new IllegalArgumentException("Missing class type");

		try {
			return jsonMapper.convertValue(json, pojo);	
		} catch (Exception e) {
			throw new JSONException(e);
		}
	}
	
	/**
	 * Serializes the given object to a JSON string
	 * @param object The object to serialize
	 * @return A JSON formatted string
	 * @throws IllegalArgumentException if the object was null
	 * @throws JSONException if the object could not be serialized
	 */
	public static final String serializeToString(final Object object) {
		if (object == null)
			throw new IllegalArgumentException("Object was null");
		try {
			return jsonMapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			throw new JSONException(e);
		}
	}
	
	/**
	 * Serializes the given object to a JSON byte stream and writes it to the passed output stream.
	 * @param object The object to serialize
	 * @param os The output stream to write to
	 * @throws IllegalArgumentException if the object or output stream was null
	 * @throws JSONException if the object could not be serialized
	 * @throws IOException thrown on io errors in the output
	 */
	public static final void serializeOut(final Object object, final OutputStream os) throws IOException {
		if (object == null) throw new IllegalArgumentException("Object was null");
		if (os == null) throw new IllegalArgumentException("OutputStream was null");
		try {
			jsonMapper.writeValue(os, object);
			os.flush();
		} catch (JsonProcessingException e) {
			throw new JSONException(e);
		}
	}
	

}
