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
package com.heliosapm.easymq.http;

import static spark.Spark.awaitInitialization;
import static spark.Spark.before;
import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.staticFiles;
import static spark.Spark.stop;
import static spark.Spark.threadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.heliosapm.easymq.MQ;
import com.heliosapm.easymq.MQConfig;
import com.heliosapm.easymq.json.JSONOps;

import spark.Request;
import spark.Response;

/**
 * <p>Title: HttpServer</p>
 * <p>Description: The easymq http server to provide REST and UI services</p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.easymq.http.HttpServer</code></p>
 */

public class HttpServer {
	/** The singleton instance */
	private static volatile HttpServer instance = null;
	/** The singleton instance ctor lock */
	private static Object lock = new Object();

	
	/** The listening port */
	private final int port;
	/** The maximum number of threads */
	private final int maxThreads;
	/** The minimum number of threads */
	private final int minThreads;
	/** The thread task timeout in ms. */
	private final int timeOutMillis;
	
	private static final JsonNode EMPTY_NODE = JSONOps.getNodeFactory().nullNode();
	private static final Pattern URI_SPLITTER = Pattern.compile("/");
	
	/** The JSON content type */
	public static final String JSON_TYPE = "application/json";
	/** The JSON response mq instance header key */
	public static final String MQ_KEY_HEADER = "easymq.key";
	/** The JSON response mq instance header key */
	public static final String MQ_JSON_KEY_HEADER = "easymq.key.json";
	/** The JSON response mq pool name header key */
	public static final String MQ_POOLNAME_HEADER = "easymq.poolname";
	
	/** The handler return for successful routes */
	public static final String OK_200 = "200 OK";
	/** The handler return for resource not found routes */
	public static final String NOTFOUND_404 = "404 NOTFOUND";
	/** The handler return for resource not found routes */
	public static final String ERROR_500 = "500 Internal Server Error";
	
	
	/** Instance logger */
	protected final Logger log = LoggerFactory.getLogger(getClass());

	/**
	 * Acquires the HttpServer singleton instance
	 * @return the HttpServer singleton
	 */
	public static HttpServer getInstance() {
		if(instance==null) {
			synchronized(lock) {
				if(instance==null) {					
					instance = new HttpServer();
				}
			}
		}
		return instance;
	}
	
	
	private HttpServer() {
		final JsonNode httpNode = MQConfig.getInstance().getConfigNode("httpServer");
		port = nvl(httpNode.get("port")).asInt(1892);
		maxThreads = nvl(httpNode.get("maxThreads")).asInt(Runtime.getRuntime().availableProcessors() * 2);
		minThreads = nvl(httpNode.get("minThreads")).asInt(2);
		timeOutMillis = nvl(httpNode.get("timeOutMillis")).asInt(10000);
		initialize();
	}
	
	/**
	 * Stops the http server
	 */
	public void shutdown() {
		stop();
	}
	
	protected static JsonNode nvl(final JsonNode node) {
		return node==null ? EMPTY_NODE : node;
	}
	
	protected void initialize() {
		port(port);		
		threadPool(maxThreads, minThreads, timeOutMillis);
		staticFiles.location("/ui");
		before((request, response) -> {
			final String[] frags = splitUri(request);
			log.info("URI frags: {}", Arrays.toString(frags));
		});
		get("/ping", (req, res) -> {
			res.type(JSON_TYPE);
			return "{\"msg\":\"pong\"}";
		});
		get("/qnames/:mq", (req, res) -> {					
			final MQ mq = MQ.getInstance(req.params(":mq"), true);
			if(mq==null) return err(res, 404, "Failed to find MQ instance [" + req.params(":mq") + "]");
			return sendMQResponse(res, mq.getQueueNames(), mq);
		});
		get("/tnames/:mq", (req, res) -> {					
			final MQ mq = MQ.getInstance(req.params(":mq"), true);
			if(mq==null) return err(res, 404, "Failed to find MQ instance [" + req.params(":mq") + "]");
			return sendMQResponse(res, mq.getTopicNames(), mq);
		});
		
		get("/subnames/:topic/:mq", (req, res) -> {					
			final MQ mq = MQ.getInstance(req.params(":mq"), true);
			if(mq==null) return err(res, 404, "Failed to find MQ instance [" + req.params(":mq") + "]");
			final String topicName = req.params(":topic");
			if(!mq.topicExists(topicName)) return err(res, 404, "Topic does not exist [" + topicName + "]");
			return sendMQResponse(res, mq.getTopicSubscriptions(topicName), mq);			
		});
		
		awaitInitialization(); 
		log.info("HTTP Server Started on [{}]", port);
	}
	
	
	protected String err(final Response res, final int code, final String errorMessage) {
		res.type(JSON_TYPE);
		res.status(code);
		return "{\"error\":\"" + errorMessage + "\"}";
	}
	
	/**
	 * Standard send of an MQ query response
	 * @param res The spark http response
	 * @param object The response object (will be jsonized using JSONOps)
	 * @param mq The MQ instance the query was executed against
	 * @throws IOException thrown on any io error
	 */
	protected String sendMQResponse(final Response res, final Object object, final MQ mq) throws IOException {
		try {
			final HttpServletResponse raw = res.raw();
			raw.setContentType(JSON_TYPE);
			raw.setStatus(200);
			raw.setHeader(MQ_KEY_HEADER, mq.key().toString());
			raw.setHeader(MQ_POOLNAME_HEADER, mq.poolName());
			JSONOps.serializeOut(object, raw.getOutputStream());
			return OK_200;
		} catch (Exception ex) {
			return err(res, 500, ex.getMessage());
		}
	}
	
	
	protected String[] splitUri(final Request request) {
		final String[] frags = URI_SPLITTER.split(request.uri());		
		final String[] trimmed = new String[frags.length-1];
		System.arraycopy(frags, 1, trimmed, 0, frags.length-1);
		return trimmed;
	}

}
