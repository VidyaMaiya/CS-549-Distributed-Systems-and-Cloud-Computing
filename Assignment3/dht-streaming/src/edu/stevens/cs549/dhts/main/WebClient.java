package edu.stevens.cs549.dhts.main;

import java.net.URI;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.bind.JAXBElement;

import org.glassfish.jersey.media.sse.EventSource;
import org.glassfish.jersey.media.sse.SseFeature;

import edu.stevens.cs549.dhts.activity.DHTBase;
import edu.stevens.cs549.dhts.activity.NodeInfo;
import edu.stevens.cs549.dhts.resource.TableRep;
import edu.stevens.cs549.dhts.resource.TableRow;

public class WebClient {

	private Logger log = Logger.getLogger(WebClient.class.getCanonicalName());

	private void error(String msg) {
		log.severe(msg);
	}

	/*
	 * Encapsulate Web client operations here.
	 * 
	 * TODO: Fill in missing operations.
	 */

	/*
	 * Creation of client instances is expensive, so just create one.
	 */
	protected Client client;
	
	protected Client listenClient;

	public WebClient() {
		client = ClientBuilder.newClient();
		listenClient = ClientBuilder.newBuilder().register(SseFeature.class).build();	
	}

	private void info(String mesg) {
		Log.info(mesg);
	}

	private Response getRequest(URI uri) {
		try {
			Response cr = client.target(uri)
					.request(MediaType.APPLICATION_XML_TYPE)
					.header(Time.TIME_STAMP, Time.advanceTime())
					.get();
			processResponseTimestamp(cr);
			return cr;
		} catch (Exception e) {
			error("Exception during GET request: " + e);
			return null;
		}
	}

	private Response putRequest(URI uri, Entity<?> entity) {
		// TODO Complete.
		try {
			Response pr = client.target(uri)
					.request(MediaType.APPLICATION_XML_TYPE)
					.header(Time.TIME_STAMP, Time.advanceTime())
					.put(entity);
			processResponseTimestamp(pr);
			return pr;
		}catch(Exception e) {
			error("Exception during PUT request: "+e);
			return null;
		}
	}
	
	private Response putRequest(URI uri) {
		return putRequest(uri, Entity.text(""));
	}
	
	private Response deleteRequest(URI uri) {
		try {
			Response cr = client.target(uri)
					.request(MediaType.APPLICATION_XML_TYPE)
					.header(Time.TIME_STAMP, Time.advanceTime())
					.delete();
			processResponseTimestamp(cr);
			return cr;
		} catch(Exception e) {
			error("Exception during DELETE request: "+e);
			return null;
		}
	}
	
	private Response stopListenRequest(URI uri) {
		try {
			Response cr = client.target(uri)
					.request(MediaType.APPLICATION_XML_TYPE)
					.delete();
			return cr;
		} catch(Exception e) {
			error("Exception during DELETE request: "+e);
			return null;
		}
	}

	private void processResponseTimestamp(Response cr) {
		Time.advanceTime(Long.parseLong(cr.getHeaders().getFirst(Time.TIME_STAMP).toString()));
	}

	/*
	 * Jersey way of dealing with JAXB client-side: wrap with run-time type
	 * information.
	 */
	private GenericType<JAXBElement<NodeInfo>> nodeInfoType = new GenericType<JAXBElement<NodeInfo>>() {
	};
	
	private GenericType<JAXBElement<TableRow>> tableRowType = new GenericType<JAXBElement<TableRow>>() {
	};
	
	/*
	 * Ping a remote site to see if it is still available.
	 */
	public boolean isFailed(URI base) {
		URI uri = UriBuilder.fromUri(base).path("info").build();
		Response c = getRequest(uri);
		return c.getStatus() >= 300;
	}

	/*
	 * Get the predecessor pointer at a node.
	 */
	public NodeInfo getPred(NodeInfo node) throws DHTBase.Failed {
		URI predPath = UriBuilder.fromUri(node.addr).path("pred").build();
		info("client getPred(" + predPath + ")");
		Response response = getRequest(predPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /pred");
		} else {
			NodeInfo pred = response.readEntity(nodeInfoType).getValue();
			return pred;
		}
	}
	
	/*
	 * Get the successor pointer at a node
	 */
	public NodeInfo getSucc(NodeInfo node) throws DHTBase.Failed {
		URI succPath = UriBuilder.fromUri(node.addr).path("succ").build();
		info("client getSucc(" +succPath+ ")");
		Response response = getRequest(succPath);
		if(response == null || response.getStatus() >=300) {
			throw new DHTBase.Failed("GET /succ");
		} else {
			NodeInfo succ = response.readEntity(nodeInfoType).getValue();
			return succ;
		}
	}
	
	/*
	 * Search the network for the successor of ID.
	 */
	public NodeInfo findSuccessor(URI addr, int id) throws DHTBase.Failed {
		UriBuilder ub = UriBuilder.fromUri(addr).path("find");
		URI succPath = ub.queryParam("id", id).build();
		info("client findSuccessor(" + succPath +")");
		Response response = getRequest(succPath);
		if(response == null || response.getStatus() >=300) {
			throw new DHTBase.Failed("Get /find?id=ID");
		} else {
			NodeInfo succNode = response.readEntity(nodeInfoType).getValue();
			return succNode;
		}
	}
	
	/*
	 * Find closest preceding finger of ID
	 */
	public NodeInfo closestPrecedingFinger(NodeInfo node,int id) throws DHTBase.Failed  {
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("finger");
		URI closestPrecedingFingerPath = ub.queryParam("id", id).build();
		info("client closestPrecedingFinger("+ closestPrecedingFingerPath +")");
		Response response = getRequest(closestPrecedingFingerPath);
		if(response == null || response.getStatus() >=300) {
			throw new DHTBase.Failed("Get /finger?id=ID");
		}else {
			NodeInfo closestPrecedingFinger = response.readEntity(nodeInfoType).getValue();
			return closestPrecedingFinger;
		}
		
	}
	
	/*
	 * Get List of values for the key
	 */
	public String[] get(NodeInfo node, String key) throws DHTBase.Failed {
		UriBuilder ub = UriBuilder.fromUri(node.addr);
		URI getKeyValuePath = ub.queryParam("key", key).build();
		info("client getKeyValuePath("+getKeyValuePath+")");
		Response response = getRequest(getKeyValuePath);
		if(response == null || response.getStatus()>=300) {
			throw new DHTBase.Failed("Get ?key=KEY");
		} else {
			TableRow tr = response.readEntity(tableRowType).getValue();
			return tr.vals;
		}
	}
	
	/*
	 * Add a key and a value
	 */
	public void add(NodeInfo node, String key, String value) throws DHTBase.Failed {
		UriBuilder ub = UriBuilder.fromUri(node.addr);
		URI addKeyValuePath = ub.queryParam("key", key).queryParam("val", value).build();
		info("client addKeyValuePath("+addKeyValuePath+")");
		Response response = putRequest(addKeyValuePath);
		if(response == null || response.getStatus() >=300) {
			throw new DHTBase.Failed("PUT ?key=KEY&val=VAL");
			} 
	}
	
	/*
	 * Delete a key and a value
	 */
	public void delete(NodeInfo node, String key, String value) throws DHTBase.Failed {
		UriBuilder ub = UriBuilder.fromUri(node.addr);
		URI deleteKeyPath = ub.queryParam("key", key).queryParam("val", value).build();
		info("client deleteKeyPath("+deleteKeyPath+")");
		Response response = deleteRequest(deleteKeyPath);
		if(response == null || response.getStatus() >=300) {
			throw new DHTBase.Failed("DELETE ?key=KEY&val=VAL");
			} 
	}

	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public TableRep notify(NodeInfo node, TableRep predDb) throws DHTBase.Failed {
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null. This is represented in HTTP by RC=304 (Not Modified).
		 */
		NodeInfo thisNode = predDb.getInfo();
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("notify");
		URI notifyPath = ub.queryParam("id", thisNode.id).build();
		info("client notify(" + notifyPath + ")");
		Response response = putRequest(notifyPath, Entity.xml(predDb));
		if (response != null && response.getStatusInfo() == Response.Status.NOT_MODIFIED) {
			/*
			 * Do nothing, the successor did not accept us as its predecessor.
			 */
			return null;
		} else if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("PUT /notify?id=ID");
		} else {
			TableRep bindings = response.readEntity(TableRep.class);
			return bindings;
		}
	}

	
	public EventSource listenForBindings(NodeInfo node, int id, String skey) throws DHTBase.Failed {
		// TODO listen for SSE subscription requests on http://.../dht/listen?key=<key>
		// On the service side, don't expect LT request or response headers for this request.
		// Note: "id" is client's id, to enable us to stop event generation at the server.
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("listen");
		URI listenForBindingsPath = ub.queryParam("id", id).queryParam("key", skey).build();
		info("client listenForBindings("+listenForBindingsPath+")");
		WebTarget target = listenClient.target(listenForBindingsPath);
		return EventSource.target(target).build();		
	}

	public void listenOff(NodeInfo node, int id, String skey) throws DHTBase.Failed {
		// TODO listen for SSE subscription requests on http://.../dht/listen?key=<key>
		// On the service side, don't expect LT request or response headers for this request.
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("listen");
		URI listenOffPath = ub.queryParam("id", id).queryParam("key", skey).build();
		info("client listenOff("+listenOffPath+")");
		Response response = stopListenRequest(listenOffPath);
		if(response == null || response.getStatus() >=300) {
			throw new DHTBase.Failed("DELETE/listen?key=KEY&val=VAL");
			} 
	}

}
