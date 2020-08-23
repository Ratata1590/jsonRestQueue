package ratata.jsonRestQueue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.rapidoid.annotation.Controller;
import org.rapidoid.annotation.DELETE;
import org.rapidoid.annotation.GET;
import org.rapidoid.annotation.POST;
import org.rapidoid.annotation.PUT;
import org.rapidoid.http.Req;
import org.rapidoid.setup.App;
import org.rapidoid.setup.Setup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Controller
public class Main {

	private static ObjectMapper mapper = new ObjectMapper();
	private static String host;
	private static int port;
	private static int queueSize = 100;
	private static boolean stop = false;
	private static final Map<String, ArrayBlockingQueue<JsonNode>> restQueue = new HashMap<String, ArrayBlockingQueue<JsonNode>>();

	@GET("/*")
	public Object download(Req req) {
		try {
			return restQueue.get(req.uri()).poll();
		} catch (Exception e) {
			req.response().code(500);
			req.response().body("".getBytes());
			return "";
		}
	}

	@POST("/*")
	public void upload(Req req) {
		try {
			if (stop) {
				throw new Exception("stoping");
			}
			ArrayBlockingQueue<JsonNode> queue;
			JsonNode data = mapper.readTree(req.body());
			if (!restQueue.containsKey(req.uri())) {
				queue = new ArrayBlockingQueue<JsonNode>(queueSize);
				queue.add(data);
				restQueue.put(req.uri(), queue);
				return;
			}
			restQueue.get(req.uri()).add(data);
		} catch (Exception e) {
			req.response().code(500);
		} finally {
			req.response().body("".getBytes());
		}
	}

	@DELETE("/*")
	public void delete(Req req) {
		try {
			ArrayBlockingQueue<JsonNode> queue = restQueue.get(req.uri());
			restQueue.remove(req.uri());
			queue.clear();
		} catch (Exception e) {
			req.response().code(500);
		} finally {
			req.response().body("".getBytes());
		}
	}

	@PUT("/info")
	public Map<String, String> getInfo(Req req) {
		Map<String, String> info = new HashMap<String, String>();
		for (String key : restQueue.keySet()) {
			ArrayBlockingQueue<JsonNode> queue = restQueue.get(key);
			info.put(key, String.valueOf(queue.size()).concat("/").concat(String.valueOf(queueSize)));
		}
		return info;
	}

	@PUT("/quit")
	public void quit() throws InterruptedException {
		stop = true;
		for (String key : restQueue.keySet()) {
			ArrayBlockingQueue<JsonNode> queue = restQueue.get(key);
			while (!queue.isEmpty()) {
				Thread.sleep(1000);
			}
		}
		System.exit(0);
	}

	@PUT("/quit_force")
	public void quit_force(Req req) {
		System.exit(0);
	}

	public static void main(String[] args) {
		App.boot();
		if (args.length > 0) {
			host = args[0];
			port = Integer.valueOf(args[1]).intValue();
			queueSize = Integer.valueOf(args[2]).intValue();
			Setup.create("RestQueue").address(host).port(port).scan();
		} else {
			Setup.create("RestQueue").address("0.0.0.0").port(9090).scan();
		}

	}

}
