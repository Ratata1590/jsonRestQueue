package ratata.jsonRestQueue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.rapidoid.annotation.Controller;
import org.rapidoid.annotation.GET;
import org.rapidoid.annotation.POST;
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
	private static final Map<String, ArrayBlockingQueue<JsonNode>> restQueue = new HashMap<String, ArrayBlockingQueue<JsonNode>>();

	@GET("/*")
	public Object hey(Req req) {
		try {
			return restQueue.get(req.uri()).poll();
		} catch (Exception e) {
			req.response().code(500);
			req.response().body("".getBytes());
			return "";
		}
	}

	@POST("/*")
	public void size(Req req) {
		try {
			ArrayBlockingQueue<JsonNode> queue;
			JsonNode data = mapper.readTree(req.body());
			if (!restQueue.containsKey(req.uri())) {
				queue = new ArrayBlockingQueue<JsonNode>(queueSize);
				queue.add(data);
				restQueue.put(req.uri(), queue);
			} else {
				restQueue.get(req.uri()).add(data);
			}
		} catch (Exception e) {
			req.response().code(500);
		}
		req.response().body("".getBytes());
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
