package ratata.jsonRestQueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.rapidoid.annotation.Controller;
import org.rapidoid.annotation.DELETE;
import org.rapidoid.annotation.GET;
import org.rapidoid.annotation.Header;
import org.rapidoid.annotation.POST;
import org.rapidoid.annotation.PUT;
import org.rapidoid.http.Req;
import org.rapidoid.setup.App;
import org.rapidoid.setup.Setup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Controller
public class Main {

	private static final String extension = ".ratata_queue";
	private static ObjectMapper mapper = new ObjectMapper();
	private static String queueName = "RestQueue";
	private static String host;
	private static int port;
	private static int queueSize = 100;
	private static boolean stop = false;
	private static Hashtable<String, ArrayBlockingQueue<JsonNode>> restQueue = new Hashtable<String, ArrayBlockingQueue<JsonNode>>();

	@GET("/*")
	public Object download(Req req, @Header("blocking") Boolean blocking) {
		try {
			if (stop) {
				throw new Exception("stoping");
			}
			if (Boolean.TRUE.equals(blocking)) {
				return restQueue.get(req.uri()).take();
			}
			return restQueue.get(req.uri()).poll();
		} catch (Exception e) {
			req.response().code(500);
			req.response().body("".getBytes());
			return "";
		}
	}

	@POST("/*")
	public void upload(Req req, @Header("blocking") Boolean blocking) {
		try {
			if (stop) {
				throw new Exception("stoping");
			}
			JsonNode data = mapper.readTree(req.body());
			if (!restQueue.containsKey(req.uri())) {
				restQueue.put(req.uri(), new ArrayBlockingQueue<JsonNode>(queueSize));
			}
			if (Boolean.TRUE.equals(blocking)) {
				restQueue.get(req.uri()).put(data);
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
	public Map<String, Object> getInfo(Req req) {
		Map<String, Object> result = new HashMap<String, Object>();
		Map<String, String> info = new HashMap<String, String>();
		for (String key : restQueue.keySet()) {
			ArrayBlockingQueue<JsonNode> queue = restQueue.get(key);
			info.put(key, String.valueOf(queue.size()).concat("/").concat(String.valueOf(queueSize)));
		}
		result.put("queueName", queueName);
		result.put("queueSize", queueSize);
		result.put("host", host);
		result.put("port", port);
		result.put("stop", stop);
		result.put("info", info);
		return result;
	}

	@PUT("/quit")
	public void quit(@Header("force") Boolean force) {
		try {
			if (stop) {
				throw new Exception("stoping");
			}
			stop = true;
			if (Boolean.TRUE.equals(force)) {
				System.exit(0);
				return;
			}
			saveQueue();
			System.exit(0);
		} catch (Exception e) {
		}
	}

	public static void saveQueue() throws Exception {
		FileOutputStream fos = new FileOutputStream(queueName.concat(extension));
		PrintWriter writer = new PrintWriter(fos);
		writer.println(queueSize);
		for (String key : restQueue.keySet()) {
			ArrayBlockingQueue<JsonNode> queue = restQueue.get(key);
			writer.println("#".concat(key));
			while (!queue.isEmpty()) {
				JsonNode data = queue.poll();
				writer.println(mapper.writeValueAsString(data));
			}
		}
		writer.flush();
		fos.flush();
		writer.close();
		fos.close();
	}

	public static void loadQueue() throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(queueName.concat(extension)));
		String key = null;
		String line = reader.readLine();
		queueSize = Integer.valueOf(line).intValue();
		while (true) {
			line = reader.readLine();
			if (line == null) {
				break;
			}
			if (line.startsWith("#")) {
				key = line.substring(1);
				restQueue.put(key, new ArrayBlockingQueue<JsonNode>(queueSize));
				continue;
			}
			restQueue.get(key).put(mapper.readTree(line));
		}
		reader.close();
	}

	public static void main(String[] args) throws Exception {
		App.boot();
		if (args.length > 0) {
			queueName = args[0];
			host = args[1];
			port = Integer.valueOf(args[2]).intValue();
			queueSize = Integer.valueOf(args[3]).intValue();

		} else {
			host = "0.0.0.0";
			port = 9090;
		}
		Setup.create(queueName).address(host).port(port).scan();
		if ((new File(queueName.concat(extension))).exists()) {
			loadQueue();
		}
	}

}
