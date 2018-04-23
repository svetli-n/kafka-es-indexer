import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Main {

  private static Properties props;

  public static void main(String... args) throws InterruptedException, IOException {

    runLoadTest(args);

  }

  private static void runLoadTest(String[] args) throws IOException, InterruptedException {
    props = getProperties(args);

    int numWorkers = Integer.valueOf(props.getProperty("loadtest.numWorkers"));

    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(numWorkers);
    ArrayList<KafkaClient> workers = new ArrayList<>(numWorkers);

    for (int i = 0; i < numWorkers; i++) {

      ElasticIndexer elasticIndexer = new ElasticIndexer(props, i);
      KafkaClient worker = new KafkaClient(props, i, elasticIndexer);

      workers.add(worker);
      executorService.submit(worker);

    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      workers.stream().forEach(KafkaClient::shutdown);
      executorService.shutdown();
      while (!executorService.isTerminated()) {
        //wait for all tasks to finish
      }
    }));

    Thread.sleep(Long.MAX_VALUE);
  }


  private static Properties getProperties(String... args) throws IOException {
    String path = "";
    if (args.length == 1 && !args[0].endsWith("/")) {
      path = args[0] + "/";
    }
    Properties props = new Properties();
    try (InputStream resourceStream = new FileInputStream(path + "app.properties")) {
      props.load(resourceStream);
    }
    return props;
  }

}


