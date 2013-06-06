package com.example.cassandra;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

public class MetricsExample extends SimpleClient {
	private static PreparedStatement preparedSelect;

	private Concordance concordance;
	private int numberInserts = 0;
	private List<InsertMetrics> insertMetrics;

	public MetricsExample() {
		this.insertMetrics = new ArrayList<InsertMetrics>();
   }
	
   @Override
	public void createSchema() {
		getSession().execute(
         "CREATE KEYSPACE lexicon WITH replication " + 
         "= {'class':'SimpleStrategy', 'replication_factor':2};");
		getSession().execute(
         "CREATE TABLE lexicon.concordance (" +
               "id uuid," + 
               "word text PRIMARY KEY, " +
               "contexts list<text>, " +
               "occurrences int )");
	}
   
   @Override
   public void loadData() {
		concordance = new Concordance();
		concordance.analyzeText(new File(System.getProperty("user.home") 
				+ "/Documents/houndBaskervilles.txt"), "HoB");
		PreparedStatement preparedInsert = getSession().prepare(
            "INSERT INTO lexicon.concordance " +
            "(id, word, contexts, occurrences) " +
            "VALUES (?, ?, ?, ?);");
		BoundStatement boundInsert;
		for (String entry : concordance.getEntries().keySet()) {
			numberInserts += 1;
      	boundInsert = new BoundStatement(preparedInsert);
      	//boundInsert.setConsistencyLevel(ConsistencyLevel.ANY);
      	UUID uuid = UUID.randomUUID();
      	List<String> contexts = concordance.getEntries().get(entry);
         boundInsert.bind(uuid, entry, contexts, contexts.size());
         try {
         	ResultSet results = getSession().execute(boundInsert);
         	ExecutionInfo execInfo = results.getExecutionInfo();
         	InsertMetrics metric = new InsertMetrics();
         	metric.setQueriedNode(execInfo.getQueriedHost());
         	metric.setConsistencyLevelAchieved(execInfo.getAchievedConsistencyLevel());
         	metric.setRowId(uuid);
         	this.insertMetrics.add(metric);
         } catch (Exception e) {
         	System.err.printf("Problem inserting data: %s\n", e.getMessage());
         }
      }
		System.out.println("Data loaded.");
   }

	public void queryData(String word) {
		BoundStatement boundSelect = new BoundStatement(preparedSelect);
		boundSelect.bind(word);
		ResultSet results = getSession().execute(boundSelect);
		Row row = results.one();
		System.out.printf("Word: %s; occurrences: %d\n", word, row.getList("contexts", String.class).size());
		for (String context : row.getList("contexts", String.class)) {
			System.out.printf("%s\n", context);
      }
   }

   public void addMetrics() {
	   getSession().getCluster().getMetrics().getRegistry()
	   	.newGauge(
	   			getClass(),
	   			"com.example.cassandra.numberInserts", 
	   			new Gauge<Integer>() {
	   				@Override
			   		public Integer value() {
			   			return numberInserts;
			   		}
	   	});
   }
   
   public void addMetrics02() {
	   getSession().getCluster().getMetrics().getRegistry()
   	.newGauge(
   			getClass(),
   			"com.example.cassandra.insertMetrics", 
   			new Gauge<List<InsertMetrics>>() {
   				@Override
		   		public List<InsertMetrics> value() {
		   			return insertMetrics;
		   		}
   	});
   }

	public void listMetrics() {
   	Map<MetricName, Metric> metrics = getSession().getCluster().getMetrics().getRegistry().allMetrics();
	   for (MetricName metricName : metrics.keySet()) {
	      System.out.printf("%s:%s - %s\n", 
	      		metricName.getType(), 
	      		metricName.getName(), 
	      		metrics.get(metricName).getClass() );
      }
   }

	public void printMetrics() {
      System.out.println("Metrics");
      Metrics metrics = getSession().getCluster().getMetrics();
      Gauge<Integer> gauge = metrics.getConnectedToHosts();
      Integer numberOfHosts = gauge.value();
      System.out.printf("Number of hosts: %d\n", numberOfHosts);
      Metrics.Errors errors = metrics.getErrorMetrics();
      Counter counter = errors.getReadTimeouts();
      System.out.printf("Number of read timeouts: %d\n", counter.count());
      Timer timer = metrics.getRequestsTimer();
      System.out.printf("Number of user requests: %d %s\n", timer.count(), timer.eventType());
      Metric ourMetric = getSession()
      		.getCluster()
      		.getMetrics()
      		.getRegistry()
      		.allMetrics()
      		.get(new MetricName(getClass(), "com.example.cassandra.numberInserts"));
      System.out.printf("Number of insert statements executed: %5d\n", ((Gauge<?>) ourMetric).value());
   }
	
	public void printMetrics02() {
      Metric ourMetric = getSession()
      		.getCluster()
      		.getMetrics()
      		.getRegistry()
      		.allMetrics()
      		.get(new MetricName(getClass(), "com.example.cassandra.insertMetrics"));
		@SuppressWarnings("unchecked")
      List<InsertMetrics> inserts = (List<InsertMetrics>) ((Gauge<?>) ourMetric).value();
		for (InsertMetrics insertMetrics : inserts) {
			System.out.printf("Host queried: %s; rowId: %s; achieved consistency level: %s\n", 
					insertMetrics.getQueriedNode().getAddress(), 
					insertMetrics.getRowId(),
					insertMetrics.getAchievedConsistencyLevel());
      }
	}
  
   public static void main(String[] args) {
      MetricsExample client = new MetricsExample();
      client.connect("127.0.0.1");
      client.createSchema();
		preparedSelect = client.getSession().prepare(
				"SELECT * FROM lexicon.concordance WHERE word = ?;");
      client.loadData();
      client.listMetrics();
      client.addMetrics();
      client.addMetrics02();
      client.printMetrics();
      client.printMetrics02();
      client.queryData("revolver");
      try {
      	Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException ie) {
      	ie.printStackTrace();
      }
      client.dropSchema("lexicon");
      client.close();
   }
   
   public class InsertMetrics {
   	private Host queriedNode;
   	private UUID rowId;
   	private ConsistencyLevel achievedConsistencyLevel;

   	public InsertMetrics() {
   	}

		public Host getQueriedNode() {
	      return queriedNode;
      }

		public void setQueriedNode(Host queriedNode) {
	      this.queriedNode = queriedNode;
      }

		public UUID getRowId() {
	      return rowId;
      }

		public void setRowId(UUID rowId) {
	      this.rowId = rowId;
      }

		public ConsistencyLevel getAchievedConsistencyLevel() {
	      return achievedConsistencyLevel;
      }

		public void setConsistencyLevelAchieved(ConsistencyLevel consistencyLevelAchieved) {
	      this.achievedConsistencyLevel = consistencyLevelAchieved;
      }
   }
}
