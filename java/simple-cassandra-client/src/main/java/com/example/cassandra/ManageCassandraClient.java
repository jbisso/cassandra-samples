package com.example.cassandra;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.yammer.metrics.reporting.JmxReporter;

public class ManageCassandraClient {
	private MBeanServerConnection mbsConnection;
	
	public void connectMBeanServer() {
		try {
	      JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:9999/jmxrmi");
	      JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
	      mbsConnection = jmxc.getMBeanServerConnection();
      } catch (MalformedURLException mue) {
	      mue.printStackTrace();
      } catch (IOException ioe) {
	      ioe.printStackTrace();
      }
	}
	
	public void printDomains() {
		try {
	      String[] domains = mbsConnection.getDomains();
	      Arrays.sort(domains);
	      System.out.println("Domains");
	      for (String domain : domains) {
	         System.out.printf("Domain: %s\n",  domain);
         }
	      System.out.printf("Default domain: %s\n\n", mbsConnection.getDefaultDomain());
      } catch (IOException ioe) {
	      ioe.printStackTrace();
      }
		
	}
	
	public void printMBeanNames() {
		try {
	      Set<ObjectName> names = new TreeSet<ObjectName>(mbsConnection.queryNames(null,  null));
	      for (ObjectName objectName : names) {
	         System.out.printf("%s\n", objectName);
         }
      } catch (IOException ioe) {
	      ioe.printStackTrace();
      }
	}
	
	public void printNumberInserts() {
		System.out.println("Calling Yammer gauge via JMX.");
		try {
	      ObjectName objectName = new ObjectName("\"com.example.cassandra\":type=\"MetricsExample\"," +
	      		"name=\"com.example.cassandra.numberInserts\"");
	      JmxReporter.GaugeMBean mBean = JMX.newMBeanProxy(mbsConnection, objectName, JmxReporter.GaugeMBean.class);
	      System.out.printf("Number of inserts: %5d\n",  mBean.getValue());
      } catch (MalformedObjectNameException mone) {
	      mone.printStackTrace();
      } catch (NullPointerException npe) {
	      npe.printStackTrace();
      }
	}

	public static void main(String[] args) {
		ManageCassandraClient manager = new ManageCassandraClient();
		manager.connectMBeanServer();
		manager.printDomains();
		manager.printMBeanNames();
		manager.printNumberInserts();
	}
}
