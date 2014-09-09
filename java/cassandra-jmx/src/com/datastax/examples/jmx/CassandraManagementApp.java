package com.datastax.examples.jmx;

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

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogMBean;

public class CassandraManagementApp {
    private MBeanServerConnection mbsConnection;
    
    public CassandraManagementApp() {
    }
    
    public void connectMBeanServer() {
        try {
          JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://localhost:1099/jndi/rmi://localhost:7100/jmxrmi");
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
          System.out.println("MBean");
          for (ObjectName objectName : names) {
             System.out.printf("%s\n", objectName);
          }
        } catch (IOException ioe) {
              ioe.printStackTrace();
        }
    }

    private void printValue(String mbeanName) {
        try {
            ObjectName objectName = new ObjectName(mbeanName);
            CommitLogMBean mbean = JMX.newMBeanProxy(mbsConnection, objectName, CommitLogMBean.class);
            for ( String segment :  mbean.getArchivingSegmentNames() ) {
                System.out.println("Archiving segment: " + segment);
            }
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        CassandraManagementApp theApp = new CassandraManagementApp();
        theApp.connectMBeanServer();
        theApp.printDomains();
        //theApp.printMBeanNames();
        theApp.printValue("org.apache.cassandra.db:type=Commitlog");
    }
}

