package org.jmxcassandra;

import java.io.IOException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.Map;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import com.yammer.metrics.reporting.JmxReporter;
import com.yammer.metrics.reporting.JmxReporter.GaugeMBean;

public class JmxConnect implements AutoCloseable {
	private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";

	@SuppressWarnings("unused")
	private static final int defaultPort = 7199;

	final String host;
	final int port;
	private String username;
	private String password;

	private JMXConnector jmxc;
	private MBeanServerConnection mbeanServerConn;
	private GaugeMBean guageMBean;

	private boolean failed;

	/**
	 * Creates a JmxConnect using the specified JMX host, port, username, and
	 * password.
	 *
	 * @param host
	 *            hostname or IP address of the JMX agent
	 * @param port
	 *            TCP port of the remote JMX agent
	 * @throws IOException
	 *             on connection failures
	 */
	public JmxConnect(String host, int port, String username, String password) throws IOException {
		assert username != null && !username.isEmpty() && password != null
				&& !password.isEmpty() : "neither username nor password can be blank";

		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		connect();
	}

	/**
	 * Creates a NodeProbe using the specified JMX host and port.
	 *
	 * @param host
	 *            hostname or IP address of the JMX agent
	 * @param port
	 *            TCP port of the remote JMX agent
	 * @throws IOException
	 *             on connection failures
	 */
	public JmxConnect(String host, int port) throws IOException {
		this.host = host;
		this.port = port;
		connect();
	}

	/**
	 * Create a connection to the JMX agent and setup the M[X]Bean proxies.
	 *
	 * @throws IOException
	 *             on connection failures
	 */
	private void connect() throws IOException {
		JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
		Map<String, Object> env = new HashMap<String, Object>();
		if (username != null) {
			String[] creds = { username, password };
			env.put(JMXConnector.CREDENTIALS, creds);
			env.put("jmx.remote.x.server.connection.timeout", new Long(120000L));
		}

		env.put("com.sun.jndi.rmi.factory.socket", getRMIClientSocketFactory());

		jmxc = JMXConnectorFactory.connect(jmxUrl, env);
		mbeanServerConn = jmxc.getMBeanServerConnection();

	}

	private RMIClientSocketFactory getRMIClientSocketFactory() throws IOException {
		if (Boolean.parseBoolean(System.getProperty("ssl.enable")))
			return new SslRMIClientSocketFactory();
		else
			return RMISocketFactory.getDefaultSocketFactory();
	}

	public void close() throws IOException {
		jmxc.close();
	}

	public boolean isFailed() {
		return failed;
	}

	/**
	 * Retrieve Client metrics
	 * 
	 * @param metricName
	 *            connectedNativeClients, connectedThriftClients
	 * @return
	 */
	public Object getConnectedClients(String metricName) {
		try {
			ObjectName name = new ObjectName(
					String.format("org.apache.cassandra.metrics:type=Client,name=%s", metricName));
			guageMBean = JMX.newMBeanProxy(mbeanServerConn, name, JmxReporter.GaugeMBean.class);

			return guageMBean.getValue();
		} catch (MalformedObjectNameException e) {
			throw new RuntimeException("Invalid ObjectName? Please report this as a bug.", e);
		}
	}

	/**
	 * Retrieve ColumnFamily metrics
	 * 
	 * @param ks
	 *            Keyspace for which stats are to be displayed.
	 * @param cf
	 *            ColumnFamily for which stats are to be displayed.
	 * @param metricName
	 *            View {@link org.apache.cassandra.metrics.ColumnFamilyMetrics}.
	 */
	public Object getColumnFamilyMetric(String ks, String cf, String metricName) {
		try {
			String type = cf.contains(".") ? "IndexColumnFamily" : "ColumnFamily";
			ObjectName oName = new ObjectName(String.format(
					"org.apache.cassandra.metrics:type=%s,keyspace=%s,scope=%s,name=%s", type, ks, cf, metricName));
			switch (metricName) {
			case "BloomFilterDiskSpaceUsed":
			case "BloomFilterFalsePositives":
			case "BloomFilterFalseRatio":
			case "BloomFilterOffHeapMemoryUsed":
			case "IndexSummaryOffHeapMemoryUsed":
			case "CompressionMetadataOffHeapMemoryUsed":
			case "CompressionRatio":
			case "EstimatedColumnCountHistogram":
			case "EstimatedRowSizeHistogram":
			case "EstimatedRowCount":
			case "KeyCacheHitRate":
			case "LiveSSTableCount":
			case "MaxRowSize":
			case "MeanRowSize":
			case "MemtableColumnsCount":
			case "MemtableLiveDataSize":
			case "MemtableOffHeapSize":
			case "MinRowSize":
			case "RecentBloomFilterFalsePositives":
			case "RecentBloomFilterFalseRatio":
			case "SnapshotsSize":
				return JMX.newMBeanProxy(mbeanServerConn, oName, JmxReporter.GaugeMBean.class).getValue();
			case "LiveDiskSpaceUsed":
			case "MemtableSwitchCount":
			case "SpeculativeRetries":
			case "TotalDiskSpaceUsed":
			case "WriteTotalLatency":
			case "ReadTotalLatency":
			case "PendingFlushes":
				return JMX.newMBeanProxy(mbeanServerConn, oName, JmxReporter.CounterMBean.class).getCount();
			case "ReadLatency":
			case "CoordinatorReadLatency":
			case "CoordinatorScanLatency":
			case "WriteLatency":
				return JMX.newMBeanProxy(mbeanServerConn, oName, JmxReporter.TimerMBean.class);
			case "LiveScannedHistogram":
			case "SSTablesPerReadHistogram":
			case "TombstoneScannedHistogram":
				return JMX.newMBeanProxy(mbeanServerConn, oName, JmxReporter.HistogramMBean.class);
			default:
				throw new RuntimeException("Unknown column family metric.");
			}
		} catch (MalformedObjectNameException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Retrieve Proxy metrics
	 * 
	 * @param metricName
	 *            CompletedTasks, PendingTasks, BytesCompacted or
	 *            TotalCompactionsCompleted.
	 */
	public Object getCompactionMetric(String metricName) {
		try {
			switch (metricName) {
			case "BytesCompacted":
				return JMX.newMBeanProxy(mbeanServerConn,
						new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=" + metricName),
						JmxReporter.CounterMBean.class).getCount();
			case "CompletedTasks":
			case "PendingTasks":
				return JMX.newMBeanProxy(mbeanServerConn,
						new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=" + metricName),
						JmxReporter.GaugeMBean.class).getValue();
			case "TotalCompactionsCompleted":
				return JMX.newMBeanProxy(mbeanServerConn,
						new ObjectName("org.apache.cassandra.metrics:type=Compaction,name=" + metricName),
						JmxReporter.MeterMBean.class).getCount();
			default:
				throw new RuntimeException("Unknown compaction metric.");
			}
		} catch (MalformedObjectNameException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Retrieve Proxy metrics
	 * 
	 * @param metricName
	 *            Exceptions, Load, TotalHints or TotalHintsInProgress.
	 */
	public long getStorageMetric(String metricName) {
		try {
			return JMX.newMBeanProxy(mbeanServerConn,
					new ObjectName("org.apache.cassandra.metrics:type=Storage,name=" + metricName),
					JmxReporter.CounterMBean.class).getCount();
		} catch (MalformedObjectNameException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Retrieve OperatingSystem metrics
	 * @param metricName
	 * 			ProcessCpuLoad, AvailableProcessors
	 * @return
	 */
	public Object getOperatingSystemMetric(String metricName) {
		try {
			return mbeanServerConn.getAttribute(new ObjectName("java.lang:type=OperatingSystem"), metricName);
		} catch (MalformedObjectNameException e) {
			throw new RuntimeException(e);
		} catch (AttributeNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstanceNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MBeanException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ReflectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return null;
	}
}