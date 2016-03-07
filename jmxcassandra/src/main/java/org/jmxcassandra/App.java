package org.jmxcassandra;

import com.google.common.base.Throwables;
import com.yammer.metrics.reporting.JmxReporter;

import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.EMPTY;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.io.util.FileUtils;
import org.jmxcassandra.JmxConnect;

public class App {

	public static void main(String... args) {

		@SuppressWarnings("unchecked")
		List<Class<? extends Runnable>> commands = newArrayList(
				Help.class, 
				Table.class, 
				Clients.class,
				OSMetrics.class,
				CompactionStats.class);

		Cli<Runnable> parser = Cli.<Runnable> builder("cassmon")
				.withDescription("Get metrics of Cassandra Process Remotely")
				.withDefaultCommand(Help.class)
				.withCommands(commands)
				.build();

		int status = 0;
		try {
			Runnable parse = parser.parse(args);
			parse.run();
		} catch (Exception e) {
			badUse(e);
			status = 1;
		} catch (Throwable throwable) {
			err(Throwables.getRootCause(throwable));
			status = 2;
		}

		System.exit(status);
	}

	private static void badUse(Exception e) {
		System.out.println("cassmon: " + e.getMessage());
		System.out.println("See 'cassmon help' or 'cassmon help <command>'.");
	}

	private static void err(Throwable e) {
		System.err.println("error: " + e.getMessage());
		System.err.println("-- StackTrace --");
		System.err.println(getStackTraceAsString(e));
	}

	public static abstract class CassMonCmd implements Runnable {

		@Option(type = OptionType.GLOBAL, name = { "-h", "--host" }, description = "Node hostname or ip address")
		private String host = "127.0.0.1";

		@Option(type = OptionType.GLOBAL, name = { "-p", "--port" }, description = "Remote jmx agent port number")
		private String port = "7199";

		@Option(type = OptionType.GLOBAL, name = { "-u", "--username" }, description = "Remote jmx agent username")
		private String username = EMPTY;

		@Option(type = OptionType.GLOBAL, name = { "-pw", "--password" }, description = "Remote jmx agent password")
		private String password = EMPTY;

		@Override
		public void run() {

			JmxConnect jmxConnect = connect();
			execute(jmxConnect);
			if (jmxConnect.isFailed())
				throw new RuntimeException("cassmon failed, check server logs");

		}

		protected abstract void execute(JmxConnect jmxConnect);

		private JmxConnect connect() {
			JmxConnect nodeClient = null;

			try {
				if (username.isEmpty())
					nodeClient = new JmxConnect(host, parseInt(port));
				else
					nodeClient = new JmxConnect(host, parseInt(port), username, password);
			} catch (IOException e) {
				Throwable rootCause = Throwables.getRootCause(e);
				System.err.println(format("cassmon: Failed to connect to '%s:%s' - %s: '%s'.", host, port,
						rootCause.getClass().getSimpleName(), rootCause.getMessage()));
				System.exit(1);
			}

			return nodeClient;
		}

		protected String[] parseOptionalColumnFamilies(List<String> cmdArgs) {
			return cmdArgs.size() <= 1 ? EMPTY_STRING_ARRAY : toArray(cmdArgs.subList(1, cmdArgs.size()), String.class);
		}

	}

	@Command(name = "tablestats", description = "Print information about the table in cassandra")
	public static class Table extends CassMonCmd {

		@Option(type = OptionType.COMMAND, name = { "-ks", "--keyspace" }, description = "Keyspace")
		private String keyspace = "";
		
		@Option(type = OptionType.COMMAND, name = { "-t", "--table" }, description = "Table")
		private String cfname = "";
		
		@Option(name = {"-d", "--diskused"}, description = "Live Disk Space Used")
		private boolean liveDiskSpaceUsed = false;
		
		@Option(name = {"-r", "--readlatency"}, description = "Read Latency")
		private boolean readLatency = false;
		
		@Option(name = {"-s", "--sstablecount"}, description = "Sstable Count")
		private boolean sstableCount = false;
		
		@Override
		public void execute(JmxConnect jmxConnect) {

			if(liveDiskSpaceUsed)
				System.out.println("Disk Usage: " + format((Long) jmxConnect.getColumnFamilyMetric(keyspace, cfname, "LiveDiskSpaceUsed"), true));
			
			if(readLatency) {
				double cfReadLatency = ((JmxReporter.TimerMBean) jmxConnect.getColumnFamilyMetric(keyspace, cfname, "ReadLatency")).getCount();
				cfReadLatency = cfReadLatency > 0 ? cfReadLatency / 1000 : Double.NaN;
				System.out.println("Read Latency: " + cfReadLatency + " ms");
			}
			
			if(sstableCount)
				System.out.println("SSTABLE Count: " + jmxConnect.getColumnFamilyMetric(keyspace, cfname, "LiveSSTableCount"));
		}

		private static String format(long bytes, boolean humanReadable) {
			return humanReadable ? FileUtils.stringifyFileSize(bytes) : Long.toString(bytes);
		}
	}

	@Command(name = "clients", description = "Prints information about number of clients connected to cassandra")
	public static class Clients extends CassMonCmd {

		@Option(name = {"-n", "--native"}, description = "Native Client Connections")
		private boolean nativeClients = false;
		
		@Option(name = {"-t", "--thrift"}, description = "Thrift Client Connections")
		private boolean thriftClients = false;
		
		@Option(name = {"-a", "--all"}, description = "All Client Connections")
		private boolean allClients = false;

		@Override
		protected void execute(JmxConnect jmxConnect) {

			if(nativeClients || allClients)
				System.out.println("Thrift Clients: " + jmxConnect.getConnectedClients("connectedNativeClients"));
			
			if(thriftClients || allClients)
					System.out.println("Natvie Client: " + jmxConnect.getConnectedClients("connectedThriftClients"));
		}
	}
	
	@Command(name = "compactionstats", description = "Prints information about the compactions")
	public static class CompactionStats extends CassMonCmd {

		@Option(name = {"-b", "--bytescompacted"}, description = "Bytes compacted")
		private boolean bytesCompacted = false;
		
		@Option(name = {"-c", "--completedtasks"}, description = "Completed compaction tasks")
		private boolean completedTasks = false;
		
		@Option(name = {"-p", "--pendingtasks"}, description = "Pending compaction Tasks")
		private boolean pendingTasks = false;

		@Option(name = {"-t", "--totalcompactionscompleted"}, description = "Total Compactions Completed")
		private boolean totalCompactionsCompleted = false;
		
		@Override
		protected void execute(JmxConnect jmxConnect) {

			if(bytesCompacted)
				System.out.println("Bytes Compacted: " + jmxConnect.getCompactionMetric("BytesCompacted"));
			
			if(completedTasks)
					System.out.println("Completed Tasks: " + jmxConnect.getCompactionMetric("CompletedTasks"));
			
			if(pendingTasks)
				System.out.println("Pending Tasks: " + jmxConnect.getCompactionMetric("PendingTasks"));
			
			if(totalCompactionsCompleted)
				System.out.println("Total Compactions Completed: " + jmxConnect.getCompactionMetric("TotalCompactionsCompleted"));
		}
	}
	
	@Command(name = "os", description = "Prints information about Operating System metrics")
	public static class OSMetrics extends CassMonCmd {

		@Option(name = {"-c", "--cpuload"}, description = "Get CPU load")
		private boolean cpuload = false;
		
		@Option(name = {"-s", "--sysload"}, description = "Get SYSTEM load")
		private boolean sysload = false;
		
		@Option(name = {"-p", "--processors"}, description = "Get number of processors")
		private boolean processors = false;
		
		@Option(name = {"-a", "--arch"}, description = "Get architecture of OS")
		private boolean arch = false;
	
		@Option(name = {"-l", "--sysavgload"}, description = "System Average Load")
		private boolean sysavgload = false;
		
		@Option(name = {"-v", "--version"}, description = "System version")
		private boolean version = false;
		
		@Option(name = {"-n", "--name"}, description = "OS name")
		private boolean name = false;
		
		@Option(name = {"-t", "--processcputime"}, description = "Process cpu time")
		private boolean processcputime = false;
		
		@Option(name = {"-m", "--memory"}, description = "Used memory/Total memory")
		private boolean memory = false;
		
		@Option(name = {"-f", "--filedescriptor"}, description = "Open/Max File descriptors")
		private boolean filedescriptor = false;
		
		@Override
		protected void execute(JmxConnect jmxConnect) {
			if (cpuload) {
				System.out.println("Cpu Load: " + jmxConnect.getOperatingSystemMetric("ProcessCpuLoad"));
			}
			
			if (sysload) {
				System.out.println("System Load: " + jmxConnect.getOperatingSystemMetric("SystemCpuLoad"));
			}
			
			if(processors) {
				System.out.println("Processors: " + jmxConnect.getOperatingSystemMetric("AvailableProcessors"));
			}
			
			if(arch) {
				System.out.println("OS Architechture: " + jmxConnect.getOperatingSystemMetric("Arch"));
			}
			
			if(sysavgload) {
				System.out.println("System Avg. Load: " + jmxConnect.getOperatingSystemMetric("SystemLoadAverage"));
			}
			
			if(version) {
				System.out.println("OS Version: " + jmxConnect.getOperatingSystemMetric("Version"));
			}
			
			if(name) {
				System.out.println("OS Name: " + jmxConnect.getOperatingSystemMetric("Name"));
			}
			
			if(processcputime) {
				long processTime = (long) jmxConnect.getOperatingSystemMetric("ProcessCpuTime");
				processTime = processTime > 0 ? processTime / 1000000 : Long.MIN_VALUE;
				System.out.println("Process cpu time: " + processTime + " ms");
			}
			
			if(memory) {
				System.out.println("System memory(Free/Total): " + format((long) jmxConnect.getOperatingSystemMetric("FreePhysicalMemorySize"), true) + "/" 
									+ format((long) jmxConnect.getOperatingSystemMetric("TotalPhysicalMemorySize"), true));
				
				System.out.println("Swap Memory(Free/Total): " + format((long) jmxConnect.getOperatingSystemMetric("FreeSwapSpaceSize"), true) + "/" 
				+ format((long) jmxConnect.getOperatingSystemMetric("TotalSwapSpaceSize"), true));
			}
			
			if(filedescriptor) {
				System.out.println("File Descriptors(Open/Max): " + jmxConnect.getOperatingSystemMetric("MaxFileDescriptorCount") + "/" 
						+ jmxConnect.getOperatingSystemMetric("OpenFileDescriptorCount"));
			}
		}
		
		private static String format(long bytes, boolean humanReadable) {
			return humanReadable ? FileUtils.stringifyFileSize(bytes) : Long.toString(bytes);
		}
	}
}