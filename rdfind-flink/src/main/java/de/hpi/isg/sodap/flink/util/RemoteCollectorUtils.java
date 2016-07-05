package de.hpi.isg.sodap.flink.util;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.io.RemoteCollector;
import org.apache.flink.api.java.io.RemoteCollectorConsumer;
import org.apache.flink.api.java.io.RemoteCollectorImpl;
import org.apache.flink.api.java.io.RemoteCollectorOutputFormat;

import java.io.Serializable;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Enumeration;
import java.util.UUID;

/**
 * Offers convenience functions for create {@link RemoteCollectorUtils}.
 *
 * @author Sebastian Kruse
 * @since 04.02.2015.
 */
public class RemoteCollectorUtils {

    private RemoteCollectorUtils() { }

    /**
     * Configures a RemoteCollectorOutputFormat
     * @param consumer accepts collected data
     * @param <T> is the type of elements to be collected
     * @return an output format that will send its data to the consumer
     */
    public static <T> OutputFormat<T> create(RemoteCollectorConsumer<T> consumer) {
        RmiDescriptor rmiDescriptor = createCollector(consumer);

        // create and configure the output format
        OutputFormat<T> remoteCollectorOutputFormat = new RemoteCollectorOutputFormat<T>(
                rmiDescriptor.getIp(), rmiDescriptor.getPort(), rmiDescriptor.getRmiId());

        return remoteCollectorOutputFormat;
    }

    /**
     * Creates an RMI-based collector locally.
     * @param consumer is the object that will receive the collected data
     * @param <T> is the type of the elements to be collected
     * @return a description of the collector so as to use in output formats etc.
     */
    public static <T> RmiDescriptor createCollector(RemoteCollectorConsumer<T> consumer) {
        // if the RMI parameter was not set by the user make a "good guess"
        String ip = System.getProperty("java.rmi.server.hostname");
        if (ip == null) {
            Enumeration<NetworkInterface> networkInterfaces = null;
            try {
                networkInterfaces = NetworkInterface.getNetworkInterfaces();
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = (NetworkInterface) networkInterfaces
                        .nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface
                        .getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = (InetAddress) inetAddresses
                            .nextElement();
                    if (!inetAddress.isLoopbackAddress()
                            && inetAddress instanceof Inet4Address) {
                        ip = inetAddress.getHostAddress();
                        System.setProperty("java.rmi.server.hostname", ip);
                    }
                }
            }
        }

        // get some random free port
        Integer randomPort = 0;
        try {
            ServerSocket tmp = new ServerSocket(0);
            randomPort = tmp.getLocalPort();
            tmp.close();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        // create an ID for this output format instance
        String rmiId = String.format("%s-%s", RemoteCollectorOutputFormat.class.getName(), UUID.randomUUID());

        RmiDescriptor rmiDescriptor = new RmiDescriptor(ip, randomPort, rmiId);

        // create the local listening object and bind it to the RMI registry
        RemoteCollectorImpl.createAndBind(rmiDescriptor.getPort(), consumer, rmiDescriptor.getRmiId());
        return rmiDescriptor;
    }

    public static <T> RemoteCollector<T> connectTo(RmiDescriptor rmiDescriptor) {
        Registry registry;

        // get the remote's RMI Registry
        try {
            registry = LocateRegistry.getRegistry(rmiDescriptor.getIp(), rmiDescriptor.getPort());
        } catch (RemoteException e) {
            throw new IllegalStateException(e);
        }

        // try to get an intance of an IRemoteCollector implementation
        try {
            RemoteCollector<T> remoteCollector = (RemoteCollector<T>) registry.lookup(rmiDescriptor.getRmiId());
            return remoteCollector;
        } catch (AccessException e) {
            throw new IllegalStateException(e);
        } catch (RemoteException e) {
            throw new IllegalStateException(e);
        } catch (NotBoundException e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("serial")
    public static class RmiDescriptor implements Serializable {

        private String ip;

        private int port;

        private String rmiId;

        public RmiDescriptor() {
        }

        public RmiDescriptor(String ip, int port, String rmiId) {

            this.ip = ip;
            this.port = port;
            this.rmiId = rmiId;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getRmiId() {
            return rmiId;
        }

        public void setRmiId(String rmiId) {
            this.rmiId = rmiId;
        }
    }
}
