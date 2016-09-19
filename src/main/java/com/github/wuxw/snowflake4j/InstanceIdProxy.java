package com.github.wuxw.snowflake4j;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * Created by wuxinw on 2016/9/19.
 */
public class InstanceIdProxy {
    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceIdProxy.class);

    private static final Long LOCAL_IP = ipToLong(getNetworkAddress());
    /**
     * A default value for current instanceID , last 10 bits of local ip .
     */
    private static final Long LOCAL_ID = LOCAL_IP % (1L << (10));
    private static InstanceIdManager instanceIdManager;

    public static long getInstanceId() {
        return instanceIdManager == null ? LOCAL_ID : instanceIdManager.getCurrentId();
    }

    /**
     * none static for spring autowire
     *
     * @param instanceIdManager
     */
    public void setInstanceIdManager(InstanceIdManager instanceIdManager) {
        InstanceIdProxy.instanceIdManager = instanceIdManager;
    }

    private static long ipToLong(String ipAddress) {
        long result = 0;
        String[] ipAddressInArray = ipAddress.split("\\.");
        for (int i = 3; i >= 0; i--) {
            long ip = Long.parseLong(ipAddressInArray[3 - i]);
            //left shifting 24,16,8,0 and bitwise OR
            //1. 192 << 24
            //1. 168 << 16
            //1. 1   << 8
            //1. 2   << 0
            result |= ip << (i * 8);
        }
        return result;
    }

    public static String getNetworkAddress() {
        Enumeration<NetworkInterface> netInterfaces;
        try {
            netInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip;
            while (netInterfaces.hasMoreElements()) {
                NetworkInterface ni = netInterfaces.nextElement();
                Enumeration<InetAddress> addresses = ni.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    ip = addresses.nextElement();
                    if (ip instanceof Inet4Address && !ip.isLoopbackAddress() && !ip.getHostAddress().contains(":")) {
                        return ip.getHostAddress();
                    }
                }
            }
            return "0.0.0.0";
        } catch (SocketException e) {
            LOGGER.error(e.getMessage(), e);
            return "0.0.0.0";
        }
    }
}
