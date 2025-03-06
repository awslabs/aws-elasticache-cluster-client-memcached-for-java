/**
 * Copyright (C) 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved. 
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package net.spy.memcached.config;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * NodeEndPoint captures the information needed to connect to a cache node.
 * Both hostName and ipAddress to capture the latest information instead of relying entirely on the DNS servers.
 *
 */
public class NodeEndPoint {
  private final String hostName;
  private final String ipAddress;
  private final int port;

  /**
   * Immutable instance with a hostname, ip address, and port.
   * 
   * @param hostName
   *            hostname (ex. foo.bar.com)
   * @param ipAddress
   *            string form of IP address (ex. 10.10.1.1). It is expected that
   *            this IP address is valid.
   * @param port
   *            port (ex. 11211)
   */
  public NodeEndPoint(final String hostName, final String ipAddress, final int port) {
      this.hostName = hostName != null ? hostName.trim() : null;
      this.ipAddress = ipAddress != null ? ipAddress.trim() : null;
      this.port = port;
  }

  /**
   * Immutable instance, without an IP address.
   * 
   * @param hostName
   *            hostname (ex. foo.bar.com).
   * @param port
   *            port (ex. 11211)
   */
  public NodeEndPoint(final String hostName, final int port) {
    this(hostName, null, port);
  }

  /**
   * @return hostname (ex. foo.bar.com)
   */
  public String getHostName() {
      return hostName;
  }

  /**
   * @return IP address, in textual form (ex. 10.1.10.1)
   */
  public String getIpAddress() {
      return ipAddress;
  }

  /**
   * @return port (ex. 11211)
   */
  public int getPort() {
      return port;
  }

  public InetSocketAddress getInetSocketAddress(){
	  return getInetSocketAddress(false);
  }
  
  public InetSocketAddress getInetSocketAddress(boolean reresolve){
    if(reresolve || ipAddress == null || ipAddress.trim().equals("")){
      return new InetSocketAddress(hostName, port);
    } else {
      try {
        return new InetSocketAddress(InetAddress.getByName(ipAddress) , port);
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Invalid server with hostName:" + hostName + ", IpAddress:" + ipAddress);
      }
    }
  }
  
  /**
   * copy one object to another.
   */
  public static NodeEndPoint copy(final NodeEndPoint other) {
      return new NodeEndPoint(other.getHostName(), other.getIpAddress(), other.getPort());
  }
  
  @Override
  public String toString(){
    return "NodeEndPoint - " + "HostName:" + hostName +
        " IpAddress:" + ipAddress +
        " Port:" + port;
  }

}
