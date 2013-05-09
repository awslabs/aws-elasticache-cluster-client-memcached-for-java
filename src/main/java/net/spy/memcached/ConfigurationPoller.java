/**
 * Copyright (C) 2012-2012 Amazon.com, Inc. or its affiliates. All Rights Reserved. 
 *
 * Licensed under the Amazon Software License (the "License"). You may not use this 
 * file except in compliance with the License. A copy of the License is located at
 *  http://aws.amazon.com/asl/
 * or in the "license" file accompanying this file. This file is distributed on 
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or
 * implied. See the License for the specific language governing permissions and 
 * limitations under the License. 
 */
package net.spy.memcached;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.compat.SpyThread;
import net.spy.memcached.config.ClusterConfiguration;
import net.spy.memcached.config.ClusterConfigurationObserver;
import net.spy.memcached.config.NodeEndPoint;
import net.spy.memcached.ops.ConfigurationType;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

/**
 * A periodic poller to fetch configuration information from the server. This also acts as
 * the publisher when there is change in the configuration.
 *
 */
public class ConfigurationPoller extends SpyThread{

  //5 second initial delay before starting poller.
  private static final long INITIAL_DELAY = 5000l;
  // 1 minute polling interval
  public static final long DEFAULT_POLL_INTERVAL = 60000l;
  private static final int MAX_RETRY_ATTEMPT = 3;
  
  //500 ms interval between retries;
  private static final long RETRY_INTERVAL = 500l;
  private final MemcachedClient client;
  private List<ClusterConfigurationObserver> clusterConfigObservers = new ArrayList<ClusterConfigurationObserver>();
  private String currentClusterConfigResponse;
  private ClusterConfiguration currentClusterConfiguration;
  //The transcoder used for config.
  private Transcoder<Object> configTranscoder = new SerializingTranscoder();
  private int currentIndex = 0;
  private Date date = new Date();
  private long lastSuccessfulPoll = date.getTime(); 
  private int pollingErrorCount = 0;
  
  //The executor is used to keep the task and it's execution independent. The scheduled thread polls takes care of 
  //the periodic polling.
  private ScheduledThreadPoolExecutor scheduledExecutor;
  
  public ConfigurationPoller(final MemcachedClient client){
    this(client, DEFAULT_POLL_INTERVAL, false);
  }
  
  public ConfigurationPoller(final MemcachedClient client, long pollingInterval, final boolean useDaemonThreads){
    this.client = client;
    this.scheduledExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
		@Override
		public Thread newThread(Runnable runnable) {
			Thread thread = Executors.defaultThreadFactory().newThread(runnable);
			thread.setDaemon(useDaemonThreads);
			return thread;
		}
	});
    setDaemon(useDaemonThreads);
    
    //The explicit typed emptyList assignment avoids type warning.
    List<NodeEndPoint> emptyList = Collections.emptyList();
    this.currentClusterConfiguration = new ClusterConfiguration(-1, emptyList);
    
    //Keep the initial delay to few seconds to begin polling quickly. This is useful if the config API call timed out in Client constructor 
    //and did not initialize the set of nodes in the cluster. The poller takes over the responsibility of configuring the client with the 
    //nodes in the cluster.  
    scheduledExecutor.scheduleAtFixedRate(this, INITIAL_DELAY, pollingInterval, TimeUnit.MILLISECONDS);
  }
  
  public void subscribeForClusterConfiguration(ClusterConfigurationObserver observer){
    clusterConfigObservers.add(observer);
  }
  
  @Override
  public void run(){
    try{
      getLogger().info("Starting configuration poller.");
      String newConfigResponse = null;
      NodeEndPoint endpointToGetConfig = null;
    
      Collection<NodeEndPoint> endpoints = client.getAvailableNodeEndPoints();
      if(endpoints.isEmpty()){
        //If no nodes are available status, then get all the endpoints. This provides an 
        //oppurtunity to re-resolve the hostname by recreating InetSocketAddress instance in "NodeEndPoint".getInetSocketAddress().
        endpoints = client.getAllNodeEndPoints();
      }
      currentIndex = (currentIndex+1)%endpoints.size();
      Iterator<NodeEndPoint> iterator = endpoints.iterator();
      for(int i =0;i<currentIndex;i++){
        iterator.next();
      }
      
      endpointToGetConfig = iterator.next();
      InetSocketAddress socketAddressToGetConfig = endpointToGetConfig.getInetSocketAddress();
      
      getLogger().info("Endpoint to use for configuration access in this poll " + endpointToGetConfig.toString());
      
      int retryCount = 0;
      
      //If client is not initialized for the first time with the list of cache nodes, keep retrying till the call succeeds.
      //To avoid execessive calls, there is a small retry interval between the retry attempts.
      while(retryCount < MAX_RETRY_ATTEMPT || !client.isConfigurationInitialized()){
        try{
          if(client.isConfigurationProtocolSupported()){
            try{
              newConfigResponse = (String)client.getConfig(socketAddressToGetConfig, 
                                                                                         ConfigurationType.CLUSTER,
                                                                                         configTranscoder);
              if(newConfigResponse == null || newConfigResponse.trim().isEmpty()){
                newConfigResponse = (String)client.get(socketAddressToGetConfig, ConfigurationType.CLUSTER.getValueWithNameSpace(), configTranscoder);
                if(newConfigResponse != null && ! newConfigResponse.trim().isEmpty()){
                  client.setIsConfigurationProtocolSupported(false);
                }
              }

            }catch(OperationNotSupportedException e){
              //Fallback to key based config access.
              client.setIsConfigurationProtocolSupported(false);
              continue;
            }
          } else {
            newConfigResponse = (String)client.get(socketAddressToGetConfig, 
                                                                             ConfigurationType.CLUSTER.getValueWithNameSpace(), 
                                                                             configTranscoder);
          }
          
          //Operation succeeded and break out of the loop.
          break;
        }catch(OperationTimeoutException e){
          retryCount++;
          try{
            Thread.sleep(RETRY_INTERVAL);
          }catch (InterruptedException ex) {
            getLogger().warn("Poller thread interrupted during the retry interval for config call. Continue with retry.", ex);
          }
          if(retryCount >= MAX_RETRY_ATTEMPT && client.isConfigurationInitialized()) { 
            getLogger().warn("Max retry attempt reached for config call. Stopping the current poll cycle.", e);
            return;
          }else if(retryCount == MAX_RETRY_ATTEMPT - 1){
            //Fall back to config endpoint
            socketAddressToGetConfig = client.getConfigurationNode().getInetSocketAddress();
          }else {
            //Reresolve on retry attempt
            socketAddressToGetConfig = endpointToGetConfig.getInetSocketAddress(true);
          }
        }
      }
      
      if(newConfigResponse == null){
        getLogger().warn("The configuration is null in the server " + endpointToGetConfig.getHostName());
        trackPollingError();
        return;
      }
      
      getLogger().debug("Retrieved configuration value:" + newConfigResponse);
      
      if(newConfigResponse != null && !newConfigResponse.equals(currentClusterConfigResponse)){
        ClusterConfiguration newClusterConfiguration = AddrUtil.parseClusterTypeConfiguration(newConfigResponse);
        getLogger().warn("Change in configuration - Existing configuration: " + currentClusterConfiguration + "\n New configuration:" +  newClusterConfiguration);
        if(newClusterConfiguration.getConfigVersion() > currentClusterConfiguration.getConfigVersion()){
          currentClusterConfigResponse = newConfigResponse;
          currentClusterConfiguration = newClusterConfiguration;
          for(ClusterConfigurationObserver observer : clusterConfigObservers){
            getLogger().info("Notifying observers about configuration change.");
            observer.notifyUpdate(newClusterConfiguration);
          }
          if(!client.isConfigurationInitialized()){
            client.setIsConfigurtionInitialized(true);
          }
        } else if(newClusterConfiguration.getConfigVersion() < currentClusterConfiguration.getConfigVersion()){
          getLogger().info("Ignoring stale configuration - Existing configuration: " + currentClusterConfigResponse + "\n Stale configuration:" +  newConfigResponse);
          trackPollingError();
          return;
        }
      }
      
      pollingErrorCount = 0;
      lastSuccessfulPoll = date.getTime();
    }catch(Exception e){
      getLogger().error("Error encountered in the poller. Current cluster configuration: " + currentClusterConfigResponse, e);
      trackPollingError();
    }
  }
  
  private void trackPollingError(){
    pollingErrorCount++;
    getLogger().warn("Number of consecutive poller errors is " + Long.toString(pollingErrorCount) + 
        ". Number of minutes since the last successful polling is " + Long.toString(date.getTime() - lastSuccessfulPoll));
  }
  
  public void shutdown(){
    scheduledExecutor.shutdownNow();
  }
  
}
