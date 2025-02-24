# Amazon ElastiCache Cluster Client

[![Build Status](https://travis-ci.org/awslabs/aws-elasticache-cluster-client-memcached-for-java.svg?branch=master)](https://travis-ci.org/awslabs/aws-elasticache-cluster-client-memcached-for-java)

Amazon ElastiCache Cluster Client is an enhanced Java library to connect to ElastiCache clusters. This client library has been built upon Spymemcached and is released under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0).

# Building

Amazon ElastiCache Cluster Client can be compiled using Apache Ant by running the following
command:

    ant

This will generate binary, source, and javadoc jars in the build
directory of the project.

# Configuring the client in TLS mode

As memcached supports TLS since version 1.5.13, Amazon ElastiCache Cluster Client added TLS support for better security.

In order to create a client in TLS mode, do the following to initialize the client with the appropriate SSLContext:

    import java.security.cert.CertificateFactory;
    import java.security.KeyStore;
    import javax.net.ssl.SSLContext;
    import javax.net.ssl.TrustManagerFactory;
    import net.spy.memcached.AddrUtil;
    import net.spy.memcached.ConnectionFactoryBuilder;
    import net.spy.memcached.MemcachedClient;
    public class TLSDemo {
        public static void main(String[] args) throws Exception {
            ConnectionFactoryBuilder connectionFactoryBuilder = new ConnectionFactoryBuilder();
            // Build SSLContext
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((KeyStore) null);
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            // Create the client in TLS mode
            connectionFactoryBuilder.setSSLContext(sslContext);
            // TLS mode enables hostname verification by default. It is always recommended to do that.
            connectionFactoryBuilder.setHostnameForTlsVerification("my_website.com");
            // To disable hostname verification, do the following:
            // connectionFactoryBuilder.setSkipTlsHostnameVerification(true);
            MemcachedClient client = new MemcachedClient(connectionFactoryBuilder.build(), AddrUtil.getAddresses("my_website.com:11211"));
            // Store a data item
            client.set("theKey", 3600, "This is the data value");
        }
    }

To create the TLS mode client with customized TLS certificate, initialize the SSLContext as follows:

    InputStream inputStream = new FileInputStream("/tmp/my_certificate");
    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    ks.load(null);
    ks.setCertificateEntry("cert", CertificateFactory.getInstance("X.509").generateCertificate(inputStream));
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ks);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, tmf.getTrustManagers(), null);

The rest of the logic to create the client follows.

#### Troubleshooting TLS Connection Issues
##### Verify TLS Configuration Using OpenSSL
Test TLS connection using OpenSSL command: `openssl s_client -connect <host>:<port>`, Check for:
* Successful handshake
* Valid certificate chain
* Certificate expiration dates
* Correct hostname matching
##### Dependency Management
Review project dependencies for conflicts:
* Ensure `elasticache-java-cluster-client` is the primary memcached client
* Remove or exclude conflicting clients (e.g., spymemcached)
* Check Maven/Gradle dependency tree for transitive dependencies
##### Network Connectivity
* Check network connectivity between client and cluster
* Verify DNS resolution for cluster endpoints
* Confirm no firewall rules blocking TLS traffic

# Testing

The latest version of Amazon ElastiCache Cluster Client supports unit tests and integration tests.

## Unit Tests
Unit tests do not require any running memcached servers, and can be run using Apache Ant by the following command:

    ant test

## Integration Tests
Integration tests are always run against local memcached servers. Start integration tests by the
following command:

    ant it

**NOTE**: Integration tests will start servers with ports: 11200, 11201, 11211, 11212, 22211, 22212. The integration tests can not run if one or more of those ports are using.

It has a set of command line arguments that can be used to configure your client mode, your local testing server, your type of testing server, and your certificates directory (needed for TLS mode). The arguments are listed below.

    -Dclient.mode=memcached_client_mode

This argument is used to specify the mode of the client that you want to run. Supported options are _Static_ and _Dynamic_.
_Dynamic_ mode enables Auto Discovery feature. _Static_ mode runs without Auto Discovery and has the same functionality as a classic spymemcached client. By default it is set to _Static_.

    -Dserver.bin=local_binary_of_testing_server

This argument is used to specify the location of your testing
server binary. By default it is set to _/usr/bin/memcached_.

    -Dserver.type=type_of_testing_server

This argument is used to specify the type of your testing server. Supported options are _oss_ and _elasticache_. By default it is set to _oss_.

## Additional argument for running integration tests with TLS mode

    -Dcert.folder=certificates_folder_of_testing_server

This argument is used to specify the folder of the 2 certificates for starting memcached server with TLS enabled. Named those 2 certificates as _private.cert_ and _public.cert_. This is mandatory if you want to run integration tests with TLS mode.
Besides, your testing server should be built with TLS capability. See instruction: https://github.com/memcached/memcached/wiki/TLS

# More Information for Amazon ElastiCache Cluster Client
Github link: https://github.com/amazonwebservices/aws-elasticache-cluster-client-memcached-for-java.
This repository is a fork of the spymemcached Java client for connecting to memcached (specifically the https://github.com/dustin/java-memcached-client repo).

Additional changes have been made to support Amazon ElastiCache Auto Discovery. To read more about Auto Discovery, please go here: https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.html.

For more information about Spymemcached see the link below:

[Spymemcached Project Home](http://code.google.com/p/spymemcached/)
contains a wiki, issue tracker, and downloads section.
