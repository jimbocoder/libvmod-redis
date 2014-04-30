vmod_redis [![Build Status](https://travis-ci.org/brandonwamboldt/libvmod-redis.png?branch=master)](https://travis-ci.org/brandonwamboldt/libvmod-redis)
==========

A Varnish module that allows sending commands to redis from your VCL, as well as retrieving data.

Usage
-----

In your VCL file, include the following line to load the redis module.

```
import redis;
```

Functions
---------

### redis.init_redis

```
redis.init_redis(host, port, timeout_ms)
```

Use the redis server at the given `host` and `port` with a timeout of `timeout_ms` milliseconds. If `port` is less than or equal to zero, the default port of 6379 is used. If `timeout_ms` is less than or equal to zero, a default timeout of `200ms` is used.

This function is intended to be called from the Varnish subroutine `vcl_init`. If the call is left out, the module will attempt to connect to the Redis server at `127.0.0.1:6379` with a connect timeout of `200ms`.

**Example:**

```
redis.init_redis("localhost", 6379, 200);
```

### redis.send

```
redis.send(command)
```

Sends the given `command` to redis; the response will be ignored.

**Example:**

```
redis.send("LPUSH client " + client.ip);
```

### redis.call

```
redis.call(command)
```

Sends the given `command` to redis; any response will be returned as a string.

**Example:**

```
set req.http.x-redis = redis.call("LTRIM client 0 99");
```

### redis.pipeline

```
redis.pipeline()
```

creates a redis pipeline.  redis.push(cmd) to push commands. then redis.pop() to retrieve

**Example:**

```
redis.pipeline();
redis.push("get your:special:key");
redis.push("incr your:special:counter");
set req.http.special_key = redis.pop();
set req.http.special_counter = redis.pop();
```

Examples
--------

See the [`examples/`](/examples) directory.

Dependencies
------------

* [hiredis](https://github.com/redis/hiredis) - minimalistic C client for Redis (`libhiredis-dev` on Ubuntu)
* automake
* autoconf
* libtool

Installation
------------

#### Step 1. Download Varnish Source

On Ubuntu, this can be easily done using `apt-get source varnish`. However, you can get the source for Varnish on any OS using Git.

```
cd /opt
git clone https://github.com/varnish/Varnish-Cache.git varnish
cd varnish
git checkout tags/varnish-3.0.3
```

**Important:** You must download the same version of the Varnish source code as you have installed, or you'll get an error when loading the vmod. You can get your current version using `varnishd -V`.

#### Step 2. Compile & Install

```
./autogen.sh
./configure VARNISHSRC=/opt/varnish VMODDIR=/usr/lib/varnish/vmods
make
sudo make install
```

Be sure to point `VARNISHSRC` to wherever you downloaded the Varnish source code to.

**Important:** Some systems such as CentOS 64-bit store vmods in `/usr/lib64/varnish/vmods` not `/usr/lib/varnish/vmods`, so change that if needed.

Troubleshooting
---------------

#### Could not load module redis

If you get an error like this when starting Varnish:

```
Message from VCC-compiler:
Could not load module redis
/usr/lib64/varnish/vmods/libvmod_redis.so
ABI mismatch, expected <Varnish 3.0.4 9f83e8f>, got <Varnish 3.0.5 1a89b1f>
('input' Line 7 Pos 8)
```

You compiled `libvmod-redis` against the wrong version of the Varnish source code.
