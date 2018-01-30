## **Zeppelin: A Scalable, High-Performance Distributed Key-Value Platform**

[![Build Status](https://travis-ci.org/Qihoo360/zeppelin.svg?branch=master)](https://travis-ci.org/Qihoo360/zeppelin)

Zeppelin is developed and maintained by Qihoo PikaLab and DBA Team, inspired by [Pika](https://github.com/Qihoo360/pika) and [Ceph](https://github.com/ceph/ceph). It's a Distributed Key-Value Platform which aim to providing excellent performance, reliability, and scalability. And based on Zeppelin, we could easily build other services like Table Store, S3 or Redis.


#### **Feature**

- **Excellent Performance**
- **Horizontal Scalability**：a reliable key-value storage service that can scales to many thousands of devices by leveraging the intelligence present in individual storage node.
- **Support upper Protocol**


#### **Build Zeppelin**

Zeppelin based  on some other basic libraries, such as [slash](https://github.com/PikaLabs/slash), [pink](https://github.com/PikaLabs/pink), [floyd](https://github.com/PikaLabs/floyd) and [rocksdb](https://github.com/facebook/rocksdb), so you need to download them first as bellow: 

> git submodule init
>
> git submodule update
>
> make


#### **Usage**

After build and setup, you need to access the service through Zeppeln Client, See more [here](https://github.com/Qihoo360/zeppelin-client)



#### **Performance**

#### Wiki
[Wiki](https://github.com/baotiao/zeppelin/wiki)

[DataServer Design](http://catkang.github.io/2018/01/16/zeppelin-node.html)

[MetaServer Design](http://catkang.github.io/2018/01/19/zeppelin-meta.html)

#### Latest Release
[1.2.4](https://github.com/Qihoo360/zeppelin/releases)
