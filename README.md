# RAID-5 Storage Area Network 

## Problem and Goal 

With the rapid increase in data over the past two decades, effective and dependable data storage systems have become imperative. Our project, titled “RAID-5 Storage Area Network” (RAID-5 SAN), aims to address this need by emulating a distributed, fault-tolerant storage solution on a Python-based UNIX file system. This system utilizes a client/server model with Remote Procedure Calls (RPC) to facilitate network interaction, promoting the server as a dependable and efficient storage system.

The primary challenges addressed by our project involve uneven load distribution and fault tolerance, which are common issues in traditional storage systems. These challenges significantly impact performance and reliability. We specifically targeted two types of failures: soft failures, where data degradation occurs over time, and hard failures, where complete server shutdown takes place.

To confront these challenges, our system embraces a RAID-5 configuration, which utilizes checksums and distributed parity to enable data recovery from corruption and provide repair capabilities upon disk replacement or failure. The goal of the RAID-5 SAN project was to create a robust storage solution that ensures high reliability, efficient data distribution, and fault tolerance, thereby enhancing overall system performance and dependability in handling large volumes of data.
