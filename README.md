# Workload Extrapolation Simulator

The simulator is part of our research project of
**Evaluating Scalability Bottlenecks by Workload Extrapolation**.
We record a node's workloads at small
scales and extrapolate such workload at a large scale. And we build simulator
to play the extrapolated workloads to the target node and measure its performance.
For design of simulator, please refer to our **MASCOTS'18 paper** [1] for details.

With the help of PatternMiner tool (refer to [1] for details), we are able to
extract workload patterns, which are then played by simulator to emulate large
scale experiments and evaluate target node. Our simulator are able to run a
variety of benchmarks and can be easily extended to support new benchmarks
and applications.

## Getting Started

These instructions will get you a copy of the project up and running on
your local machine for development and testing purposes. See deployment
for notes on how to deploy the project on a live system.

### Prerequisites

Java SDK 1.8+ is required.

### Installing and running the tests

Please refer to doc/usage.md for instructions

## Built With

* [Hadoop](https://archive.apache.org/dist/hadoop/common/hadoop-2.7.3/) - The target framework used
* [Maven](https://maven.apache.org/) - Dependency Management

## Authors

* **Yifan Gan** - [SayuRanger](https://github.com/SayuRanger)
* **Rong Shi** - [vdr007](https://github.com/vdr007/)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* todo

---

### Reference

[1] Rong Shi, Yifan Gan, and Yang Wang,
Evaluating Scalability Bottlenecks by Workload Extrapolation, 
26th IEEE International Symposium on the Modeling, Analysis, and Simulation of Computer and Telecommunication Systems (MASCOTS) 2018


