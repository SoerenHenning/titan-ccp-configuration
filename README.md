# Titan CCP - Sensor Management

The [Titan Control Center](https://doi.org/10.1016/j.simpa.2020.100050)
is a scalable and extensible analytics platform for [Industrial DevOps](https://industrial-devops.org/en).
It analyzes and visualizes data streams from Internet of Things (IIoT) sensors
(e.g. electrical power consumption) in industrial production.

This repository contains the **Sensor Management** (formerly called Configuration) microservice of the Titan CCP.

## Build and Run

We use Gradle as a build tool. In order to build the executables run 
`./gradlew build` on Linux/macOS or `./gradlew.bat build` on Windows. This will
create the file `build/distributions/titanccp-sensormanagement.tar` which contains
start scripts for Linux/macOS and Windows.

This repository also contains a Dockerfile. Run
`docker build -t titan-ccp-sensormanagement .` to create a container from it (after
building it with Gradle).

## Reference

Please cite the Titan Control Center as follows:

> S. Henning, W. Hasselbring, *The Titan Control Center for Industrial DevOps analytics research*, Software Impacts 7 (2021), DOI: [10.1016/j.simpa.2020.100050](https://doi.org/10.1016/j.simpa.2020.100050).

BibTeX:

```bibtex
@article{Henning2021,
    title = {The Titan Control Center for Industrial DevOps analytics research},
    journal = {Software Impacts},
    volume = {7},
    pages = {100050},
    year = {2021},
    doi = {10.1016/j.simpa.2020.100050},
    author = {Sören Henning and Wilhelm Hasselbring},
}
```
