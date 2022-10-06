# espo2redrose
Connector between EspoCRM and RedRose.

Developed in support to the [Ukraine crisis 2022](https://go.ifrc.org/emergencies/5854).

## Description

Synopsis: a [dockerized](https://www.docker.com/) [python app](https://www.python.org/) that connects EspoCRM and RedRose.

Worflow: push new entities from EspoCRM to RedRose.

## WARNING
Lots of configurations specific to the Slovakia Shelter program are currently hard-coded in the app, needs heavy refactoring to be generalizable.

## Setup
Generic requirements:
- a running EspoCRM instance
- a running RedRose instance
- the mapping of fields from EspoCRM to RedRose (as .csv uder `data/`)
- all necessary credentials (as .env under `credentials/`)

### with Docker
1. Install [Docker](https://www.docker.com/get-started)
2. Build the docker image from the root directory
```
docker build -t rodekruis/espo2redrose .
```
3. Run the docker image in a new container and access it
```
docker run -it --entrypoint /bin/bash rodekruis/espo2redrose
```
4. Check that everything is working by running the pipeline (see [Usage](https://github.com/rodekruis/espo2redrose#usage) below)

### Manual Setup
TBI

## Usage
Command:
```
espo2redrose [OPTIONS]
```
Options:
  ```
  --verbose                   print more output
  --help                      show this message and exit
  ```
