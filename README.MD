# Oak processionary moth data

## Goals
- Extract data 
- Reproducability
    - A new connected device must be able to get exactly the same data as I'm working with.
        - Show every transformation made.

### Reproducacility
- Keep libraries, python version fixed

## Architecture
![Data architecture](./static/images/data_architecture.jpg "Data architecture")

## Justifications
### Why google cloud?
- Personal data
- Single source
    - No need for daily updating of data.
    - Some data sources are one-time delivery only.
    - Only one interface needed to build.

### Why docker-compose?
- Seemed cool
    - One could go for portability reasons

### Why Python library Pathlib
- Cross platform compatibility
    - Developing on Windows + Ubuntu and deploying on Alpine

## Data sources
- NDFF -> one time dump
- Bomenregister -> one time dump



