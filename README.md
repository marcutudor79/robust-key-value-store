# robust-key-value-store

This is a project in which a multi-actor key-value store system is implemented. It is based on the AKKA frawework.

# Getting started
Please clone this repository taking into account the submodules:
```
git clone <url> --recurse-submodules
```

# Prerequisites (Ubuntu 24.04 LTS)
- Java >= 21
```
    sudo apt install openjdk-21-jdk
```
- Apache Maven >= 3.9
```
    sudo apt install maven=3.8.7-2
```
- curl
```
    sudo apt install curl
```
- Akka cli
``` bash
    curl -sL https://doc.akka.io/install-cli.sh | bash
```

Then follow the instrunctions from https://doc.akka.io/getting-started/starthere.html to see if a starting sample builds and runs.

# Akka resumed
Akka actors - higher level of abstraction for writing concurrent distributed systems. It alleviates the developer from having to deal with explicit locking and thread management.

1. How to create an actor
---

# Contributors

- Elena Barau [@lena0097](https://github.com/lena0097)
- Daniele Fama [@DanieleFamà](https://github.com/danielefam)
- Aymen Kabil [@AymenKabil]()
- and me