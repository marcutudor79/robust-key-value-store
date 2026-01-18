# robust-key-value-store

This is a project in which a multi-actor key-value store system is implemented. It is based on the AKKA frawework.

# Getting started
Please clone this repository taking into account the submodules:
```
git clone <url> --recurse-submodules

// takes into consideration the example folder - optional for development
```

# Prerequisites (Ubuntu 24.04 LTS)
- Java Development Kit
```
    sudo apt install openjdk-21-jdk

    // works with Java 8 as well
```
- Apache Maven >= 3.8.7.2
```
    sudo apt install maven=3.8.7-2
```
- Make
```
    sudo apt install make
```
- LaTeX compiler and packages
```
    sudo apt install texlive-full

    sudo apt install latexmk
```

Then follow the instrunctions from https://doc.akka.io/getting-started/starthere.html to see if a starting sample builds and runs.

# Akka framework overview
Akka actors - higher level of abstraction for writing concurrent distributed systems. It alleviates the developer from having to deal with explicit locking and thread management.

1. ActorSystem: container for actors, manages their lifecycle, provides configuration, logging, etc. https://doc.akka.io/libraries/akka-core/current/general/actor-systems.html
2. Actor: processing unit that can communicate with others via messages. https://doc.akka.io/libraries/akka-core/current/general/actors.html
   - Contain:
        - State (not accesible from outside)
        - Behavior (not accesible from outside) - defines how a message shall be processed
        - Mailbox (accesible via reference) - where other actors send messages. Piece that connects receiver with sender. FIFO by default.
        -  Child Actors - can delegate subtasks to them.
   - Has:
        - ActorReference: unique address to send messages to an actor, to restart it, etc.

## Running the Project

To build and run the project, navigate to the `code/project` directory and run:

```bash
make all
```

# Available Commands
Additional commands are available to demonstrate full project capabilities:

```bash
make all
```
Compiles the code and runs the full benchmark suite (Scenarios 1–9).

```bash
make benchmark
```
Same as make all, runs the full suite and prints the performance table.

```bash
make run
```
Runs a single default test case (N=10, f=4, M=3).

```bash
make clean
```
Removes compiled files and previous results.

# Contributors

- Elena Barau [@lena0097](https://github.com/lena0097)
- Daniele Fama [@DanieleFamà](https://github.com/danielefam)
- Aymen Kabil [@AymenKabil]()
- and me