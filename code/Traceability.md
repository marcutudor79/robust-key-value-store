- [x] 1.REQ Main class creates N actors

- [x] 2.REQ Main class passes references of all actors to each actor

- [x] 3.REQ Use the name Process for the process class

- [x] 4.REQ Process class creates methods for executing put and get operations

- [x] 5.REQ Main class selects F random processes then sends a special CrashMessage to each of them

- [x] 6.REQ Upon receiving the CrashMessage, the process enters silent mode

- [x] 7.REQ Main class sends a LaunchMessage to all non-crashed processes

- [x] 8.REQ Upon receiving the LaunchMessage, the process starts executing put and get operations:
    - M put operations with k = 1 and v = i, N+i, 2N+i, ... MN+i
    - M get operations for k = 1

- [x] 9.REQ Every process performs at most one operation at a time

- [x] 10.REQ perform the experiment with N = 3,10,100 (with f = 1, 4, 49) and M = 3,10,100
      10.1 REQ take f, M as arguments

- [x] 11.REQ measure latency - total computation time

