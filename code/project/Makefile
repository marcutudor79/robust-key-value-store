JAVA        ?= java
JAVAC       ?= javac
MAVEN       ?= mvn

all: compile benchmark

compile:
	${MAVEN} clean compile

# Run a single default test case
run:
	${MAVEN} exec:java -Dexec.mainClass="keyValueStore.Main" -Dexec.args="10 4 3"

# Run all 9 Project Scenarios sequentially
benchmark: compile
	# 1. Initialize Results Table File
	rm -f benchmark_results.csv
	@echo "N     | f     | M     | Latency" > benchmark_results.csv
	@echo "------+-------+-------+---------" >> benchmark_results.csv
	
	@echo "==================================================="
	@echo "STARTING BENCHMARK (Console Output)"
	@echo "==================================================="

	@echo "--- SCENARIO 1: N=3, f=1, M=3 ---"
	${MAVEN} -q exec:java -Dexec.mainClass="keyValueStore.Main" -Dexec.args="3 1 3"
	
	@echo "--- SCENARIO 2: N=3, f=1, M=10 ---"
	${MAVEN} -q exec:java -Dexec.mainClass="keyValueStore.Main" -Dexec.args="3 1 10"

	@echo "--- SCENARIO 3: N=3, f=1, M=100 ---"
	${MAVEN} -q exec:java -Dexec.mainClass="keyValueStore.Main" -Dexec.args="3 1 100"

	@echo "--- SCENARIO 4: N=10, f=4, M=3 ---"
	${MAVEN} -q exec:java -Dexec.mainClass="keyValueStore.Main" -Dexec.args="10 4 3"

	@echo "--- SCENARIO 5: N=10, f=4, M=10 ---"
	${MAVEN} -q exec:java -Dexec.mainClass="keyValueStore.Main" -Dexec.args="10 4 10"

	@echo "--- SCENARIO 6: N=10, f=4, M=100 ---"
	${MAVEN} -q exec:java -Dexec.mainClass="keyValueStore.Main" -Dexec.args="10 4 100"

	@echo "--- SCENARIO 7: N=100, f=49, M=3 (High Memory) ---"
	export MAVEN_OPTS="-Xmx12g -XX:+UseG1GC" && ${MAVEN} -q exec:java -Dexec.mainClass="keyValueStore.Main" -Dexec.args="100 49 3"

	@echo "--- SCENARIO 8: N=100, f=49, M=10 (High Memory) ---"
	export MAVEN_OPTS="-Xmx12g -XX:+UseG1GC" && ${MAVEN} -q exec:java -Dexec.mainClass="keyValueStore.Main" -Dexec.args="100 49 10"

	@echo "--- SCENARIO 9: N=100, f=49, M=100 (High Memory) ---"
	export MAVEN_OPTS="-Xmx12g -XX:+UseG1GC" && ${MAVEN} -q exec:java -Dexec.mainClass="keyValueStore.Main" -Dexec.args="100 49 100"

	@echo ""
	@echo "╔══════════════════════════════════════╗"
	@echo "║           FINAL RESULTS              ║"
	@echo "╚══════════════════════════════════════╝"
	@cat benchmark_results.csv
	@echo ""

clean:
	rm -rf target/* benchmark_results.csv