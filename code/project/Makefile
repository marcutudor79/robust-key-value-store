JAVA 		?= java
JAVAC		?= javac
MAVEN 		?= mvn

all: compile run

compile:
	${MAVEN} clean compile

run:
	${MAVEN} exec:java -Dexec.mainClass="keyValueStore.Main" -Dexec.args="10 4"

clean:
	rm -rf target/*