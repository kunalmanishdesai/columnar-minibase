JDKPATH="/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
LIBPATH=".:.."
CLASSPATH="$LIBPATH"
BINPATH="$JDKPATH/bin"
JAVAC="$BINPATH/javac -classpath $CLASSPATH"
JAVA="$BINPATH/java -classpath $CLASSPATH"

PROGS = together

init:
	chmod +x batchInsert index query delete_query

clean:
	\rm -f *.class *~ \#* core 