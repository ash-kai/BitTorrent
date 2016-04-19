
if find peer_1*; then
    rm -r peer_1*
fi

if find *.class; then
    rm *.class
fi
javac *.java
clear
echo "The previous files and removed and new class files generated"
