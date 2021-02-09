# Code

This repository contains code for our experimental study.

### Preliminary Installations 

#### Setup Java Environment

You need to install following packages before installing sdkman:
```sh
    sudo apt-get install unzip
    sudo apt-get install zip
    sudo apt-get install sed
```

Uninstall skdman if you have it installed already:
```sh
    $ tar zcvf ~/sdkman-backup_$(date +%F-%kh%M).tar.gz -C ~/ .sdkman
    $ rm -rf ~/.sdkman
```

Then reinstalled sdkman:
```sh
    $ curl -s "https://get.sdkman.io" | bash
```

Next, open a new terminal or enter:
```sh
    $ source "$HOME/.sdkman/bin/sdkman-init.sh"
```
Lastly, run the following code snippet to ensure that installation succeeded:
```sh
    $ sdk version
```

#### Install Java 

Install java with fx version

```sh 
    sdk install java 8.0.232.fx-zulu
```

#### Install Gradle 

```sh 
    sdk install gradle 6.3
```


### Build Project Jar

```bash
    gradle clean

    gradle shadowJar 
```

### Locate Project Jar

```bash
/home/ubuntu/GIT/RDFShapes/build/libs/RDFShapes-all.jar
```


### Configuration File
This file contains all the config properties required to run the project 
```bash
 RDFShapes/config.properties
```


### Running Jar File
Use the following command to run the jar, along with specifying the dataset i.e., YAGO, WATDIV, or LUBM.

The below command is to run the jar for LUBM dataset
```bash
 java -jar build/libs/RDFShapes-all.jar RDFShapes/config.properties LUBM
```

In case you want to output all the logs (including apache query logs) to a file to see the jena query plans etc later on,
you should use the following command:
```bash
 java -jar build/libs/RDFShapes-all.jar RDFShapes/config.properties LUBM  &> output.log
```

In case you want to allocate more ram to the jar execution, use the following command:
```bash
 java  -Xmx200g -jar build/libs/RDFShapes-all.jar RDFShapes/config.properties LUBM
```

###### In case you need to install mvn:
```bash
    sudo apt update
    sudo apt install maven
    mvn -version
```




