# Hadoop Operation Util collection
- A collection of programs necessary for Hadoop operation.
- If you find useful programs, don't keep them to yourself; share them and you'll be rewarded.
- Aims to reduce time spent on writing duplicate programs and improve efficiency.

## Project Structure
- Mainly, you can write Java and Scala programs.
- Uses JDK Eclipse Temurin 21.0.1-tem.
- Python scripts are written based on version 3.8.12.
    - Recommend using a combination of `pyenv` and `virtualenv` for Python.
    - Shell scripts are written based on the Bash shell. (Servers use Bash shell)

```bash
/hadoop-ops-util
├── src
│   ├── main
│   │   ├── scala
│   │   │   ├── ...
│   │   ├── java
│   │   │   ├── ...
│── scripts 
│   │   ├── python
│   │   ├── bash   
```


## sttp reference
- The Scala HTTP client
- doc link: https://sttp.softwaremill.com/en/stable

## better-files
- better-files is a dependency-free pragmatic thin Scala wrapper around Java NIO.
- doc link: https://github.com/pathikrit/better-files?tab=readme-ov-file#unix-dsl
