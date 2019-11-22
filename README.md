# marklogic
Marklogic connector

## Setup Local Environment and run tests

1. [Install Docker Compose](https://docs.docker.com/compose/install/)
2. [Login or register in Docker hub](https://hub.docker.com/signup)
3. [Get MarkLogic developer image](https://hub.docker.com/_/marklogic)
4. [Login in your docker hub account via console](https://docs.docker.com/engine/reference/commandline/login/)
5. Enter the folder with docker-compose file:
   ```bash
   cd docker-compose/marklogic-plugin-env
   ```
6. Start docker environment by running commands:
   ```bash
   docker-compose up -d
   ```
7. Run script to create database:
      ```bash
      bash init_db.sh
      ```   
8. Run tests from project's root folder:
   ```bash
   mvn clean test
   ```   
   
## Properties
* **marklogic.host** - Server host. Default: localhost.
* **marklogic.port** - Server port. Default: 8011.
* **marklogic.database** - Server database for test databases. Default: mydb.
* **marklogic.username** - Server username. Default: root.
* **marklogic.password** - Server password. Default: 123Qwe123.