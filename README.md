# Spring Canal

![Gradle CI](https://github.com/spring-canal/spring-canal/workflows/Gradle%20CI/badge.svg?branch=dev)

A Spring framework Canal support

[中文说明](https://github.com/spring-canal/spring-canal/blob/dev/README-zh.md)

# How to use

1. Import Dependency

    - Gradle
        ```groovy
        repositories {
            maven {
                url 'https://dl.bintray.com/spring-canal/maven'
            }
        }

        dependencies {
            implementation 'com.springframework.canal:spring-canal:1.1.4'
        }
        ```
    - Maven
      ```xml
      <project>
          <repositories>
              <repository>
                  <id>bintray-spring-canal-maven</id>
                  <name>bintray</name>
                  <url>https://dl.bintray.com/spring-canal/maven</url>
              </repository>
          </repositories>
    
          <dependencies>
              <dependency>
                  <groupId>com.springframework.canal</groupId>
                  <artifactId>spring-canal</artifactId>
                  <version>1.1.4</version>
              </dependency>
          </dependencies>
      </project>
      ```
2. Write Code

    Usage is similar to *spring-kafka*, annotate `@CanalListener` on classes or methods to consume Canal messages

    This is a simple example: 
    ```java
    // support annotations @Condition...
    @EnableCanal
    @Configuration
    public class Config {
        @Bean
        public CanalConnectorFactory canalConnectorFactory() {
            ConsumerProperties consumerProperties = new ConsumerProperties();
            // you can use 'SimpleCanalConnectorFactory' too.
            return new SharedCanalConnectorFactory(consumerProperties);
        }
    
        @Bean(CanalBootstrapConfiguration.CANAL_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
        public CanalListenerContainerFactory<?> canalListenerContainerFactory(CanalConnectorFactory canalConnectorFactory) {
            ContainerProperties containerProperties = new ContainerProperties();
            ConcurrencyCanalListenerContainerFactory<?> canalListenerContainerFactory = new ConcurrencyCanalListenerContainerFactory<>();
            canalListenerContainerFactory.setContainerProperties(containerProperties);
            canalListenerContainerFactory.setConnectorFactory(canalConnectorFactory);
            return canalListenerContainerFactory;
        }
    }
    
    @Component
    public class Somewhere {
        // payload generic type can be any JPA entity type
        @CanalListener(subscribes = {"Schema\\.Table"})
        public void listen(@Payload Collection<CanalEntry.RowChange> payloads) {
            // Do something...
        }
    }
    ```
    
    **For more details, please see the test code**

# Checking out and Building

To check out the project and build from the source, do the following:

    git clone git://github.com/spring-canal/spring-canal.git
    cd spring-canal
    ./gradlew build

- The Java SE 8 or higher is recommended to build the project.
- The `test` task requires **MySQL** and **Canal server**
  (If you don't have environment, just replace `./gradlew build` with `./gradlew build -x test` to skip the task)
    > Detailed requirements:
    > 
    > > MySQL runs on `localhost:3306` with username:`root` password:`123456`
    > > 
    > > _to change this, go to file `src/test/resources/META-INF/persistence.xml`_
    > 
    > > Canal server runs on `localhost:11111` with username:`canal` password:`canal` destination:`example`
    > > 
    > > _to change this, add system properties `canal.hostname` `canal.port` `canal.username` `canal.password` `canal.destination`_

To build API Javadoc (results will be in `build/docs`):

    ./gradlew javadoc

# Working principle

- Class level:
    |ConcurrencyMessageListenerContainer↓|↓↓↓↓↓↓↓↓↓↓↓↓↓|
    |------------------------------------|-------------|
    |DefaultMessageListenerContainer↓↓↓↓↓|↓↓↓↓↓↓↓↓↓↓↓↓↓|
    |MessageListenerTask↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓|↓↓↓↓↓↓↓↓↓↓↓↓↓|
    |CanalConnector                     +|Listen Method|

- Process level:

```
    ConnectorFactory    @CanalListener
           ↓                   ↓
    ContainerFactory       Endpoint
           ↘ ↖            ↗ ↙
               |-----------|
               | Registrar |<---.customize.---:user:
               |-----------|
                     ↓ ContainerFactory, Endpoint
                 Registry
                     ↓ Endpoint--------------|
              ContainerFactory               ↓
                     ↓ ConnectorFactory, ListenerAdapter
                 Container
                     ↓ ConnectorFactory, ListenerAdapter
                ListenerTask [Connector, ListenerAdapter]
```

# License

Spring Canal is released under the terms of the Apache Software License Version 2.0 (see LICENSE).

# Reference

[Alibaba Canal](https://github.com/alibaba/canal)

[Spring Kafka](https://github.com/spring-projects/spring-kafka)

![Your code is great, Now it's mine](pic.gif)
