# Spring Canal

Spring框架的Canal支持

[English Document](https://github.com/spring-canal/spring-canal/blob/main/README.md)

# 如何使用

1. 导入依赖

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
                  <name>spring-canal repo</name>
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
2. 编写代码

    使用方法类似*spring-kafka*, 在类或方法上添加注解 `@CanalListener` 来消费Canal的消息
    
    简单示例: 
    ```java
    // 支持条件注解@Condition...
    @EnableCanal
    @Configuration
    public class Config {
        @Bean
        public CanalConnectorFactory canalConnectorFactory() {
            ConsumerProperties consumerProperties = new ConsumerProperties();
            // 亦可使用'SimpleCanalConnectorFactory'
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
        // 有效载荷泛型参数可以是任意JPA实体类型
        @CanalListener(subscribes = {"库名\\.表名"})
        public void listen(@Payload Collection<CanalEntry.RowChange> payloads) {
            // 做一点事...
        }
    }
    ```
    
    **有关更多详细信息，请参见测试代码**

# 检出和构建

欲检出项目并从源码进行构建，请执行以下操作：

    git clone git://github.com/spring-canal/spring-canal.git
    cd spring-canal
    ./gradlew build

- 请使用Java SE 8或更高版本来构建项目
- `test`任务需要 **MySQL**和**Canal服务端**
  (若您无此环境，请将`./gradlew build`更换为`./gradlew build -x test`跳过此任务)
  > 详细要求:
  >
  > > MySQL运行于`localhost:3306`并配置用户名:`root` 密码:`123456`
  > >
  > > _欲更改此配置，请前往文件`src/test/resources/META-INF/persistence.xml`_
  >
  > > Canal服务端运行于`localhost:11111`并配置用户名:`canal` 密码:`canal` 目标:`example`
  > >
  > > _欲更改此配置，请添加系统配置`canal.hostname` `canal.port` `canal.username` `canal.password` `canal.destination`_

欲构建接口文档 (结果位于 `build/docs`):

    ./gradlew javadoc

# 工作原理

- 类级别:
    |ConcurrencyMessageListenerContainer↓|↓↓↓↓↓↓↓↓↓↓↓↓↓|
    |------------------------------------|-------------|
    |DefaultMessageListenerContainer↓↓↓↓↓|↓↓↓↓↓↓↓↓↓↓↓↓↓|
    |MessageListenerTask↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓|↓↓↓↓↓↓↓↓↓↓↓↓↓|
    |CanalConnector                     +|Listen Method|

- 流程级别:

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

# 许可

Spring Canal根据Apache License 2.0版本条款发行（参阅LICENSE）

# 参照

[Alibaba Canal](https://github.com/alibaba/canal)

[Spring Kafka](https://github.com/spring-projects/spring-kafka)

![Your code is great, Now it's mine](pic.gif)
