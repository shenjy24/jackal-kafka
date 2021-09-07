### 一. 提交消费位移

1. 自动提交
- 设置`enable.auto.commit`为`true`;
- 设置自动提交间隔`auto.commit.interval.ms`。

2. 手动提交
- 设置`enable.auto.commit`为`false`;
- `commitSync`同步提交，失败可以自动重试，但会阻塞消费端；
- `commitAsync`异步提交，失败无法自动重试，但不会阻塞消费端。

3. 最佳实践
- 对于常规性、阶段性的手动提交，我们调用 commitAsync() 避免程序阻塞；
- 在 Consumer 要关闭前，我们调用 commitSync() 方法执行同步阻塞式的位移提交，以确保 Consumer 关闭前能够保存正确的位移数据。

4. 批量优化
- 如果poll返回的数据过多，可以分批次进行手动提交。

### 二. 事务
#### 1. 应用场景
在一个原子操作中，根据包含的操作类型，可以分为三种情况，前两种情况是事务引入的场景，最后一种情况没有使用价值。
- 只有Producer生产消息；
- 消费消息和生产消息并存，这个是事务场景中最常用的情况，就是我们常说的“consume-transform-produce ”模式；
- 只有consumer消费消息，这种操作其实没有什么意义，跟使用手动提交效果一样，而且也不是事务属性引入的目的，所以一般不会使用这种情况。

#### 2. 事务配置
##### 消费者配置
- 将配置中的自动提交属性`enable.auto.commit`进行关闭
- 在代码里面也不能使用手动提交commitSync或者commitAsync
- 设置事务隔离级别`isolation.level`

##### 生产者配置
- 配置`transactional.id`属性
- 配置`enable.idempotence`属性