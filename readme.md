### 一. 提交消费偏移量

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