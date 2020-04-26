# ordered data collection program

## 系统描述
一个系统包含多个数据生产者，每个数据生产者都会不停的生产数据，并且生产者会为每个 data 发送两条 messages
* P message(`type == prepare`), 包含了 perpare token
* C message(`type == commit`), 包含了 prepare 和 commit token

### 实现要求
* 按照 commit token 的升序顺序对 data 进行排序，并且尽快的输出 data
* 为你的排序过程实现 `流控` 机制，用来控制内存和 CPU 资源的使用
* 请按照一个完善工程的标准来完成这个小作业