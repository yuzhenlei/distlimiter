# distlimiter
协调分布式节点可使用的总qps

## 问题
Q: 某些机器由于redis等remote异常导致报备失败，机器所允许的qps将置0，所有打到这台机器的请求都会失败？

A: 如果distlimiter返回失败，业务侧要负责将请求转发到其他机器