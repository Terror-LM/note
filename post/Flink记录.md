# 记录

## Watermark

### Trigger触发机制

[Trigger](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/triggers/Trigger.java)决定了何时触发`ProcessFunction`, 默认的Trigger会在窗口结束时执行这个它.

> ProcessFunction有别于Reduce, Aggregate和Fold这些增量聚合函数之处在于, 后者在每个事件进来时都会执行, 并且各自有一个获取汇总结果的接口(getResult), 而前者在Window最后调用, 用于输出结果. 二者组合使用时, 窗口有效期内, 增量函数及时处理数据, 并计算汇总结果, 窗口期结束(或Trigger触发)时将结果传入Process函数, 此时, Process函数只有一条记录

通常, 我们希望实时得到输出, 而不是等到窗口结束, 于是Trigger派上用场了, Trigger提供了3中机制来触发Process函数

- `onElement()` 每个事件进来都会执行该方法, 可以在这里记录事件的数量, 以满足按事件数量触发, 见[CountTrigger](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/triggers/CountTrigger.java). 另外一种用法是通过事件来触发接下来的执行
- `onEventTime` 通过onElement设置的定时器(类似js里的setTimeout), 由定时器来调用该函数, 同时该方法会注册下一次执行, 见[ContinuousEventTimeTrigger](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/triggers/ContinuousEventTimeTrigger.java)
- `onProcessingTime` 跟onEventTime类似, 基于处理时间来触发, 换句话说, 即使没有事件进来, 该方法一样会被调用, 见[ContinuousProcessingTimeTrigger](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/triggers/ContinuousProcessingTimeTrigger.java)

### 疑惑

通过源码可以看到后面两个方法会先判断预期触发时间与实际触发时间, 二者相等时才触发, 否则什么事情都不会发生.

``` java
@Override
public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
	ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);

	if (fireTimestamp.get().equals(time)) { // 等值判断
		fireTimestamp.clear();
		fireTimestamp.add(time + interval);
		ctx.registerProcessingTimeTimer(time + interval);
		return TriggerResult.FIRE;
	}
	return TriggerResult.CONTINUE;
}
```

flink是如何保证每次触发这两个时间都相等的呢, 因为任何卡顿都会影响`实际调用时间`, 比如gc. 这里flink用基于时间的优先级队列来管理触发时机, 每次Trigger返回FIRE就往队列里添加一个元素, 这样触发顺序有了保证, 同时预期的触发时间也得以保留, 触发的动作收拢到了这个队列的消费上. 当线程拿到的记录符合条件, 则调用触发函数, 此时将预期的触发时间传入, 就保证了任何时间触发`预期`与`实际`两个时间都是相等的.

上述队列见[InternalTimerServiceImpl](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/InternalTimerServiceImpl.java), 不通时间机制触发条件不一样:

onProcessTime

``` java
@Override
public void onProcessingTime(long time) throws Exception {
	// null out the timer in case the Triggerable calls registerProcessingTimeTimer()
	// inside the callback.
	nextTimer = null;

	InternalTimer<K, N> timer;

	while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
		processingTimeTimersQueue.poll();
		keyContext.setCurrentKey(timer.getKey());
		triggerTarget.onProcessingTime(timer);
	}

	if (timer != null && nextTimer == null) {
		nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this);
	}
}
```

onEventTime(触发时间早于水位线才能触发)

``` java
public void advanceWatermark(long time) throws Exception {
	currentWatermark = time;

	InternalTimer<K, N> timer;

	while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
		eventTimeTimersQueue.poll();
		keyContext.setCurrentKey(timer.getKey());
		triggerTarget.onEventTime(timer);
	}
}
```

> 至于为什么一定要用等值判断, 推测跟窗口的边界有关系, 多一秒少一秒都不行, 如果触发时间超过了预期时间, 则意味着本次触发超过了窗口期, 那么如何维护当前窗口与下一个窗口的数据完整性是个麻烦事

### Trigger.onElement会导致`Process`函数短时间内重复执行

基于event时间的作业中, 如果自定义Trigger的间隔, 会发现短时间内Process被重复触发, 这其实是正常现象, **说明程序的消费能力超过了数据摄入能力(水位线领先于`Pane`的时间)**, 在处理历史数据时更明显

> `Pane` flink为Window划分了若干个面板([Pane](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/windowing/triggers/Trigger.java#L37)), 是个逻辑概念, 是相对于Window开始时间的偏移量来划分的, 实际触发时间是Pane的结束时间, Pane的大小由Trigger的间隔来决定. 例如5min的窗口, 30s的触发间隔, 则窗口的宽度: [00:00:00, 00:05:00), Pane就是:
>
> - [00:00:00, 00:00:30)
> - [00:00:30, 00:01:00)
> - [00:01:00, 00:01:30)
> - ...

根据Trigger的机制我们可以知道: **基于事件时间的触发需要满足Pane的结束时间小于当前水位**

水位线描述了程序处理的进度, 即小于水位线的事件都已经得到处理. 如果程序处理的很快, 水位很高, 比如5min的窗口, [**最大乱序间隔**](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/timestamps/BoundedOutOfOrdernessTimestampExtractor.java#L46)为2min, 程序在30s内就处理完了这个窗口(事件时间在5min内)的数据, 则水位就到了00:03:00, 此时第一个`Pane(00:00:30)`触发, 同时注册下一个`Pane(00:01:00)`, 这个Pane又小于水位线, 于是再次触发并注册回调, 如此往复. 由此就出现了**1s内连续触发了多次Process函数, 且输出都一样**.

> 在当前触发的Pane与水位线之间有多少个Pane(n)就触发多少(n)次, 这会卡主Window吗(n * Pane Size)? 答案是不会. flink对基于事件时间的触发是用**优先级队列**来管理的, Pane触发的回调只是**往队列中添加一条包含下次触发时间的记录**, 队列的消费几乎不需要时间, 所以这n个Pane会迅速触发完毕, 不会造成阻塞.

------
