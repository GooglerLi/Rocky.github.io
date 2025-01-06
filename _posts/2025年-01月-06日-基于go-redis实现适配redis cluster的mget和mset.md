---
layout: post
title: 基于go-redis实现适配redis cluster的mget和mset 
---

# 基于go-redis实现适配redis cluster的mget和mset   

目前主流开源的redis cluster sdk不直接提供适配custer的***mget***和***mset***,如go-redis/redigo等。也有一些非主流sdk实现了集群版本的mget、mset,如https://github.com/wuxibin89/redis-go-cluster。但是该开源版本七年未更新，另外实现对开发并不友好，很多功能并不确定是否有问题。      
基于实际情况考虑，组内用go-redis较多，因此决定基于go-redis实现mget和mset。

## 实现方案一
 借鉴redis-go-cluster。根据slot对key进行分task，然后多协程并发对单个slot进行mget/mset。针对mset使用pipieline设置过期时间。
 
```
func (c *RedisClient) MGet(keys []string) (map[string]string, error) {
	reply := make(map[string]string)
	tasks := make([]MGetTask, 0)

	for _, v := range keys {
		slot := hashtag.Slot(v)
		cli, err := c.client.MasterForKey(context.TODO(), v)
		if err != nil {
			yylog.Warn("RedisCli.MasterForKey failed", zap.Any("key", v), zap.Any("err", err))
			return reply, nil
		}

		var j int
		for j = 0; j < len(tasks); j++ {
			if tasks[j].slot == slot {
				tasks[j].args = append(tasks[j].args, v)
				break
			}
		}

		if j == len(tasks) {
			task := MGetTask{
				slot:   slot,
				client: cli,
				args:   []string{v},
			}
			tasks = append(tasks, task)
		}
	}

	var wg sync.WaitGroup
	for i, t := range tasks {
		p := i
		tk := t
		wg.Add(1)
		gfy.Go(context.Background(), func(ctx context.Context) int {
			defer wg.Done()
			tasks[p].replys = tk.client.MGet(ctx, tk.args...)
			return 0
		})
	}
	wg.Wait()

	for _, t := range tasks {
		if t.replys != nil {
			d := t.replys.Val()
			err := t.replys.Err()
			if len(d) != len(t.args) || err != nil {
				yylog.Warn("tasks.replys get fail", zap.Any("err", err),
					zap.Any("tk.args.size", len(t.args)), zap.Any("d.size", len(d)))
				return reply, nil
			}

			for i, a := range t.args {
				if d[i] != redis.Nil && d[i] != nil {
					reply[a] = d[i].(string)
				}
			}
		}

	}

	return reply, nil
}
```

```
func (c *RedisClient) MSet(keyVal map[string]string, expiration int) error {

	tasks := make([]MSetTask, 0)
	for k, v := range keyVal {
		slot := hashtag.Slot(k)
		cli, err := c.client.MasterForKey(context.TODO(), k)
		if err != nil {
			yylog.Warn("RedisCli.MasterForKey failed", zap.Any("key", k), zap.Any("err", err))
			continue
		}

		var j int
		for j = 0; j < len(tasks); j++ {
			if tasks[j].slot == slot {
				tasks[j].args[k] = v
				break
			}
		}

		if j == len(tasks) {
			task := MSetTask{
				slot:   slot,
				client: cli,
			}
			task.args = make(map[string]interface{})
			task.args[k] = v
			tasks = append(tasks, task)
		}
	}

	var wg sync.WaitGroup
	for i, t := range tasks {
		p := i
		tk := t
		wg.Add(1)
		gfy.Go(context.Background(), func(ctx context.Context) int {
			defer wg.Done()
			reply := tk.client.MSet(ctx, tk.args)
			if reply != nil && reply.Err() != nil {
				yylog.Warn("tk.client.MSet fail", zap.Any("err", reply.Err()))
				return -1
			}

			tasks[p].replys = reply
			if expiration > 0 {
				pp := tk.client.Pipeline()
				for k, _ := range tk.args {
					pp.Expire(ctx, k, time.Duration(expiration+rand.Intn(KeyExpirationRandom))*time.Second)
				}

				// 执行 Pipeline
				_, err := pp.Exec(ctx)
				if err != nil {
					yylog.Warn("Pipeline.Exec Expire fail", zap.Any("args", tk.args), zap.Any("err", err))
				}
			}
			return 0
		})
	}
	wg.Wait()

	for _, t := range tasks {
		if t.replys != nil {
			if err := t.replys.Err(); err != nil {
				return err
			}
		}
	}

	return nil
}
```

## 实现方案二
借鉴redis-go-cluster。根据slot对key进行分task，然后对单个slot进行mget/mset,使用pipeline聚合。并使用pipe设置过期时间。

```
func (c *RedisClient) MGet2(keys []string) (map[string]string, error) {

	reply := make(map[string]string)
	tasks := make([]MGetTask, 0)

	for _, v := range keys {
		slot := hashtag.Slot(v)
		/*
			cli, err := c.client.MasterForKey(context.TODO(), v)
			if err != nil {
				yylog.Warn("RedisCli.MasterForKey failed", zap.Any("key", v), zap.Any("err", err))
				return reply, err
			}*/

		var j int
		for j = 0; j < len(tasks); j++ {
			if tasks[j].slot == slot {
				tasks[j].args = append(tasks[j].args, v)
				break
			}
		}

		if j == len(tasks) {
			task := MGetTask{
				slot: slot,
				//client: cli,
				args: []string{v},
			}
			tasks = append(tasks, task)
		}
	}

	pp := c.client.Pipeline()
	for _, t := range tasks {
		pp.MGet(context.Background(), t.args...)
	}

	// 执行 Pipeline
	cmds, err := pp.Exec(context.Background())
	if err != nil && err != redis.Nil {
		yylog.Warn("Pipeline.Exec fail", zap.Any("err", err))
		return reply, err
	}

	if len(tasks) != len(cmds) {
		yylog.Warn("Pipeline.Exec fail", zap.Any("tasks.size", len(tasks)),
			zap.Any("cmds.size", len(cmds)))
		return reply, fmt.Errorf("Pipeline.Exec fail, cmds size wrong")
	}

	for i, t := range tasks {
		res, err := cmds[i].(*redis.SliceCmd).Result()
		if err != nil && err != redis.Nil {
			yylog.Warn("Pipeline.Exec fail", zap.Any("err", err))
			return reply, err
		}

		for i, v := range t.args {
			if res[i] != redis.Nil && res[i] != nil {
				reply[v] = res[i].(string)
			}
		}
	}

	return reply, nil
}
```
```
func (c *RedisClient) MSet2(keyVal map[string]string, expiration int) error {

	tasks := make([]MSetTask, 0)
	for k, v := range keyVal {
		slot := hashtag.Slot(k)
		/*
			cli, err := c.client.MasterForKey(context.TODO(), k)
			if err != nil {
				yylog.Warn("RedisCli.MasterForKey failed", zap.Any("key", k), zap.Any("err", err))
				continue
			}*/

		var j int
		for j = 0; j < len(tasks); j++ {
			if tasks[j].slot == slot {
				tasks[j].args[k] = v
				break
			}
		}

		if j == len(tasks) {
			task := MSetTask{
				slot: slot,
				//client: cli,
			}
			task.args = make(map[string]interface{})
			task.args[k] = v
			tasks = append(tasks, task)
		}
	}

	pp := c.client.Pipeline()
	for _, t := range tasks {
		pp.MSet(context.Background(), t.args)
		if expiration > 0 {
			for k, _ := range t.args {
				pp.Expire(context.Background(), k, time.Duration(expiration+rand.Intn(KeyExpirationRandom))*time.Second)
			}
		}
	}

	// 执行 Pipeline
	cmds, err := pp.Exec(context.Background())
	if err != nil {
		yylog.Warn("Pipeline.Exec fail", zap.Any("err", err))
		return err
	}

	if len(keyVal) != len(cmds) {
		yylog.Warn("Pipeline.Exec fail", zap.Any("keyVal.size", len(keyVal)),
			zap.Any("cmds.size", len(cmds)))
		return fmt.Errorf("Pipeline.Exec fail, cmds size wrong")
	}

	return nil
}
```

## 实现方案三
直接对所有key使用get/setex，然后pipeline聚合。

```
func (c *RedisClient) MGet3(keys []string) (map[string]string, error) {

	reply := make(map[string]string)

	pp := c.client.Pipeline()
	for _, v := range keys {
		pp.Get(context.Background(), v)
	}

	// 执行 Pipeline
	cmds, err := pp.Exec(context.Background())
	if err != nil && err != redis.Nil {
		yylog.Warn("Pipeline.Exec fail", zap.Any("err", err))
		return reply, err
	}

	if len(keys) != len(cmds) {
		yylog.Warn("Pipeline.Exec fail", zap.Any("keys.size", len(keys)),
			zap.Any("cmds.size", len(cmds)))
		return reply, fmt.Errorf("Pipeline.Exec fail, cmds size wrong")
	}

	for i, k := range keys {
		res, err := cmds[i].(*redis.StringCmd).Result()
		if err != nil && err != redis.Nil {
			yylog.Warn("Pipeline.Exec fail", zap.Any("err", err), zap.Any("key", k))
			return reply, err
		}

		if err != redis.Nil {
			reply[k] = res
		}
	}

	return reply, nil
}
```
```
func (c *RedisClient) MSet3(keyVal map[string]string, expiration int) error {

	pp := c.client.Pipeline()
	for k, v := range keyVal {
		pp.SetEX(context.Background(), k, v, time.Duration(expiration+rand.Intn(KeyExpirationRandom))*time.Second)
	}

	// 执行 Pipeline
	cmds, err := pp.Exec(context.Background())
	if err != nil {
		yylog.Warn("Pipeline.Exec fail", zap.Any("err", err))
		return err
	}

	if len(keyVal) != len(cmds) {
		yylog.Warn("Pipeline.Exec fail", zap.Any("keyVal.size", len(keyVal)),
			zap.Any("cmds.size", len(cmds)))
		return fmt.Errorf("Pipeline.Exec fail, cmds size wrong")
	}

	return nil
}
```

# 压测数据

容器配置：4核，1GB。同机房读4主4从节点redis cluster。目前组内是使用方案三。
  
## 测试场景1
一个协程for循换批量mget3000个key，其中1500个key存在，1500个key不存在。
| MGet      | 内存使用率 | CPU使用率 |  Qps/min | 
| ----------- | ----------- |----------- |----------- |
| 方案一      | 0.1%       | 310%       |  1010次/m       |
| 方案二   | 0.1%        | 115%      | 2828次/m        |
| 方案三   | 0.1%        | 125%       | 10670次/m        |


## 测试场景2
一个协程for循换批量mset3000个key。并设置过期时间。mset场景qps忘记保存。
| MSet      | 内存使用率 | CPU使用率 |  Qps/min | 
| ----------- | ----------- |----------- |----------- |
| 方案一      | 0.1%       | 325%      |  1010次/m       |
| 方案二   | 0.1%        | 109%      | 2828次/m        |
| 方案三   | 0.1%        | 112%      | 10670次/m        |